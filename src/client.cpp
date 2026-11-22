#include <cstring>
#include <iostream>
#include <ostream>
#include <sys/socket.h>
#include <sys/un.h>

#include <capnp/dynamic.h>
#include <capnp/pretty-print.h>
#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>

#include <mp/proxy.capnp.h>
#include <init.capnp.h>
#include <mining.capnp.h>


int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: client <path-to-node.sock>\n";
        return 1;
    }

    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    sockaddr_un addr{.sun_family = AF_UNIX};
    std::strncpy(addr.sun_path, argv[1], sizeof(addr.sun_path) - 1);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("connect");
        return 1;
    }

    auto io = kj::setupAsyncIo();
    auto stream = io.lowLevelProvider->wrapSocketFd(fd, kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
    capnp::TwoPartyClient client(*stream);

    auto init = client.bootstrap().castAs<ipc::capnp::messages::Init>();

    auto constructPromise = init.constructRequest().send();
    auto threadMapClient = constructPromise.getThreadMap();

    auto makePoolReq = threadMapClient.makePoolRequest();
    makePoolReq.setCount(2);
    auto makePoolPromise = makePoolReq.send();

    auto request = init.makeEventsRequest();
    auto options = request.initOptions();
    options.initBlockConnected().setIncludeData(true);
    options.initBlockDisconnected().setIncludeData(true);
    options.initTxAddedToMempool().setIncludeData(true);
    options.initTxRemovedFromMempool().setIncludeData(true);
    auto eventsClient = request.send().getResult();

    uint64_t last{0};
    try {
        for (;;) {
            auto waitNextRequest = eventsClient.waitNextRequest();
            waitNextRequest.setAfter(last);
            waitNextRequest.setMaxEvents(1000);
            auto response = waitNextRequest.send().wait(io.waitScope);
            auto events = response.getResult();
            std::cout << "received " << events.size() << " events" << std::endl;
            if (events.size() == 0) {
                std::cout << "node shutting down" << std::endl;
                break;
            }
            for (auto event : events) {
                if (event.getSequence() != last + 1) {
                    std::cout << "gap: missed " << event.getSequence() - last - 1 << " events" << std::endl;
                }
                last = event.getSequence();
                switch (event.which()) {
                    case ipc::capnp::messages::Event::BLOCK_CONNECTED: {
                        auto e = event.getBlockConnected();
                        std::cout << last << " connected " << e.getHeight() << " " << e.getData().size() << " bytes" << std::endl;
                        break;
                    }
                    case ipc::capnp::messages::Event::BLOCK_DISCONNECTED: {
                        auto e = event.getBlockDisconnected();
                        std::cout << last << " disconnected " << e.getHeight() << " " << e.getData().size() << " bytes" << std::endl;
                        break;
                    }
                    case ipc::capnp::messages::Event::TX_ADDED_TO_MEMPOOL: {
                        auto e = event.getTxAddedToMempool();
                        std::cout << last << " tx added " << e.getMempoolSequence() << " " << e.getData().size() << " bytes" << std::endl;
                        break;
                    }
                    case ipc::capnp::messages::Event::TX_REMOVED_FROM_MEMPOOL: {
                        auto e = event.getTxRemovedFromMempool();
                        std::cout << last << " tx removed " << e.getMempoolSequence() << " " << e.getData().size() << " bytes" << std::endl;
                        break;
                    }
                }
            }
        }
    } catch (const kj::Exception& e) {
        std::cerr << "disconnected: " << e.getDescription().cStr() << std::endl;
    }
}

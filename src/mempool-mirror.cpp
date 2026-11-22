#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unordered_map>
#include <vector>

#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <nlohmann/json.hpp>

#include <mp/proxy.capnp.h>
#include <init.capnp.h>
#include <rpc.capnp.h>

using ipc::capnp::messages::Event;

using uint256 = std::array<uint8_t, 32>;

struct Uint256Hasher {
    size_t operator()(const uint256& h) const
    {
        size_t r;
        std::memcpy(&r, h.data(), sizeof(r));
        return r;
    }
};

struct Entry {
    std::vector<uint8_t> tx;
    int64_t fee;
    int64_t vsize;
};

uint256 ToUint256(capnp::Data::Reader txid)
{
    assert(txid.size() == 32);
    uint256 r;
    std::memcpy(r.data(), txid.begin(), 32);
    return r;
}

std::vector<uint8_t> ParseHex(const std::string& hex)
{
    std::vector<uint8_t> r(hex.size() / 2);
    for (size_t i = 0; i < r.size(); ++i) r[i] = static_cast<uint8_t>(std::stoi(hex.substr(2 * i, 2), nullptr, 16));
    return r;
}

// RPC prints txids byte-reversed.
uint256 ParseTxid(const std::string& hex)
{
    auto bytes = ParseHex(hex);
    uint256 r;
    std::copy(bytes.rbegin(), bytes.rend(), r.begin());
    return r;
}

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: mempool-mirror <path-to-node.sock>\n";
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

    // Subscribe before taking the snapshot so no event is missed in between.
    auto request = init.makeEventsRequest();
    auto options = request.initOptions();
    options.initTxAddedToMempool().setIncludeData(true);
    options.initTxRemovedFromMempool();
    auto eventsClient = request.send().getResult();

    std::unordered_map<uint256, Entry, Uint256Hasher> mempool;
    uint64_t snapshotSequence{0};

    auto rpcClient = init.makeRpcRequest().send().getResult();
    auto rpc = [&](const nlohmann::json& request) {
        auto req = rpcClient.executeRpcRequest();
        req.setRequest(request.dump());
        req.setUri("");
        req.setUser("");
        auto response = req.send().wait(io.waitScope);
        return nlohmann::json::parse(response.getResult().cStr());
    };

    auto loadSnapshot = [&] {
        mempool.clear();
        auto reply = rpc({{"jsonrpc", "2.0"}, {"id", 0}, {"method", "getrawmempool"}, {"params", {false, true}}});
        if (reply.contains("error")) throw std::runtime_error("getrawmempool: " + reply["error"].dump());
        const auto& result = reply["result"];
        snapshotSequence = result["mempool_sequence"].get<uint64_t>();

        // Fetch fee, vsize and raw tx in one batch. Entries evicted meanwhile
        // return an error and are skipped; their removal event has a later
        // sequence.
        nlohmann::json batch = nlohmann::json::array();
        for (const auto& txid : result["txids"]) {
            batch.push_back({{"jsonrpc", "2.0"}, {"id", {txid, "entry"}}, {"method", "getmempoolentry"}, {"params", {txid}}});
            batch.push_back({{"jsonrpc", "2.0"}, {"id", {txid, "tx"}}, {"method", "getrawtransaction"}, {"params", {txid}}});
        }
        if (!batch.empty()) {
            std::unordered_map<uint256, nlohmann::json, Uint256Hasher> entries, txs;
            for (const auto& r : rpc(batch)) {
                if (r.contains("error")) continue;
                auto txid = ParseTxid(r["id"][0].get<std::string>());
                (r["id"][1] == "entry" ? entries : txs)[txid] = r["result"];
            }
            for (const auto& [txid, e] : entries) {
                auto tx = txs.find(txid);
                if (tx == txs.end()) continue;
                mempool[txid] = {ParseHex(tx->second.get<std::string>()), std::llround(e["fees"]["base"].get<double>() * 1e8), e["vsize"].get<int64_t>()};
            }
        }
        std::cout << "snapshot: " << mempool.size() << " txs at mempool sequence " << snapshotSequence << std::endl;
    };

    auto printStats = [&] {
        int64_t fee{0}, vsize{0};
        for (const auto& [_, e] : mempool) {
            fee += e.fee;
            vsize += e.vsize;
        }
        std::cout << "mempool: " << mempool.size() << " txs, " << vsize << " vB, " << fee << " sat" << std::endl;
    };

    uint64_t last{0};
    try {
        loadSnapshot();
        printStats();
        for (;;) {
            auto waitNextRequest = eventsClient.waitNextRequest();
            waitNextRequest.setAfter(last);
            waitNextRequest.setMaxEvents(1000);
            auto response = waitNextRequest.send().wait(io.waitScope);
            auto events = response.getResult();
            if (events.size() == 0) {
                std::cout << "node shutting down" << std::endl;
                break;
            }
            bool resync = false;
            for (auto event : events) {
                if (event.getSequence() != last + 1) {
                    std::cout << "gap: missed " << event.getSequence() - last - 1 << " events, resyncing" << std::endl;
                    resync = true;
                }
                last = event.getSequence();
                switch (event.which()) {
                case Event::TX_ADDED_TO_MEMPOOL: {
                    auto e = event.getTxAddedToMempool();
                    if (e.getMempoolSequence() >= snapshotSequence) {
                        auto data = e.getData();
                        mempool[ToUint256(e.getTxid())] = {{data.begin(), data.end()}, e.getFee(), e.getVsize()};
                    }
                    break;
                }
                case Event::TX_REMOVED_FROM_MEMPOOL: {
                    auto e = event.getTxRemovedFromMempool();
                    assert(e.hasData() == false);
                    if (e.getMempoolSequence() >= snapshotSequence) mempool.erase(ToUint256(e.getTxid()));
                    break;
                }
                default:
                    break;
                }
            }
            // Events from before the new snapshot are skipped by the
            // mempool sequence check, so keep reading from `last`.
            if (resync) loadSnapshot();
            printStats();
        }
    } catch (const kj::Exception& e) {
        std::cerr << "disconnected: " << e.getDescription().cStr() << std::endl;
    }
}

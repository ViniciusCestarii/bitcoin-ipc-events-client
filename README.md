# bitcoin-ipc-events-client

Cap'n Proto clients for Bitcoin Core's IPC events interface.

- `client`: prints block and mempool events from the node.
- `mempool-mirror`: keeps a local copy of the node's mempool from a snapshot plus events.

```sh
cmake -B build && cmake --build build
./build/client <path-to-node.sock>
./build/mempool-mirror <path-to-node.sock>
```

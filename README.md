# Contract Indexer

This indexer watches for new contract deployments and sends NEP-141 deployments to Redis stream `newcontract_nep141`. To avoid handling contract update (second deployment on the same address), it saves existing tokens in `known_tokens.txt` on each line. Before running, it's recommended to backfill or manually enter all known tokens in `known_tokens.txt` so that it doesn't trigger an event with wrong timestamp when an existing contract is updated.

The NEP-141 detection works by calling `ft_metadata` on RPC, set `REDIS_URL` environment variable to override the RPC URL. It calls this method at the specific block when a "deploy code" receipt was executed, but since RPCs can garbage collect some relatively old blocks, set `LATEST_BLOCK_META=1` environment variable, and it'll request at latest final block.

To run it, set `REDIS_URL` environment variable and `cargo run --release`

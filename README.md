# Potlock Indexer

This indexer watches for Potlock donation events (normal donation, pot project donation, pot donation) and sends them to Redis streams `potlock_donation`, `potlock_pot_project_donation`, and `potlock_pot_donation` respectively.

To run it, set `REDIS_URL` environment variable and `cargo run --release`

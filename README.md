
## Rust Implementation of an Urkel Trie

In progress implementation of an `urkel` (Base-2 Merkle) trie from the [Handshake project](https://github.com/handshake-org/urkel).

What is exactly is an Urkel Trie? From the Handshake site:

> The urkel tree was created for the Handshake protocol, and is implemented as a base-2 merkelized trie. It was created as an alternative to Ethereum's base-16 trie (which was the initial choice for Handshake name proofs).
>
> Urkel stores nodes in a series of append-only files for snapshotting and crash consistency capabilities. Due to these presence of these features, Urkel has the ability to expose a fully transactional database.

Supports: `insert, get, remove, proof` with the (alpha) Urkel embedded database. 

See `tests` for example use.

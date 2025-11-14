# pq-compact-rs

`pq-compact-rs` is a small Rust tool for compacting object-storage parquet data.
It reads objects from an input prefix, runs a parquet schema fingerprinting step,
compacts parquet files with the same schema fingerprint, and
writes the results to an output prefix. It's useful when upstream
systems produce lots of small inefficient parquet files and you want to reorganize them into
fewer, more efficient ones.

This project is still experimental and may change over time.


## Getting Started

### Requirements

-   Rust (stable toolchain)
-   Credentials for whatever bucket(s) you're accessing
-   `cargo` available on your system

### Clone the repo

``` bash
git clone https://github.com/zachdrone/pq-compact-rs.git
cd pq-compact-rs
```

### Build

Debug:

``` bash
cargo build
```

Release:

``` bash
cargo build --release
```

Resulting binaries:

-   `target/debug/pq-compact-rs`
-   `target/release/pq-compact-rs`

------------------------------------------------------------------------

## Usage

Basic structure:

``` bash
cargo run --   -i <input-prefix>   -o <output-prefix>   --input-bucket <input-bucket>   --output-bucket <output-bucket>
```

### CLI Options

Show all flags:

``` bash
cargo run -- --help
```

Or with a release binary:

``` bash
./target/release/pq-compact-rs --help
```

Key flags:

-   `-i, --input-prefix <PREFIX>` --- Prefix to read from\
-   `-o, --output-prefix <PREFIX>` --- Prefix to write compacted data
    into\
-   `--input-bucket <NAME>` --- Source bucket\
-   `--output-bucket <NAME>` --- Destination bucket

------------------------------------------------------------------------

## How It's Used

Typical workflow:

1.  An upstream process writes many small objects to an input prefix.\
2.  You run this tool manually or on a schedule.\
3.  It rewrites the data into a more compact structure under a new
    prefix.\
4.  Downstream jobs read from that compacted prefix for better
    performance.

Useful for cleanup, periodic maintenance, or reorganizing data layouts.

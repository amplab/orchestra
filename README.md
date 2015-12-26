# Hermes

## Running the Prototype

From the Hermes root directory, run

```bash
RUST_BACKTRACE=1 RUST_LOG=hermes=info cargo run --bin hermes
```

Note that you need Rust nightly to run it. From the `examples` directory, run

```bash
python matrix_multiply.py 1235
```

and

```bash
python matrix_multiply.py 1236
```

Now, from the `lib/python` directory you can run

```bash
python hermes.py
```

You will be thrown into an IPython console from which additional queries can be run.

## Running the Rust client library

Put the following into your ~/.bashrc

```bash
export LD_LIBRARY_PATH=/home/pcmoritz/hermes/target/debug/:$LD_LIBRARY_PATH
```

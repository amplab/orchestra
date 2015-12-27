# Hermes

## Setup
The instructions below suffice to run the example code on Ubuntu instance on EC2.

Install dependencies

- `sudo apt-get update`
- `sudo apt-get install -y emacs git gcc libzmq3-dev python2.7-dev python-pip`
- `sudo pip install numpy`
- `sudo pip install protobuf`

Install rust (we currently need the nightly build)

- `curl -sSf https://static.rust-lang.org/rustup.sh | sh -s -- --channel=nightly`

Build protobuf compiler

- `sudo apt-get install protobuf-compiler`
- `cd ~`
- `git clone https://github.com/stepancheg/rust-protobuf.git`
- `cd rust-protobuf`
- `cargo build`
- add the line `export PATH=$HOME/rust-protobuf/target/debug:$PATH` to `~/.bashrc`
- `source ~/.bashrc`

Clone Hermes and create schema

- `cd ~`
- `git clone https://github.com/pcmoritz/hermes.git`
- `cd hermes/schema`
- `bash make-schema.sh`
- `cd $HOME/hermes`
- add `export LD_LIBRARY_PATH=$HOME/hermes/target/debug/:$LD_LIBRARY_PATH` to `~/.bashrc`

Add Hermes to your python path

- add `export PYTHONPATH=$HOME/hermes/lib/python:$PYTHON_PATH` to `~/.bashrc `
- `source ~/.bashrc`

## Running the example code

In one terminal, do

-`cd ~/hermes`
- `RUST_BACKTRACE=1 RUST_LOG=hermes=info cargo run --bin hermes`

In another terminal, do

- `cd ~/hermes/examples`
- `python matrix_multiply.py 1235`

In another terminal, do

- `cd ~/hermes/examples`
- `python matrix_multiply.py 1236`

Finally, in another terminal, do

- `cd ~/hermes/lib/python`
- `python hermes.py`

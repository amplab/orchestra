# Orchestra

Orchestra is work in progress; all the functionality that is implemented is
expected to work, if you run into problems you should file an issue on github.
While performance is an explicit goal of this project, so far most focus has
been on correctness rather than performance.

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

Clone Orchestra and create schema

- `cd ~`
- `git clone https://github.com/amplab/orchestra.git`
- `cd orchestra/schema`
- `bash make-schema.sh`
- `cd $HOME/orchestra`
- add `export LD_LIBRARY_PATH=$HOME/orchestra/target/debug/:$LD_LIBRARY_PATH` to `~/.bashrc`

Add Orchestra to your python path

- add `export PYTHONPATH=$HOME/orchestra/lib/python:$PYTHON_PATH` to `~/.bashrc `
- `source ~/.bashrc`

## Running the example code

In one terminal, do

-`cd ~/orchestra`
- `RUST_BACKTRACE=1 RUST_LOG=orchestra=info cargo run --bin orchestra`

In another terminal, do

- `cd ~/orchestra/examples`
- `python matrix_multiply.py 1235`

In another terminal, do

- `cd ~/orchestra/examples`
- `python matrix_multiply.py 1236`

Finally, in another terminal, do

- `cd ~/orchestra/lib/python`
- `python orchestra.py`

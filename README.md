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

Install cprotobuf

- `cd ~`
- `git clone https://github.com/pcmoritz/cprotobuf.git`
- `cd cprotobuf`
- `python setup.py install`

Clone Orchestra and create schema

- `cd ~`
- `git clone https://github.com/amplab/orchestra.git`
- `cd orchestra/schema`
- `bash make-schema.sh`
- `cd $HOME/orchestra`
- add `export LD_LIBRARY_PATH=$HOME/orchestra/target/debug/:$LD_LIBRARY_PATH` to `~/.bashrc`

Build orchpy
- cd `~/orchestra/lib/orchpy/`
- `python setup.py build`
- add something like `export PYTHONPATH=PATH_TO_ORCHESTRA/orchestra/lib/orchpy/build/lib.linux-x86_64-2.7:$PYTHONPATH` to `~/.bashrc`, this will vary depending on your operating system
- `source ~/.bashrc`

Add Orchestra to your python path

- add `export PYTHONPATH=$HOME/orchestra/lib/python:$PYTHON_PATH` to `~/.bashrc `
- `source ~/.bashrc`

## Running the tests

In a terminal, run

- `cd ~/orchestra/test`
- `RUST_LOG=hermes=info python runtest.py`

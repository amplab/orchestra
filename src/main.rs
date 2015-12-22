#![feature(convert)]
#![feature(custom_derive)]
#![feature(deque_extras)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
extern crate petgraph;
extern crate protobuf;
extern crate zmq;

pub mod comm;
mod types;
mod graph;
pub mod server;
pub mod scheduler;
pub mod utils;

fn main() {
    env_logger::init().unwrap();
    let mut server = server::Server::new();
    server.main_loop();
}

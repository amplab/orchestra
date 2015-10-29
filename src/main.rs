#![feature(convert)]
#![feature(custom_derive)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
extern crate petgraph;
extern crate protobuf;
extern crate nanomsg;

mod comm;
mod types;
mod graph;
mod server;
mod utils;

use nanomsg::{Socket, Protocol};
use std::fs::File;

use server::Server;

fn main() {
    env_logger::init().unwrap();

    info!("establishing connection");
    // let mut socket = Socket::new(Protocol::Req).ok().unwrap();
    // socket.connect("tcp://127.0.0.1:1234").ok().unwrap();
    let mut socket = Socket::new(Protocol::Rep).ok().unwrap();
    socket.bind("tcp://127.0.0.1:1234").ok().unwrap();
    let mut out_file = File::create("/tmp/graph.txt").ok().unwrap();

    let mut server = Server::new();
    loop {
        server.process_request(&mut socket);
        println!("back in the main loop");
        server.dump(&mut out_file);
    }
}

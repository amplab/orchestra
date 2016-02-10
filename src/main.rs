#![feature(ip_addr)]
#![feature(convert)]
#![feature(custom_derive)]
#![feature(deque_extras)]

#[macro_use]
extern crate log;
extern crate argparse;
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

use argparse::{ArgumentParser, Store};

fn main() {
    let mut incoming_port = 0;
    let mut publish_port = 0;
    let mut setup_port = 0;
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Orchestra server");
        ap.refer(&mut incoming_port).add_argument("incoming_port", Store, "port for incoming requests");
        ap.refer(&mut publish_port).add_argument("publish_port", Store, "port for message broadcasting");
        ap.refer(&mut setup_port).add_argument("setup_port", Store, "port for setting up broadcasting");
        ap.parse_args_or_exit();
    }
    env_logger::init().unwrap();
    let mut server = server::Server::new(publish_port);
    server.main_loop(incoming_port, setup_port);
}

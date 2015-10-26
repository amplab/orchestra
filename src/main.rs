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
use std::io::{Write};
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



    /*
    let num_workers = 4;
    let n = server::make_int(num_workers);
    let mut server = Server::new();
    let objref = server.add_obj(n);

    info!("calling initial function");
    server.add_call("create_dist_matrix".to_string(), &vec!(objref));
    server::send_function_call(&mut socket, "create_dist_matrix".to_string(), vec!(objref));
    socket.flush().ok().unwrap();
    server::receive_client_message(&mut socket); // receive ack
    info!("received client ack");
    let obj = server.get_obj(objref);
    server::send_argument(&mut socket, objref, obj);
    loop {
        let msg = server::receive_client_message(&mut socket);
        if msg.get_field_type() != comm::ClientMessage_Type::TYPE_INVOKE {
            info!("shutting down");
            break;
        } else {
            info!("received another function call");
            let args : Vec<usize> = msg.get_call().get_args().iter().map(|arg| *arg as usize).collect();
            server.add_call(msg.get_call().get_name().to_string(), args.as_slice());
            // this is where scheduling should happen
            server::send_argument(&mut socket, 0, server::make_int(42));
        }
    }

    server.dump(&mut out_file);
    */
}

use comm;
use protobuf;
use protobuf::Message;
use protobuf::core::MessageStatic;
use zmq;
use zmq::{Socket};
use std::io::Cursor;
use std::ops::{Deref};
use std::collections::HashMap;
use rand;
use rand::distributions::{IndependentSample, Range};

/// A unique identifier for an object stored on one of the workers.
pub type ObjRef = u64;
/// A unique identifier for a worker.
pub type WorkerID = usize;
/// For each object, contains a vector of worker ids that hold the object.
pub type ObjTable = Vec<Vec<WorkerID>>;
/// For each function, contains a sorted vector of worker ids that can execute the function.
pub type FnTable = HashMap<String, Vec<WorkerID>>;

/// Given a predicate `absent` that can test if an object is unavailable on the client, compute
/// which objects fom `args` still need to be send so the function call can be invoked.
pub fn args_to_send<F : Fn(ObjRef) -> bool>(args: &[ObjRef], absent: F) -> Vec<ObjRef> {
  let mut scratch = args.to_vec();
  scratch.sort();
  // deduplicate
  let mut curr = 0;
  for i in 0..scratch.len() {
    let arg = scratch[i];
    if i > 0 && arg == scratch[i-1] {
      continue;
    }
    if absent(arg) {
      scratch[curr] = arg;
      curr += 1
    }
  }
  scratch.truncate(curr);
  return scratch
}

#[test]
fn test_args_to_send() {
  let args = vec![1, 4, 5, 5, 2, 2, 3, 3];
  let present = vec![1, 2, 4];
  let res = args_to_send(&args, |objref| present.binary_search(&objref).is_err());
  assert_eq!(res, vec!(3, 5));
}

pub fn push_objrefs(args: &comm::Args, result: &mut Vec<ObjRef>) {
  for elem in args.get_objrefs() {
    if *elem >= 0 {
      result.push(*elem as u64);
    }
  }
}

/// Send a protocol buffer message on a socket.
pub fn send_message(socket: &mut Socket, message: &mut comm::Message) {
  let mut buf = Vec::new();
  message.write_to_vec(&mut buf).unwrap();
  socket.send(buf.as_slice(), 0).unwrap();
}

/// Receive a protocol buffer message over a socket.
pub fn receive_message(socket: &mut Socket) -> comm::Message {
  let mut msg = zmq::Message::new().unwrap();
  socket.recv(&mut msg, 0).unwrap();
  let mut input_stream = protobuf::CodedInputStream::from_bytes(msg.deref());
  return protobuf::core::parse_from::<comm::Message>(&mut input_stream).unwrap();
}

/// Receive a protocol buffer message through a subscription socket.
pub fn receive_subscription(subscriber: &mut Socket) -> comm::Message {
  let mut msg = zmq::Message::new().unwrap();
  subscriber.recv(&mut msg, 0).unwrap();
  let mut read_buf = Cursor::new(msg.as_mut());
  read_buf.set_position(7);
  return protobuf::parse_from_reader(&mut read_buf).unwrap();
}

/// Send an acknowledgement package.
pub fn send_ack(socket: &mut Socket) {
  let mut ack = comm::Message::new();
  ack.set_field_type(comm::MessageType::ACK);
  send_message(socket, &mut ack);
}

/// Receive an acknowledgement package.
pub fn receive_ack(socket: &mut Socket) {
  let ack = receive_message(socket);
  assert!(ack.get_field_type() == comm::MessageType::ACK);
}

/// Bind a ZeroMQ socket to specific address. If port is None, connect to a free port. Return port.
pub fn bind_socket(socket: &mut Socket, host: &str, port: Option<u64>) -> u64 {
  match port {
    None => {
      loop {
        let mut rng = rand::thread_rng();
        let range = Range::new(2048, 65535);
        let port = range.ind_sample(&mut rng);
        match socket.bind(&format!("tcp://{}:{}", host, port)[..]) {
          Ok(()) => { return port },
          Err(err) => { continue }
        }
      }
    }
    Some(port) => {
      match socket.bind(&format!("tcp://{}:{}", host, port)[..]) {
        Ok(()) => { return port },
        Err(err) => { panic!("Could not bind socket. Make sure port {} is not used yet. {}", port, err) }
      }
    }
  }
}

/// Connect a ZeroMQ socket to specific address
pub fn connect_socket(socket: &mut Socket, host: &str, port: u64) {
    match socket.connect(&format!("tcp://{}:{}", host, port)[..]) {
        Ok(()) => {},
        Err(_) => { panic!("Could not connect socket. Make sure port is set correctly.") }
    }
}

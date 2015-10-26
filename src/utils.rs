use protobuf;
use protobuf::Message;
use protobuf::core::MessageStatic;
use nanomsg::Socket;
use std::io::{Read, Write};

pub type ObjRef = usize;

// Given a predicate `absent` that can test if an object is unavailable on the client, compute which
// objects fom `args` still need to be send so the function call can be invoked.
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

pub fn make_message<M: Message>(message: &M) -> Vec<u8> {
    let mut buf : Vec<u8> = Vec::new();
    message.write_to_writer(&mut buf).unwrap();
    return buf;
}

pub fn send_message<M: Message>(socket: &mut Socket, message: &mut M) {
    let buff = make_message::<M>(message);
    socket.write_all(buff.as_slice()).unwrap();
    socket.flush().unwrap();
}

pub fn receive_message<M : MessageStatic>(socket: &mut Socket) -> M {
    use std::io::Cursor;
    let mut buf = Vec::new();
    socket.read_to_end(&mut buf).unwrap();
    let mut read_buf = Cursor::new(buf);
    return protobuf::parse_from_reader::<M>(&mut read_buf).unwrap();
}

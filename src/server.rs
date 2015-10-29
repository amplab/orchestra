extern crate nanomsg;

use comm;
use graph;

use utils::{send_message, receive_message};
use utils::ObjRef;
use graph::CompGraph;
use rand;
use rand::distributions::{IndependentSample, Range};
use std::io::Write;
use std::io::Read;
use nanomsg::{Socket, Protocol};
use std::process;

pub struct Conn {
    socket: Socket,
    addr: String
}

pub struct Server<'a> {
    objstore: Vec<Vec<u8>>, // the actual data will be replaced by references to nodes that hold the data
    graph: graph::CompGraph<'a>, // computation graph for this server
    workers: Vec<Conn>, // workers that have been registered with this server
}

impl<'a> Server<'a> {
    pub fn new() -> Server<'a> {
        Server { objstore: vec!(), graph: CompGraph::new(), workers: vec!() }
    }
    // add new object to store and return objref
    pub fn add_obj<'b>(self: &'b mut Server<'a>, data: Vec<u8>) -> ObjRef {
        let (objref, _) = self.graph.add_obj();
        assert!(objref == self.objstore.len());
        self.objstore.push(data);
        return objref;
    }
    // store data in an object slot
    pub fn store_obj<'b>(self: &'b mut Server<'a>, objref: ObjRef, data: Vec<u8>) {
        self.objstore[objref] = data;
    }
    // get object corresponding to objref (for now)
    pub fn get_obj(self: &'a Server<'a>, objref: ObjRef) -> Vec<u8> {
        return self.objstore[objref].to_vec();
    }
    // add a new call to the computation graph
    pub fn add_call<'b>(self: &'b mut Server<'a>, fnname: String, args: &'b [ObjRef]) -> ObjRef {
        let result = self.add_obj(vec!());
        self.graph.add_op(fnname, args, result);
        return result;
    }
    // dump the computation graph to a .dot file
    pub fn dump<'b>(self: &'b mut Server<'a>, out: &'b mut Write) {
        let res = graph::to_dot(&self.graph);
        out.write(res.as_bytes()).unwrap();
    }

    fn schedule<'b>(self: &'b mut Server<'a>) -> Conn {
        let n = self.workers.len();
        if n < 1 {
            error!("no worker registered");
            process::exit(1);
        }
        let mut rng = rand::thread_rng(); // supposed to have no performance penalty
        let range = Range::new(0, self.workers.len());
        let workerid = range.ind_sample(&mut rng);
        let worker = self.workers.swap_remove(workerid);
        info!("chose worker {}", worker.addr);
        return worker;
    }

    // returns the object reference to the result
    pub fn handle_invoke<'b>(self: &'b mut Server<'a>, msg: comm::ClientMessage) -> ObjRef {
        let args : Vec<usize> = msg.get_call().get_args().iter().map(|arg| *arg as usize).collect();
        // handle reduce here
        let objref = self.add_call(msg.get_call().get_name().to_string(), args.as_slice());
        let mut worker = self.schedule(); // remove the worker...
        let result = self.call_remote_function(&mut worker, msg.get_call().get_name().to_string(), args.as_slice(), objref);
        // store data
        self.store_obj(objref, result);
        self.workers.push(worker); // ...and put it back in
        return objref;
    }

    // process request by client
    pub fn process_request<'b>(self: &'b mut Server<'a>, socket: &'b mut Socket) {
        let msg = receive_message::<comm::ClientMessage>(socket);
        match msg.get_field_type() {
            comm::ClientMessage_Type::TYPE_INVOKE => {
                info!("received function call {}", msg.get_call().get_name());
                let objref = self.handle_invoke(msg);
                let mut message = comm::ServerMessage::new();
                message.set_field_type(comm::ServerMessage_Type::TYPE_DONE);
                message.set_objref(objref as u64);
                send_message(socket, &mut message);
            },
            comm::ClientMessage_Type::TYPE_REGISTER => {
                info!("registered new worker");
                let mut socket = Socket::new(Protocol::Req).ok().unwrap();
                socket.connect(msg.get_address()).ok().unwrap();
                self.workers.push(Conn{socket: socket, addr: msg.get_address().to_string()});
            },
            comm::ClientMessage_Type::TYPE_PULL => {
                info!("pull request");
                let objref = msg.get_objref() as usize;
                let data = self.get_obj(objref);
                send_argument(socket, objref, data);
            },
            comm::ClientMessage_Type::TYPE_ACK => {
                error!("'ack' message not allowed in this state");
                process::exit(1);
            }
            comm::ClientMessage_Type::TYPE_DONE => {
                error!("'done' message not allowed in this state");
                process::exit(1);
            }
            comm::ClientMessage_Type::TYPE_PUSH => {
                error!("'push' message not allowed in this state");
                process::exit(1);
            }
        }
    }
    // call a remote function on a client which is connected to the socket
    pub fn call_remote_function<'b>(self: &'b mut Server<'a>, worker: &mut Conn, name: String, args: &'b [ObjRef], result: ObjRef) -> Vec<u8> {
        let addr = worker.addr.to_string();
        let socket = &mut worker.socket;
        info!("calling function {}", name.as_str());
        send_function_call(socket, name, args.iter().map(|x| *x).collect(), result); // this is weird I think, what is a better way?
        socket.flush().ok().unwrap();
        let msg = receive_message::<comm::ClientMessage>(socket);
        if msg.get_field_type() == comm::ClientMessage_Type::TYPE_ACK {
            info!("'ack' received");
        } else {
            error!("message not expected");
            process::exit(1);
        }
        // don't send duplicate arguments
        let mut sorted_args = args.to_vec();
        sorted_args.sort();
        for (i, arg) in sorted_args.iter().enumerate() {
            if i > 0 && *arg == sorted_args[i-1] {
                continue;
            }
            let data = self.get_obj(*arg);
            send_argument(socket, *arg, data);
            info!("sending argument {} for function", *arg);
            let msg = receive_message::<comm::ClientMessage>(socket);
            if msg.get_field_type() == comm::ClientMessage_Type::TYPE_ACK {
                 info!("'ack' for argument received");
            } else {
                error!("message not expected");
                process::exit(1);
            }
        }

        let mut msg = comm::ServerMessage::new();
        msg.set_field_type(comm::ServerMessage_Type::TYPE_DONE);
        send_message(socket, &mut msg); // todo: remove the 'mut' here

        // handle function calls from client
        loop {
            let msg = receive_message::<comm::ClientMessage>(socket);
            match msg.get_field_type() {
                comm::ClientMessage_Type::TYPE_DONE => {
                    info!("'done' message received");
                    break
                },
                comm::ClientMessage_Type::TYPE_INVOKE => {
                    // maybe factor this into handle_invoke to unify with the one in the main loop
                    let objref = self.handle_invoke(msg);
                    let mut message = comm::ServerMessage::new();
                    message.set_field_type(comm::ServerMessage_Type::TYPE_DONE);
                    message.set_objref(objref as u64);
                    send_message(socket, &mut message);
                }
                _ => {
                    info!("{:?}", msg.get_field_type());
                    error!("not expected");
                    process::exit(1);
                }
            }
        }
        return pull_obj_from_client(socket, &addr[..], result);
    }
}

// for now, pull it to the server using the already existing socket. Later, it will be distributed amongst other workers
// TODO: factor stuff out into send_server_message
pub fn pull_obj_from_client(socket: &mut Socket, address: &str, objref: ObjRef) -> Vec<u8> {
    let mut message = comm::ServerMessage::new();
    message.set_field_type(comm::ServerMessage_Type::TYPE_PULL);
    message.set_address(address.to_string());
    message.set_objref(objref as u64);
    info!("send pull request");
    send_message(socket, &mut message);
    // accept answer
    let answer = receive_message::<comm::ClientMessage>(socket);
    info!("received pull request");
    // assert that we actually have a PUSH object
    let result = answer.get_blob().get_data().iter().map(|x| *x).collect();
    return result;
}

pub fn send_function_call(socket: &mut Socket, name: String, arguments: Vec<ObjRef>, result: ObjRef) {
    let mut message = comm::ServerMessage::new();
    message.set_field_type(comm::ServerMessage_Type::TYPE_INVOKE);
    let mut call = comm::Call::new();
    call.set_name(name);
    call.set_args(arguments.iter().map(|arg| *arg as u64).collect());
    call.set_result(result as u64);
    message.set_call(call);
    send_message(socket, &mut message);
}

// TODO: rename this to reflect more general data transfer
pub fn send_argument(socket: &mut Socket, objref: ObjRef, data: Vec<u8>) {
    let mut message = comm::ServerMessage::new();
    let mut blob = comm::Blob::new();
    blob.set_data(data);
    blob.set_objref(objref as u64);
    message.set_field_type(comm::ServerMessage_Type::TYPE_PUSH);
    message.set_blob(blob);
    send_message(socket, &mut message);
}

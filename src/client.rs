use std::collections::HashMap;
use nanomsg::{Socket, Protocol};

use comm;
use utils::{ObjRef, receive_message, send_message};
use libc::{uint64_t, c_void};
use std::thread;

pub type FnRef = usize; // Index of locally registered function

pub struct Context {
    client: Client,
    socket: Socket
}

impl Context {
    pub fn new(server_addr: String, client_addr: String) -> Context {
        let mut socket = Socket::new(Protocol::Req).ok().unwrap();
        let mut endpoint = socket.connect(&server_addr[..]).ok().unwrap();
        if client_addr != "" {
            println!("Connecting");
            let mut reg = comm::ClientMessage::new();
            reg.set_field_type(comm::ClientMessage_Type::TYPE_REGISTER);
            reg.set_address(client_addr.clone());
            let mut clientsocket = Socket::new(Protocol::Rep).ok().unwrap();
            clientsocket.bind(&client_addr[..]).ok().unwrap();
            thread::sleep_ms(10);
            send_message::<comm::ClientMessage>(&mut socket, &mut reg);
            endpoint.shutdown().unwrap();
            socket = clientsocket;
        }
        Context { client: Client::new(), socket: socket }
    }
    pub fn client<'b>(self: &'b mut Context) -> &mut Client {
        return &mut self.client;
    }
    pub fn socket<'b>(self: &'b mut Context) -> &mut Socket {
        return &mut self.socket;
    }
}

pub struct Client {
    objects: HashMap<ObjRef, Vec<u8>>, // mapping from objrefs to data
    functions: HashMap<String, FnRef>, // mapping from function name to interpreter-local function reference
    types: HashMap<String, i32>, // mapping from type name to type id

    state: Option<comm::Call>, // Some(call) if function call has just been evaluated and None otherwise
    function: FnRef, // function that is currently active
    args: Vec<ObjRef>, // objrefs to the arguments of current function call
    result: ObjRef // objref of result from current function call
}

impl Client {
    pub fn new() -> Client {
        Client { objects: HashMap::new(), functions: HashMap::new(), types: HashMap::new(), state: None, function: 0, args: Vec::new(), result: 0 }
    }
    pub fn add_object<'b>(self: &'b mut Client, objref: ObjRef, data: Vec<u8>) {
        self.objects.insert(objref, data);
    }
    // DEPRECATED
    pub fn get_object<'b>(self: &'b mut Client, objref: ObjRef) -> Option<Vec<u8>> {
        return self.objects.get(&objref).and_then(|data| Some(data.to_vec()));
    }
    pub fn get_num_args<'b>(self: &'b Client) -> usize {
        return self.args.len();
    }
    pub fn add_function<'b>(self: &'b mut Client, name: String) -> usize {
        println!("registering function {}", name);
        let idx = self.functions.len();
        self.functions.insert(name, idx);
        return idx;
    }
    pub fn get_function<'b>(self: &'b Client) -> FnRef {
        return self.function;
    }
    pub fn get_arg_len<'b>(self: &'b Client, argidx: usize) -> Option<usize> {
        return self.objects.get(&self.args[argidx]).and_then(|data| Some(data[..].len()));
    }
    pub fn get_arg_ptr<'b>(self: &'b Client, argidx: usize) -> Option<*const u8> {
        return self.objects.get(&self.args[argidx]).and_then(|data| Some(data[..].as_ptr()));
    }
    pub fn add_type<'b>(self: &'b mut Client, name: String) {
        let index = self.types.len();
        self.types.insert(name, index as i32);
    }
    pub fn get_type<'b>(self: &'b mut Client, name: String) -> Option<i32> {
        return self.types.get(&name).and_then(|&num| Some(num));
    }
    pub fn remote_call_function<'b>(self: &'b mut Client, socket: &mut Socket, name: String, args: &[ObjRef]) -> ObjRef {
        let mut msg = comm::ClientMessage::new();
        msg.set_field_type(comm::ClientMessage_Type::TYPE_INVOKE);
        let mut call = comm::Call::new();
        call.set_name(name);
        call.set_args(args.iter().map(|arg| *arg as u64).collect());
        msg.set_call(call);
        send_message::<comm::ClientMessage>(socket, &mut msg);
        let answer = receive_message::<comm::ServerMessage>(socket);
        return answer.get_objref() as usize;
    }
    pub fn receive_args_and_prepare_call<'b>(self: &'b mut Client, socket: &mut Socket, name: String, args: &[ObjRef], result: ObjRef) {
        // let reqargs = utils::args_to_send(args, |objref| !self.objects.contains_key(&objref));
        loop {
            println!("waiting for argument");
            let message = receive_message::<comm::ServerMessage>(socket);
            if message.get_field_type() == comm::ServerMessage_Type::TYPE_DONE {
                break
            }
            let blob = message.get_blob();
            let objref = blob.get_objref() as usize;
            println!("received argument objref {}", objref);
            self.add_object(objref, blob.get_data().to_vec());
            send_ack(socket);
        }
        self.args = args.to_vec();
        self.function = self.functions.get(&name).expect("function not found").clone();
        self.result = result;
    }
    pub fn finish_request<'b>(self: &'b mut Client, socket: &'b mut Socket) {
        match self.state.clone() { // TODO: remove the clone
            Some(call) => {
                let mut done = comm::ClientMessage::new();
                done.set_field_type(comm::ClientMessage_Type::TYPE_DONE);
                done.set_call(call.clone());
                send_message::<comm::ClientMessage>(socket, &mut done);
            }
            None => {}
        }
    }
    // process request by server; return value tells us if control should be given back to the
    // interpreter, is required to set the self.state variable to an appropriate value
    // returns if control should be given back to the interpreter
    pub fn process_request<'b>(self: &'b mut Client, socket: &'b mut Socket) -> (bool, ObjRef) {
        let msg = receive_message::<comm::ServerMessage>(socket);
        match msg.get_field_type() {
            comm::ServerMessage_Type::TYPE_INVOKE => {
                let call = msg.get_call();
                println!("server called {}", call.get_name().to_string());
                // send ack
                let mut ack = comm::ClientMessage::new();
                ack.set_field_type(comm::ClientMessage_Type::TYPE_ACK);
                ack.set_call(call.clone());
                send_message::<comm::ClientMessage>(socket, &mut ack);
                let args : Vec<usize> = call.get_args().iter().map(|arg| *arg as usize).collect();
                self.receive_args_and_prepare_call(socket, call.get_name().to_string(), &args[..], call.get_result() as ObjRef);
                self.state = Some(call.clone());
                return (true, call.get_result() as ObjRef)
            }
            comm::ServerMessage_Type::TYPE_PUSH => {
                // let blob = msg.get_blob();
                // self.add_obj(blob.get_objref() as usize, blob.get_data().iter().map(|x| *x).collect());
            }
            comm::ServerMessage_Type::TYPE_PULL => {
                println!("processing pull request");
                let objref = msg.get_objref();
                match self.get_object(msg.get_objref() as usize) {
                    Some(data) => {
                        let mut msg = comm::ClientMessage::new();
                        msg.set_field_type(comm::ClientMessage_Type::TYPE_PUSH);
                        let mut blob = comm::Blob::new();
                        blob.set_objref(objref);
                        blob.set_data(data);
                        msg.set_blob(blob);
                        send_message::<comm::ClientMessage>(socket, &mut msg);
                    }
                    None => {
                        println!("objref not found on this client")
                    }
                }
            }
            comm::ServerMessage_Type::TYPE_DONE => {
            }
        }
        self.state = None;
        return (false, 0)
    }
}

pub fn send_ack(socket: &mut Socket) {
    let mut ack = comm::ClientMessage::new();
    ack.set_field_type(comm::ClientMessage_Type::TYPE_ACK);
    send_message::<comm::ClientMessage>(socket, &mut ack);
}

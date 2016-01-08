use std::collections::HashMap;
use zmq;
use zmq::{Socket};

use comm;
use utils::{ObjRef, WorkerID, receive_message, send_message, receive_subscription, send_ack, receive_ack, connect_socket};
use std::thread;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};

pub type FnRef = usize; // Index of locally registered function
pub type ObjStore = HashMap<ObjRef, Vec<u8>>; // collection of objects stored on the client

pub enum Event {
    Obj(ObjRef), // a new object becomes available
    Invoke(comm::Call), // a new job request
    Debug(comm::Message) // for debugging purposes
}

#[derive(Clone, PartialEq)]
pub enum State {
    Processing {
        call: comm::Call,
        deps: Vec<ObjRef> // sorted
    },
    Waiting
}

pub struct Context {
    zmq_ctx: zmq::Context,

    objects: Arc<Mutex<ObjStore>>, // mapping from objrefs to data
    functions: HashMap<String, FnRef>, // mapping from function name to interpreter-local function reference
    types: HashMap<String, i32>, // mapping from type name to type id

    state: State, // Some(call) if function call has just been evaluated and None otherwise
    function: FnRef, // function that is currently active
    args: Vec<ObjRef>, // objrefs to the arguments of current function call

    notify_main: Receiver<Event>, // reply thread signals main thread
    request: Socket,
    workerid: WorkerID
}

impl Context {
    pub fn start_reply_thread(zmq_ctx: &mut zmq::Context, client_addr: &str, notify_main: Sender<Event>, objects: Arc<Mutex<ObjStore>>) {
        let mut reply = zmq_ctx.socket(zmq::REP).unwrap();
        reply.bind(client_addr).unwrap();

        thread::spawn(move || {
            loop {
                let msg = receive_message(&mut reply);
                match msg.get_field_type() {
                    comm::MessageType::PUSH => {
                        let blob = msg.get_blob();
                        let objref = blob.get_objref();
                        {
                            objects.lock().unwrap().insert(objref, blob.get_data().to_vec());
                        }
                        send_ack(&mut reply);
                        notify_main.send(Event::Obj(objref)).unwrap();
                    },
                    comm::MessageType::INVOKE => {
                        notify_main.send(Event::Invoke(msg.get_call().clone())).unwrap();
                        send_ack(&mut reply);
                    },
                    _ => {
                        error!("error, got {:?}", msg.get_field_type());
                        error!("{:?}", msg.get_address());
                    }
                }
            }
        });
    }
    pub fn new(server_addr: String, client_addr: String, publish_port: u64) -> Context {
        let mut zmq_ctx = zmq::Context::new();

        let mut request = zmq_ctx.socket(zmq::REQ).unwrap();
        request.connect(&server_addr[..]).unwrap();

        let (reply_sender, reply_receiver) = mpsc::channel(); // TODO: rename this

        info!("connecting to server...");
        let mut reg = comm::Message::new();
        reg.set_field_type(comm::MessageType::REGISTER_CLIENT);
        reg.set_address(client_addr.clone());

        let objects = Arc::new(Mutex::new(HashMap::new()));

        Context::start_reply_thread(&mut zmq_ctx, &client_addr[..], reply_sender.clone(), objects.clone());

        thread::sleep_ms(10);

        send_message(&mut request, &mut reg);
        let ack = receive_message(&mut request);
        let workerid = ack.get_workerid() as WorkerID;
        info!("my workerid is {}", workerid);
        let setup_port = ack.get_setup_port();
        info!("setup port is {}", setup_port);

        // the network thread listens to commands on the master subscription channel and serves the other client channels with data. It notifies the main thread if new data becomes available.

        // do handshake with server and get client id, so we can communicate on the subscription channel

        // let (main_sender, main_receiver) = mpsc::channel();
        // let (network_sender, network_receiver) = mpsc::channel();
        let mut clients: HashMap<String, Socket> = HashMap::new(); // other clients that are part of the cluster

        let thread_objects = objects.clone();

        thread::spawn(move || {
            let mut zmq_ctx = zmq::Context::new();
            let mut subscriber = Context::connect_network_thread(&mut zmq_ctx, workerid, setup_port, publish_port);

            loop {
                let msg = receive_subscription(&mut subscriber);

                match msg.get_field_type() {
                    comm::MessageType::REGISTER_CLIENT => {
                        // push onto workers
                        info!("connecting to client {}", msg.get_address());
                        let mut other = zmq_ctx.socket(zmq::REQ).unwrap();
                        other.connect(msg.get_address()).ok().unwrap();
                        clients.insert(msg.get_address().into(), other);
                        // new code: create new thread, insert it here
                    }
                    comm::MessageType::DELIVER => {
                        let objref = msg.get_objref();
                        let mut answer = comm::Message::new();
                        answer.set_field_type(comm::MessageType::PUSH);
                        let mut blob = comm::Blob::new();
                        blob.set_objref(objref);
                        let data = {
                            let objs : MutexGuard<HashMap<ObjRef, Vec<u8>>> = thread_objects.lock().unwrap();
                            objs.get(&objref).and_then(|data| Some(data.to_vec())).expect("data not available on this client")
                        };
                        blob.set_data(data);
                        answer.set_blob(blob);
                        let target = &mut clients.get_mut(msg.get_address()).expect("target client not found"); // shouldn't happen
                        send_message(target, &mut answer);
                        receive_ack(target);
                    },
                    comm::MessageType::DEBUG => {
                        reply_sender.send(Event::Debug(msg)).unwrap();
                    },
                    _ => {}
                }
            }
        });

        return Context {
            zmq_ctx: zmq_ctx,
            objects: objects.clone(), functions: HashMap::new(), types: HashMap::new(),
            state: State::Waiting, function: 0, args: Vec::new(),
            notify_main: reply_receiver,
            request: request,
            workerid: workerid
        }
    }

    fn connect_network_thread(zmq_ctx: &mut zmq::Context, workerid: WorkerID, setup_port: u64, subscriber_port: u64) -> Socket {
        let mut subscriber = zmq_ctx.socket(zmq::SUB).unwrap();
        info!("subscriber_port {}", subscriber_port);
        connect_socket(&mut subscriber, "localhost", subscriber_port);
        subscriber.set_subscribe(format!("{:0>#07}", workerid).as_bytes()).unwrap();

        let mut setup = zmq_ctx.socket(zmq::REQ).unwrap();
        connect_socket(&mut setup, "localhost", setup_port);
        info!("setup_port {}", setup_port);
        thread::sleep_ms(10);
        // set up sub/pub socket
        let mut msg = zmq::Message::new().unwrap();
        subscriber.recv(&mut msg, 0).unwrap();

        setup.send(b"joining", 0).unwrap();

        info!("accepted server invitation");

        return subscriber
    }

    pub fn add_object<'b>(self: &'b mut Context, objref: ObjRef, data: Vec<u8>) {
        self.objects.lock().unwrap().insert(objref, data);
    }
    pub fn get_num_args<'b>(self: &'b Context) -> usize {
        return self.args.len();
    }
    pub fn add_function<'b>(self: &'b mut Context, name: String) -> usize {
        info!("registering function {}", name);
        let idx = self.functions.len();
        self.functions.insert(name.to_string(), idx);

        let mut msg = comm::Message::new();
        msg.set_field_type(comm::MessageType::REGISTER_FUNCTION);
        msg.set_fnname(name.to_string());
        msg.set_workerid(self.workerid as u64);
        send_message(&mut self.request, &mut msg);
        receive_ack(&mut self.request);

        return idx;
    }
    pub fn get_function<'b>(self: &'b Context) -> FnRef {
        return self.function;
    }
    // TODO: Make this more efficient, i.e. use only one lookup
    pub fn get_obj_len<'b>(self: &'b Context, objref: ObjRef) -> Option<usize> {
        let result = self.objects.lock().unwrap().get(&objref).and_then(|data| Some(data[..].len()));
        return result;
    }
    pub fn get_obj_ptr<'b>(self: &'b Context, objref: ObjRef) -> Option<*const u8> {
        let result = self.objects.lock().unwrap().get(&objref).and_then(|data| Some(data[..].as_ptr()));
        return result;
    }
    pub fn get_arg_len<'b>(self: &'b Context, argidx: usize) -> Option<usize> {
        return self.get_obj_len(self.args[argidx]);
    }
    pub fn get_arg_ptr<'b>(self: &'b Context, argidx: usize) -> Option<*const u8> {
        // println!("getting pointer for object {}", self.args[argidx]);
        return self.get_obj_ptr(self.args[argidx]);
    }
    pub fn add_type<'b>(self: &'b mut Context, name: String) {
        let index = self.types.len();
        self.types.insert(name, index as i32);
    }
    pub fn get_type<'b>(self: &'b mut Context, name: String) -> Option<i32> {
        return self.types.get(&name).and_then(|&num| Some(num));
    }
    pub fn remote_call_function<'b>(self: &'b mut Context, name: String, args: &[ObjRef]) -> ObjRef {
        let mut msg = comm::Message::new();
        msg.set_field_type(comm::MessageType::INVOKE);
        let mut call = comm::Call::new();
        call.set_name(name);
        call.set_args(args.to_vec());
        msg.set_call(call);
        send_message(&mut self.request, &mut msg);
        let answer = receive_message(&mut self.request);
        let result = answer.get_call().get_result();
        assert!(result.len() == 1);
        return result[0];
    }
    // TODO: Remove duplication between remote_call_function and remote_call_map
    pub fn remote_call_map<'b>(self: &'b mut Context, name: String, args: &[ObjRef]) -> Vec<ObjRef> {
        let mut msg = comm::Message::new();
        msg.set_field_type(comm::MessageType::INVOKE);
        let mut call = comm::Call::new();
        call.set_field_type(comm::Call_Type::MAP_CALL);
        call.set_name(name);
        call.set_args(args.to_vec());
        msg.set_call(call);
        send_message(&mut self.request, &mut msg);
        let answer = receive_message(&mut self.request);
        return answer.get_call().get_result().to_vec();
    }
    pub fn pull_remote_object<'b>(self: &'b mut Context, objref: ObjRef) -> ObjRef {
        {
            let objects = self.objects.lock().unwrap();
            if objects.contains_key(&objref) {
                return objref;
            }
        }
        let mut msg = comm::Message::new();
        msg.set_field_type(comm::MessageType::PULL);
        msg.set_objref(objref);
        msg.set_workerid(self.workerid as u64);
        send_message(&mut self.request, &mut msg);
        receive_ack(&mut self.request);
        loop {
            // println!("looping");
            match self.notify_main.recv().unwrap() {
                Event::Obj(pushedref) => {
                    if pushedref == objref {
                        return objref;
                    }
                }
                _ => {}
            }
        }
    }
    pub fn pull_debug_info<'b>(self: &'b mut Context) -> comm::Message {
        let mut msg = comm::Message::new();
        msg.set_field_type(comm::MessageType::DEBUG);
        msg.set_workerid(self.workerid as u64);
        send_message(&mut self.request, &mut msg);
        receive_ack(&mut self.request);
        loop {
            match self.notify_main.recv().unwrap() {
                Event::Debug(msg) => {
                    return msg;
                },
                _ => {}
            }
        }
    }
    pub fn finish_request<'b>(self: &'b mut Context) {
        match self.state.clone() { // TODO: remove the clone
            State::Processing{call, deps: _} => {
                let mut done = comm::Message::new();
                done.set_field_type(comm::MessageType::DONE);
                done.set_call(call.clone());
                done.set_workerid(self.workerid as u64);
                send_message(&mut self.request, &mut done);
                receive_ack(&mut self.request);
                self.state = State::Waiting;
            }
            State::Waiting => {}
        }
    }

    pub fn client_step<'b>(self: &'b mut Context) -> ObjRef {
        loop {
            match self.notify_main.recv().unwrap() {
                Event::Obj(objref) => {
                    // TODO: Make this more efficient:
                    // START
                    let mut acc = comm::Message::new();
                    acc.set_field_type(comm::MessageType::ACC);
                    acc.set_workerid(self.workerid as u64);
                    acc.set_objref(objref);
                    send_message(&mut self.request, &mut acc);
                    let answer = receive_message(&mut self.request);
                    // END
                    // if all elements for the current call are satisfied, evaluate it
                    match self.state {
                        State::Waiting => {},
                        State::Processing {call: _, ref mut deps} => {
                            match deps.binary_search(&objref) {
                                Ok(idx) => {
                                    deps.swap_remove(idx);
                                }
                                _ => {}
                            }
                        }
                    }
                },
                Event::Invoke(call) => {
                    info!("starting to evaluate {:?}", call.get_name());
                    assert!(self.state == State::Waiting);
                    let mut args = vec!();
                    {
                        let objects = self.objects.lock().unwrap();
                        for elem in call.get_args().iter() {
                            if !objects.contains_key(elem) {
                                args.push(*elem);
                            }
                        }
                    }
                    args.sort();
                    args.dedup();
                    info!("need args {:?}", args);
                    self.state = State::Processing{call: call.clone(), deps: args};
                    // if all elements for the current call are satisfied, evaluate it
                },
                _ => {}
            }

            match self.state {
                State::Waiting => {},
                State::Processing {ref call, ref deps} => {
                    if deps.len() == 0 {
                        // calling the function
                        self.args = call.get_args().to_vec();
                        let name = call.get_name().to_string();
                        self.function = self.functions.get(&name).expect("function not found").clone();
                        let result = call.get_result();
                        assert!(result.len() == 1);
                        return result[0]
                    }
                }
            }
        }
    }
}

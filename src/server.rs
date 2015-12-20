use comm;
use graph;

use utils::{send_message, receive_message, receive_ack, send_ack};
use utils::{WorkerID, ObjRef};
use graph::CompGraph;
use rand;
use rand::distributions::{IndependentSample, Range};
use std::io::{Read, Write};
use std::collections::VecDeque;
use zmq;
use zmq::Socket;
use std::process;

use protobuf::Message;

use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;
use std::sync::{Arc, RwLock, Mutex, MutexGuard};
use std::collections::HashMap;

pub type ObjTable = Vec<Vec<WorkerID>>; // for each object, contains a vector of workerids that hold the object
pub type FnTable = HashMap<String, Vec<WorkerID>>; // for each function, contains a sorted vector of workerids that can execute the function

pub struct Worker {
    addr: String,
    incoming: Sender<comm::Message> // for sending a new work request to this worker
}

pub enum Event {
    Worker(WorkerID), // a worker becomes available
    Obj(ObjRef), // a new object becomes available
    Job(comm::Call), // a job becomes available
    Pull(WorkerID, ObjRef) // a pull request was issued
}

pub struct WorkerPool {
    workers: Arc<Mutex<Vec<Worker>>>, // workers that have been registered with this pool
    event_notify: Sender<Event>, // notify the worker pool that a worker or job becomes available
    publish_notify: Sender<(WorkerID, comm::Message)>, // send delivery requests to clients
}

impl WorkerPool {
    pub fn new(objtable: Arc<Mutex<ObjTable>>, fntable: Arc<RwLock<FnTable>>) -> WorkerPool {
        let workers = Arc::new(Mutex::new(Vec::new()));
        let (event_sender, event_receiver) = mpsc::channel();
        let (publish_sender, publish_receiver) = mpsc::channel();
        WorkerPool::start_dispatch_thread(workers.clone(), objtable, fntable, event_receiver);
        WorkerPool::start_publisher_thread(publish_receiver);
        return WorkerPool { workers: workers, event_notify: event_sender, publish_notify: publish_sender }
    }
    pub fn start_publisher_thread(publish_notify: Receiver<(WorkerID, comm::Message)>) {
        thread::spawn(move || {
            let mut zmq_ctx = zmq::Context::new();
            let mut publisher = zmq_ctx.socket(zmq::PUB).unwrap();
            publisher.bind("tcp://*:5240").unwrap();
            loop {
                match publish_notify.recv().unwrap() {
                    (workerid, msg) => {
                        let mut buf = Vec::new();
                        write!(buf, "{:0>#07}", workerid).unwrap();
                        msg.write_to_writer(&mut buf).unwrap();
                        publisher.send(buf.as_slice(), 0).unwrap();
                    }
                }
            }
        });
    }

    // todo: just resend msg instead of reconstructing it
    pub fn send_function_call(worker: &Worker, job: comm::Call) {
        let mut msg = comm::Message::new();
        msg.set_field_type(comm::MessageType::INVOKE);
        msg.set_call(job);
        worker.incoming.send(msg).unwrap();
    }

    // todo: just resend msg instead of reconstructing it
    pub fn send_pull_request(worker: &Worker, workerid: WorkerID, objref: ObjRef) {
        let mut msg = comm::Message::new();
        msg.set_field_type(comm::MessageType::PULL);
        msg.set_workerid(workerid as u64);
        msg.set_objref(objref);
        worker.incoming.send(msg).unwrap();
    }

    // should be renamed to can_run
    pub fn dependencies_satisfied(job: &comm::Call, objtable: &MutexGuard<ObjTable>) -> bool {
        for objref in job.get_args() {
            if objtable[*objref as usize].len() == 0 {
                return false;
            }
        }
        return true;
    }

    // find job whose dependencies are met
    pub fn find_next_job(workerid: WorkerID, job_queue: &VecDeque<comm::Call>, objtable: &MutexGuard<ObjTable>, fntable: Arc<RwLock<FnTable>>) -> Option<usize> {
        for (i, job) in job_queue.iter().enumerate() {
            if fntable.read().unwrap()[job.get_name()].binary_search(&workerid).is_ok() && WorkerPool::dependencies_satisfied(job, objtable) {
                return Some(i);
            }
        }
        return None;
    }

    // TODO: replace fntable vector with bitfield
    pub fn find_next_worker(job: &comm::Call, worker_queue: &VecDeque<usize>, objtable: &MutexGuard<ObjTable>, fntable: Arc<RwLock<FnTable>>) -> Option<usize> {
        for (i, workerid) in worker_queue.iter().enumerate() {
            if fntable.read().unwrap()[job.get_name()].binary_search(workerid).is_ok() && WorkerPool::dependencies_satisfied(job, objtable) {
                return Some(i);
            }
        }
        return None;
    }

    // will be notified of workers or jobs that become available throught the worker_notify or job_notify channel
    pub fn start_dispatch_thread(workers: Arc<Mutex<Vec<Worker>>>, objtable: Arc<Mutex<ObjTable>>, fntable: Arc<RwLock<FnTable>>, event_notify: Receiver<Event>) {
        thread::spawn(move || {
            let mut worker_queue = VecDeque::<WorkerID>::new();
            let mut job_queue = VecDeque::<comm::Call>::new();
            let mut pull_queue = VecDeque::<(WorkerID, ObjRef)>::new();
            loop {
                // use the most simple algorithms for now
                match event_notify.recv().unwrap() {
                    Event::Worker(workerid) => {
                        match WorkerPool::find_next_job(workerid, &job_queue, &objtable.lock().unwrap(), fntable.clone()) {
                            Some(jobidx) => {
                                let job = job_queue.swap_front_remove(jobidx).unwrap();
                                WorkerPool::send_function_call(&workers.lock().unwrap()[workerid], job);
                            }
                            None => {
                                worker_queue.push_back(workerid);
                            }
                        }
                    },
                    Event::Job(job) => {
                        match WorkerPool::find_next_worker(&job, &worker_queue, &objtable.lock().unwrap(), fntable.clone()) {
                            Some(workeridx) => {
                                let workerid = worker_queue.swap_front_remove(workeridx).unwrap();
                                WorkerPool::send_function_call(&workers.lock().unwrap()[workerid], job);
                            }
                            None => {
                                job_queue.push_back(job);
                            }
                        }
                    },
                    Event::Obj(newobjref) => {
                        // TODO: do this with a binary search
                        for &(workerid, objref) in pull_queue.iter() {
                            if objref == newobjref {
                                WorkerPool::send_pull_request(&workers.lock().unwrap()[workerid], workerid, objref);
                            }
                        }
                    },
                    Event::Pull(workerid, objref) => {
                        if objtable.lock().unwrap()[objref as usize].len() > 0 {
                            WorkerPool::send_pull_request(&workers.lock().unwrap()[workerid], workerid, objref);
                        } else {
                            pull_queue.push_back((workerid, objref));
                        }
                    }
                }
            }
        });
    }
    // add new job to the queue
    pub fn queue_job(self: &mut WorkerPool, job: comm::Call) {
        self.event_notify.send(Event::Job(job)).unwrap();
    }
    // Return the number of workers in the pool
    pub fn len(self: &WorkerPool) -> usize {
        return self.workers.lock().unwrap().len();
    }
    // connect a new worker to the workers already present in the pool
    fn connect(self: &mut WorkerPool, zmq_ctx: &mut zmq::Context, addr: &str, workerid: WorkerID) -> Socket {
        info!("connecting worker {}", workerid);
        let mut socket = zmq_ctx.socket(zmq::REQ).unwrap();
        socket.connect(addr).unwrap();
        let mut setup = zmq_ctx.socket(zmq::REP).ok().unwrap();
        setup.bind("tcp://*:5241").ok().unwrap();
        let mut buf = zmq::Message::new().unwrap();
        loop {
            let mut hello = comm::Message::new();
            hello.set_field_type(comm::MessageType::HELLO);
            self.publish_notify.send((workerid, hello)).unwrap();
            thread::sleep_ms(10); // don't float the message queue
            match setup.recv(&mut buf, zmq::DONTWAIT) {
                Ok(_) => break,
                Err(_) => continue
            }
        }
        // connect new client with other clients that are already connected
        // and connect already connected clients with the new client
        for i in 0..self.len() {
            let mut message = comm::Message::new();
            message.set_field_type(comm::MessageType::REGISTER_CLIENT);
            let other_party =  &self.workers.lock().unwrap()[i].addr;
            message.set_address(other_party.clone()); // fix this
            self.publish_notify.send((workerid, message)).unwrap();

            let mut request = comm::Message::new();
            request.set_field_type(comm::MessageType::REGISTER_CLIENT);
            request.set_address(addr.into());
            self.publish_notify.send((i, request)).unwrap();
        }
        return socket;
    }

    pub fn send_deliver_request(pullid: WorkerID, addr: &str, objref: ObjRef, publish_notify: &Sender<(WorkerID, comm::Message)>) {
        let mut deliver = comm::Message::new();
        deliver.set_field_type(comm::MessageType::DELIVER);
        deliver.set_objref(objref);
        deliver.set_address(addr.into());
        publish_notify.send((pullid, deliver)).unwrap();
    }

    pub fn deliver_object(workerid: WorkerID, objref: ObjRef, workers: &Arc<Mutex<Vec<Worker>>>, objtable: &Arc<Mutex<ObjTable>>, publish_notify: &Sender<(WorkerID, comm::Message)>) {
        if !objtable.lock().unwrap()[objref as usize].contains(&workerid) {
            // pick random worker
            let mut rng = rand::thread_rng(); // supposed to have no performance penalty
            let range = Range::new(0, objtable.lock().unwrap()[objref as usize].len());
            let idx = range.ind_sample(&mut rng);
            let pullid = objtable.lock().unwrap()[objref as usize][idx];
            info!("delivering object from {} to {}, addr {}", pullid, workerid, &workers.lock().unwrap()[workerid].addr);
            WorkerPool::send_deliver_request(pullid, &workers.lock().unwrap()[workerid].addr, objref, &publish_notify);
            info!("delivery successful");
        }
    }

    pub fn register(self: &mut WorkerPool, zmq_ctx: &mut zmq::Context, addr: &str, objtable: Arc<Mutex<ObjTable>>) -> WorkerID {
        info!("registering new worker");
        let (incoming, receiver) = mpsc::channel();
        let workerid = self.len();
        let sender = self.event_notify.clone();
        let publish_notify = self.publish_notify.clone();
        let mut socket = self.connect(zmq_ctx, addr, workerid);
        let workers = self.workers.clone(); // increase reference count of the arc
        let objtable = objtable.clone(); // increase reference count of the arc
        thread::spawn(move || {
            sender.send(Event::Worker(workerid)).unwrap(); // pull for new work
            loop {
                let request : comm::Message = receiver.recv().unwrap(); // get the item of work the scheduler chose for us
                match request.get_field_type() {
                    comm::MessageType::INVOKE => {
                        // orchestrate packages being sent to worker node, start the work there
                        send_function_call(&mut socket, request.get_call().get_name(), request.get_call().get_args(), request.get_call().get_result());
                        receive_ack(&mut socket); // TODO: Avoid this round trip
                        for objref in request.get_call().get_args() {
                            WorkerPool::deliver_object(workerid, *objref, &workers, &objtable, &publish_notify)
                        }
                    },
                    comm::MessageType::PULL => {
                        let objref = request.get_objref();
                        WorkerPool::deliver_object(workerid, objref, &workers, &objtable, &publish_notify);
                    }
                    _ => {}
                }
            }
        });
        self.workers.lock().unwrap().push(Worker {addr: addr.into(), incoming: incoming});
        return workerid;
    }
}

pub struct Server<'a> {
    objtable: Arc<Mutex<ObjTable>>, // for each objref, store the list of workers that hold this object (is empty initially)
    fntable: Arc<RwLock<FnTable>>, // mapping from function names to workers that can execute the function (sorted)
    graph: graph::CompGraph<'a>, // computation graph for this server
    workerpool: WorkerPool,
    zmq_ctx: zmq::Context
}

impl<'a> Server<'a> {
    pub fn new() -> Server<'a> {
        let mut ctx = zmq::Context::new();

        let objtable = Arc::new(Mutex::new(vec!()));
        let fntable = Arc::new(RwLock::new(HashMap::new()));

        Server {
            workerpool: WorkerPool::new(objtable.clone(), fntable.clone()),
            objtable: objtable,
            fntable: fntable,
            graph: CompGraph::new(),
            zmq_ctx: ctx
        }
    }
    pub fn main_loop<'b>(self: &'b mut Server<'a>) {
        let mut socket = self.zmq_ctx.socket(zmq::REP).ok().unwrap();
        socket.bind("tcp://127.0.0.1:1234").ok().unwrap();
        loop {
            self.process_request(&mut socket);
        }
    }
    // add new object to the computation graph and the object pool
    pub fn register_new_object<'b>(self: &'b mut Server<'a>) -> ObjRef {
        let (objref, _) = self.graph.add_obj();
        assert!(objref as usize == self.objtable.lock().unwrap().len());
        self.objtable.lock().unwrap().push(vec!());
        return objref;
    }
    // tell the system that a worker holds a certain object
    pub fn register_result<'b>(self: &'b mut Server<'a>, objref: ObjRef, workerid: WorkerID) {
        self.objtable.lock().unwrap()[objref as usize].push(workerid);
    }
    // add a new call to the computation graph (this should be process request)
    pub fn add_call<'b>(self: &'b mut Server<'a>, fnname: String, args: &'b [ObjRef]) -> ObjRef {
        let result = self.register_new_object();
        self.graph.add_op(fnname, args, result);
        return result;
    }
    pub fn add_request<'b>(self: &'b mut Server<'a>, call: &'b comm::Call) -> comm::Message {
        let objref = self.add_call(call.get_name().to_string(), call.get_args()); // TODO: convert to into
        let mut call = call.clone();
        call.set_result(objref);
        self.workerpool.queue_job(call.clone()); // can we get rid of this clone
        // add obj refs here
        let mut message = comm::Message::new();
        message.set_field_type(comm::MessageType::DONE); // TODO: replace this by ACK message
        message.set_call(call);
        return message;
    }
    // dump the computation graph to a .dot file
    pub fn dump<'b>(self: &'b mut Server<'a>, out: &'b mut Write) {
        let res = graph::to_dot(&self.graph);
        out.write(res.as_bytes()).unwrap();
    }

    // process request by client
    pub fn process_request<'b>(self: &'b mut Server<'a>, socket: &'b mut Socket) {
        let msg = receive_message(socket);
        match msg.get_field_type() {
            comm::MessageType::INVOKE => {
                info!("received {:?} {:?}", msg.get_call().get_field_type(), msg.get_call().get_name());
                let mut message = self.add_request(msg.get_call());
                send_message(socket, &mut message);
            },
            comm::MessageType::REGISTER_CLIENT => {
                let workerid = self.workerpool.len();
                let mut ack = comm::Message::new();
                ack.set_field_type(comm::MessageType::ACK);
                ack.set_workerid(workerid as u64);
                send_message(socket, &mut ack);
                self.workerpool.register(&mut self.zmq_ctx, msg.get_address(), self.objtable.clone());
            },
            comm::MessageType::REGISTER_FUNCTION => {
                let workerid = msg.get_workerid() as WorkerID;
                let fnname = msg.get_fnname();
                info!("function {} registered (worker {})", fnname.to_string(), workerid);
                let mut table = self.fntable.write().unwrap();
                if !table.contains_key(fnname) {
                    table.insert(fnname.into(), vec!());
                }
                match table.get(fnname).unwrap().binary_search(&workerid) {
                    Ok(_) => {},
                    Err(idx) => { table.get_mut(fnname).unwrap().insert(idx, workerid); }
                }
                send_ack(socket);
            }
            comm::MessageType::PULL => {
                let workerid = msg.get_workerid() as WorkerID;
                let objref = msg.get_objref();
                info!("object {} pulled (worker {})", objref, workerid);
                send_ack(socket);
                self.workerpool.event_notify.send(Event::Pull(workerid, objref)).unwrap();
            },
            comm::MessageType::ACK => {
                error!("'ack' message not allowed in this state");
                process::exit(1);
            }
            comm::MessageType::DONE => {
                send_ack(socket);
                self.register_result(msg.get_call().get_result(), msg.get_workerid() as WorkerID);
                self.workerpool.event_notify.send(Event::Worker(msg.get_workerid() as usize)).unwrap();
                self.workerpool.event_notify.send(Event::Obj(msg.get_call().get_result())).unwrap();
            }
            comm::MessageType::PUSH => {
                error!("'push' message not allowed in this state");
                process::exit(1);
            }
            _ => {
                error!("message not allowed in this state");
                process::exit(1);
            }
        }
    }
}

pub fn send_function_call(socket: &mut Socket, name: &str, arguments: &[ObjRef], result: ObjRef) {
    let mut message = comm::Message::new();
    message.set_field_type(comm::MessageType::INVOKE);
    let mut call = comm::Call::new();
    call.set_field_type(comm::Call_Type::INVOKE_CALL);
    call.set_name(name.into());
    call.set_args(arguments.to_vec());
    call.set_result(result);
    message.set_call(call);
    send_message(socket, &mut message);
}

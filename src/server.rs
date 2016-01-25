use comm;
use graph;
use scheduler;
use scheduler::{Scheduler, Event};
use utils::{send_message, receive_message, receive_ack, send_ack, bind_socket, push_objrefs};
use utils::{WorkerID, ObjRef, ObjTable, FnTable};
use graph::CompGraph;
use rand;
use rand::distributions::{IndependentSample, Range};
use std::io::{Read, Write};
use std::collections::VecDeque;
use zmq;
use zmq::Socket;
use std::process;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;
use std::sync::{Arc, RwLock, Mutex, MutexGuard, RwLockReadGuard};
use std::collections::HashMap;
use protobuf::{Message, RepeatedField};
use std::iter::Iterator;

/// Contains informations about worker.
pub struct Worker {
  addr: String
}

/// A group of workers that are managed and scheduled together. They are connected with the server
/// using a zero mq `PUB` channel used for one-way communication from server to client.
/// Furthermore, each client is connected to each other client using a REP/REQ socket pair; all
/// data is transferred using these client side connections. It is the `WorkerPool`s task to
/// establish the connections.
pub struct WorkerPool {
  /// Workers that have been registered with this pool.
  workers: Arc<RwLock<Vec<Worker>>>,
  /// Notify the scheduler that a worker, job or object becomes available.
  scheduler_notify: Sender<Event>,
  /// Send delivery requests to clients.
  publish_notify: Sender<(WorkerID, comm::Message)>,
}

impl WorkerPool {
  /// Create a new `WorkerPool`.
  pub fn new(objtable: Arc<Mutex<ObjTable>>, fntable: Arc<RwLock<FnTable>>, publish_port: u64) -> WorkerPool {
    let (publish_sender, publish_receiver) = mpsc::channel();
    let scheduler_notify = Scheduler::start(objtable, fntable);
    WorkerPool::start_publisher_thread(publish_receiver, publish_port);
    return WorkerPool { workers: Arc::new(RwLock::new(Vec::new())), publish_notify: publish_sender, scheduler_notify: scheduler_notify }
  }

  /// Start the thread that is used to feed the PUB/SUB network between the server and the workers.
  pub fn start_publisher_thread(publish_notify: Receiver<(WorkerID, comm::Message)>, publish_port: u64) {
    thread::spawn(move || {
      let mut zmq_ctx = zmq::Context::new();
      let mut publisher = zmq_ctx.socket(zmq::PUB).unwrap();
      bind_socket(&mut publisher, "*", Some(publish_port));
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

  /// Add new job to the queue.
  pub fn queue_job(self: &mut WorkerPool, job: comm::Call) {
    self.scheduler_notify.send(scheduler::Event::Job(job)).unwrap();
  }

  /// Return the number of workers in the pool.
  pub fn len(self: &WorkerPool) -> usize {
    return self.workers.read().unwrap().len();
  }

  /// Connect a new worker to the workers already present in the pool.
  fn connect(self: &mut WorkerPool, zmq_ctx: &mut zmq::Context, addr: &str, workerid: WorkerID, setup_socket: &mut Socket) -> Socket {
    info!("connecting worker {}", workerid);
    let mut socket = zmq_ctx.socket(zmq::REQ).unwrap();
    socket.connect(addr).unwrap();
    let mut buf = zmq::Message::new().unwrap();
    loop {
      let mut hello = comm::Message::new();
      hello.set_field_type(comm::MessageType::HELLO);
      self.publish_notify.send((workerid, hello)).unwrap();
      thread::sleep_ms(10); // don't float the message queue
      match setup_socket.recv(&mut buf, zmq::DONTWAIT) {
        Ok(_) => break,
        Err(_) => continue
      }
    }
    // connect new client with other clients that are already connected
    // and connect already connected clients with the new client
    for i in 0..self.len() {
      let mut message = comm::Message::new();
      message.set_field_type(comm::MessageType::REGISTER_CLIENT);
      let other_party =  &self.workers.read().unwrap()[i].addr;
      message.set_address(other_party.clone()); // fix this
      self.publish_notify.send((workerid, message)).unwrap();

      let mut request = comm::Message::new();
      request.set_field_type(comm::MessageType::REGISTER_CLIENT);
      request.set_address(addr.into());
      self.publish_notify.send((i, request)).unwrap();
    }
    return socket;
  }

  /// Tell a client `pullid` to deliver an object to another client with address `addr`.
  pub fn send_deliver_request(pullid: WorkerID, addr: &str, objref: ObjRef, publish_notify: &Sender<(WorkerID, comm::Message)>) {
    let mut deliver = comm::Message::new();
    deliver.set_field_type(comm::MessageType::DELIVER);
    deliver.set_objref(objref);
    deliver.set_address(addr.into());
    publish_notify.send((pullid, deliver)).unwrap();
  }

  /// Deliver the object with id `objref` to the worker with id `workerid`.
  pub fn deliver_object(workerid: WorkerID, objref: ObjRef, workers: &Arc<RwLock<Vec<Worker>>>, objtable: &Arc<Mutex<ObjTable>>, publish_notify: &Sender<(WorkerID, comm::Message)>) {
    if !objtable.lock().unwrap()[objref as usize].contains(&workerid) {
      // pick random worker
      let mut rng = rand::thread_rng(); // supposed to have no performance penalty
      let range = Range::new(0, objtable.lock().unwrap()[objref as usize].len());
      let idx = range.ind_sample(&mut rng);
      let pullid = objtable.lock().unwrap()[objref as usize][idx];
      info!("delivering object {} from {} to {}, addr {}", objref, pullid, workerid, &workers.read().unwrap()[workerid].addr);
      WorkerPool::send_deliver_request(pullid, &workers.read().unwrap()[workerid].addr, objref, &publish_notify);
    }
  }

  /// Register a new worker with the worker pool.
  pub fn register(self: &mut WorkerPool, zmq_ctx: &mut zmq::Context, addr: &str, objtable: Arc<Mutex<ObjTable>>, setup_socket: &mut Socket) -> WorkerID {
    info!("registering new worker");
    let (incoming, receiver) = mpsc::channel();
    let workerid = self.len();
    let sender = self.scheduler_notify.clone();
    let publish_notify = self.publish_notify.clone();
    let mut socket = self.connect(zmq_ctx, addr, workerid, setup_socket);
    let workers = self.workers.clone();
    let objtable = objtable.clone();
    thread::spawn(move || {
      sender.send(scheduler::Event::Worker(workerid)).unwrap(); // pull for new work
      loop {
        let request : comm::Message = receiver.recv().unwrap(); // get the item of work the scheduler chose for us
        match request.get_field_type() {
          comm::MessageType::INVOKE => {
            // orchestrate packages being sent to worker node, start the work there
            let results = request.get_call().get_result();
            assert!(results.len() == 1);
            send_function_call(&mut socket, request.get_call().get_name(), request.get_call().get_args(), results[0]);
            receive_ack(&mut socket); // TODO: Avoid this round trip
            // deduplicate: (TODO: get rid of inefficiency):
            let mut args = Vec::new();
            push_objrefs(request.get_call().get_args(), &mut args);
            args.sort();
            args.dedup();
            info!("sending args {:?}", args);
            for objref in args.iter() {
              WorkerPool::deliver_object(workerid, *objref, &workers, &objtable, &publish_notify)
            }
          },
          comm::MessageType::PULL => {
            let objref = request.get_objref();
            WorkerPool::deliver_object(workerid, objref, &workers, &objtable, &publish_notify);
        },
        comm::MessageType::DEBUG => {
          println!("pull through to {}", workerid);
          publish_notify.send((workerid, request)).unwrap(); // pull request through
        },
          _ => {}
        }
      }
    });
    self.workers.write().unwrap().push(Worker {addr: addr.into()});
    self.scheduler_notify.send(scheduler::Event::Register(workerid, incoming));
    return workerid;
  }
}

/// The server orchestrates the computation.
pub struct Server<'a> {
  /// For each object reference, the `objtable` stores the list of workers that hold this object.
  objtable: Arc<Mutex<ObjTable>>,
  /// The `fntable` is the mapping from function names to workers that can execute the function (sorted).
  fntable: Arc<RwLock<FnTable>>,
  /// Computation graph for this server.
  graph: graph::CompGraph<'a>,
  /// A pool of workers that are managed by this server.
  workerpool: WorkerPool,
  /// The ZeroMQ context for this server.
  zmq_ctx: zmq::Context
}

impl<'a> Server<'a> {
  /// Create a new server.
  pub fn new(publish_port: u64) -> Server<'a> {
    let mut ctx = zmq::Context::new();

    let objtable = Arc::new(Mutex::new(Vec::new()));
    let fntable = Arc::new(RwLock::new(HashMap::new()));

    Server {
      workerpool: WorkerPool::new(objtable.clone(), fntable.clone(), publish_port),
      objtable: objtable,
      fntable: fntable,
      graph: CompGraph::new(),
      zmq_ctx: ctx
    }
  }

  /// Start the server's main loop.
  pub fn main_loop<'b>(self: &'b mut Server<'a>, incoming_port: u64) {
    let mut socket = self.zmq_ctx.socket(zmq::REP).ok().unwrap();
    bind_socket(&mut socket, "127.0.0.1", Some(incoming_port));
    loop {
      self.process_request(&mut socket);
    }
  }

  /// Add new object to the computation graph and the object pool.
  pub fn register_new_object<'b>(self: &'b mut Server<'a>) -> ObjRef {
    let (objref, _) = self.graph.add_obj();
    assert!(objref as usize == self.objtable.lock().unwrap().len());
    self.objtable.lock().unwrap().push(vec!());
    return objref;
  }

  /// Tell the server that a worker holds a certain object.
  pub fn register_result<'b>(self: &'b mut Server<'a>, objref: ObjRef, workerid: WorkerID) {
    // TODO: Keep vector sorted while inserting
    self.objtable.lock().unwrap()[objref as usize].push(workerid);
  }

  /// Add a new call to the computation graph.
  pub fn add_call<'b>(self: &'b mut Server<'a>, fnname: String, args: &'b [ObjRef]) -> ObjRef {
    let result = self.register_new_object();
    self.graph.add_op(fnname, args, result);
    return result;
  }

  /// Add a map call to the computation graph.
  pub fn add_map<'b>(self: &'b mut Server<'a>, fnname: String, args: &'b comm::Args) -> Vec<ObjRef> {
    // TODO: Do this with only one lock
    let mut result = Vec::new();
    for arg in args.get_objrefs() {
      let objref = self.register_new_object();
      result.push(objref);
    }
    // TODO: add the op here
    return result;
  }

  /// Add a reduce call to the computation graph.
  pub fn add_reduce<'b>(self: &'b mut Server<'a>, fname: String, args: &'b [ObjRef]) -> ObjRef {
      let objref = self.register_new_object();
      // TODO: add the op here
      return objref;
  }

  /// Add a worker's request for evaluation to the computation graph and notify the scheduler.
  pub fn add_request<'b>(self: &'b mut Server<'a>, call: &'b comm::Call) -> comm::Message {
    let mut call = call.clone();
    let mut args = Vec::new();
    push_objrefs(call.get_args(), &mut args);
    if call.get_field_type() == comm::Call_Type::INVOKE_CALL {
      let objref = self.add_call(call.get_name().into(), &args[..]);
      call.set_result(vec!(objref));
      self.workerpool.queue_job(call.clone()); // can we get rid of this clone?
    }
    if call.get_field_type() == comm::Call_Type::MAP_CALL {
      let objrefs = self.add_map(call.get_name().into(), call.get_args());
      // Add to the scheduler
      for (arg, res) in call.get_args().get_objrefs().iter().zip(objrefs.iter()) {
        let mut c = comm::Call::new();
        let mut a = comm::Args::new();
        a.set_objrefs(vec!((*arg).clone())); // TODO: copy needed?
        c.set_args(a);
        c.set_result(vec!(*res));
        c.set_name(call.get_name().into());
        // INVOKE_CALL is already the default
        self.workerpool.queue_job(c);
      }
      call.set_result(objrefs);
    }
    if call.get_field_type() == comm::Call_Type::REDUCE_CALL {

    }
    // add obj refs here
    let mut message = comm::Message::new();
    message.set_field_type(comm::MessageType::DONE);
    message.set_call(call);
    return message;
  }

  /// Dump the computation graph to a .dot file.
  pub fn dump<'b>(self: &'b mut Server<'a>, out: &'b mut Write) {
    let res = graph::to_dot(&self.graph);
    out.write(res.as_bytes()).unwrap();
  }

  /// Establish the setup port that will be used for setting up the client server connection
  fn bind_setup_socket(zmq_ctx: &mut zmq::Context) -> (Socket, u64) {
    let mut setup_socket = zmq_ctx.socket(zmq::REP).ok().unwrap();
    let port = bind_socket(&mut setup_socket, "*", None);
    return (setup_socket, port)
  }

  /// Process request by client.
  pub fn process_request<'b>(self: &'b mut Server<'a>, socket: &'b mut Socket) {
    let msg = receive_message(socket);
    match msg.get_field_type() {
      comm::MessageType::INVOKE => {
        let mut message = self.add_request(msg.get_call());
        // info!("add request {:?} {:?}, result {:?}", msg.get_call().get_field_type(), msg.get_call().get_name(), message.get_call().get_result());
        send_message(socket, &mut message);
      },
      comm::MessageType::REGISTER_CLIENT => {
        let workerid = self.workerpool.len();
        let (mut setup_socket, setup_port) = Server::bind_setup_socket(&mut self.zmq_ctx);
        info!("chose port {}", setup_port);
        let mut ack = comm::Message::new();
        ack.set_field_type(comm::MessageType::ACK);
        ack.set_workerid(workerid as u64);
        ack.set_setup_port(setup_port);
        send_message(socket, &mut ack);
        self.workerpool.register(&mut self.zmq_ctx, msg.get_address(), self.objtable.clone(), &mut setup_socket);
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
        self.workerpool.scheduler_notify.send(scheduler::Event::Pull(workerid, objref)).unwrap();
      },
      comm::MessageType::DONE => {
        send_ack(socket);
        let result = msg.get_call().get_result();
        assert!(result.len() == 1);
        let workerid = msg.get_workerid() as WorkerID;
        self.register_result(result[0], workerid); // this must happen before we notify the scheduler
        self.workerpool.scheduler_notify.send(scheduler::Event::Worker(msg.get_workerid() as usize)).unwrap();
        self.workerpool.scheduler_notify.send(scheduler::Event::Obj(result[0])).unwrap();
      },
      comm::MessageType::ACC => {
        send_ack(socket);
        self.objtable.lock().unwrap()[msg.get_objref() as usize].push(msg.get_workerid() as usize);
        info!("delivery of {} to {} successful", msg.get_objref(), msg.get_workerid());
      }
      comm::MessageType::DEBUG => {
        info!("received debug request");
        send_ack(socket);
        self.workerpool.scheduler_notify.send(scheduler::Event::Debug(msg.get_workerid() as usize)).unwrap();
      },
      _ => {
        error!("message {:?} not allowed in this state", msg.get_field_type());
        process::exit(1);
      }
    }
  }
}

/// Send request for function execution to a worker through the socket `socket`.
pub fn send_function_call(socket: &mut Socket, name: &str, arguments: &comm::Args, result: ObjRef) {
  let mut message = comm::Message::new();
  message.set_field_type(comm::MessageType::INVOKE);
  let mut call = comm::Call::new();
  call.set_field_type(comm::Call_Type::INVOKE_CALL);
  call.set_name(name.into());
  call.set_args(arguments.clone()); // TODO: get rid of this copy
  call.set_result(vec!(result));
  message.set_call(call);
  send_message(socket, &mut message);
}

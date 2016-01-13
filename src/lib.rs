// This file is supposed to shield all the unsafe functionality from the rest of the program
#![crate_type = "dylib"]

#![feature(convert)]
#![feature(box_syntax)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate protobuf;
extern crate libc;
extern crate rand;

extern crate zmq;

pub mod comm;
pub mod client;
pub mod utils;

use libc::{size_t, c_char, uint8_t};
use std::slice;
use client::{Context};
use std::ffi::CStr;
use std::mem::transmute;
use std::str;

fn string_from_c(string: *const c_char) -> String {
    let c_str: &CStr = unsafe { CStr::from_ptr(string) };
    let str_slice: &str = str::from_utf8(c_str.to_bytes()).unwrap();
    return str_slice.to_owned();
}

#[no_mangle]
pub extern "C" fn orchestra_create_context(server_addr: *const c_char, client_addr: *const c_char, subscriber_port: u64) -> *mut Context {
    let server_addr = string_from_c(server_addr);
    let client_addr = string_from_c(client_addr);

    match env_logger::init() {
        Ok(()) => {},
        SetLoggerError => {} // logging framework already initialized
    }

    let res = unsafe { transmute(box Context::new(server_addr, client_addr, subscriber_port)) };
    return res;
}

#[no_mangle]
pub extern "C" fn orchestra_destroy_context(context: *mut Context) {
    let _drop_me: Box<Context> = unsafe { transmute(context) };
}

/*

#[no_mangle]
pub extern "C" fn orchestra_register_type(context: *mut Context, name: *const c_char) {
    let name = string_from_c(name);
    unsafe { (*context).client().add_type(name) };
}

#[no_mangle]
pub extern "C" fn orchestra_get_type(context: *mut Context, name: *const c_char) -> c_int {
    let name = string_from_c(name);
    unsafe {
        match (*context).client().get_type(name) {
            Some(id) => return id as c_int,
            None => return -1
        }
    }
}

*/

#[no_mangle]
pub extern "C" fn orchestra_register_function(context: *mut Context, name: *const c_char) -> usize {
    let name = string_from_c(name);
    unsafe { return (*context).add_function(name) };
}

#[no_mangle]
pub extern "C" fn orchestra_store_result(context: *mut Context, objref: size_t, data: *const uint8_t, datalen: size_t) {
    let data = unsafe { slice::from_raw_parts(data, datalen as usize) };
    unsafe { (*context).add_object(objref, data.to_vec()) };
}

#[no_mangle]
pub extern "C" fn orchestra_call(context: *mut Context, name: *const c_char, arguments: *const size_t, arglen: size_t) -> size_t {
    let name = string_from_c(name);
    let args = unsafe { slice::from_raw_parts::<u64>(arguments, arglen as usize) };
    unsafe { (*context).remote_call_function(name, args) }
}

/// retlist needs to be preallocated on caller side
#[no_mangle]
pub extern "C" fn orchestra_map(context: *mut Context, name: *const c_char, arguments: *const size_t, arglen: size_t, retlist: *mut size_t) {
    let name = string_from_c(name);
    let args = unsafe { slice::from_raw_parts::<u64>(arguments, arglen as usize) };
    unsafe {
        let result = (*context).remote_call_map(name, args);
        for (i, elem) in result.iter().enumerate() {
            *retlist.offset(i as isize) = *elem;
        }
    };
}

#[no_mangle]
pub extern "C" fn orchestra_pull(context: *mut Context, objref: size_t) -> size_t {
    unsafe { return (*context).pull_remote_object(objref); }
}

#[no_mangle]
pub extern "C" fn orchestra_debug_info(context: *mut Context) {
    unsafe {
        let msg = (*context).pull_debug_info();
        println!("worker queue: {:?}", msg.get_scheduler_info().get_worker_queue());
        println!("job queue:");
        for call in msg.get_scheduler_info().get_job_queue() {
            println!("call: {:?}, {:?} -> {:?}", call.get_name(), call.get_args(), call.get_result());
        }
        println!("object table:");
        for info in msg.get_scheduler_info().get_objtable() {
            println!("entry: {:?}: {:?}", info.get_objref(), info.get_workerid());
        }
        println!("function table");
        for info in msg.get_scheduler_info().get_fntable() {
            println!("entry: {:?}: {:?}", info.get_fnname(), info.get_workerid());
        }
    }
}

#[no_mangle]
pub extern "C" fn orchestra_step(context: *mut Context) -> size_t {
    unsafe {
        (*context).finish_request();
        return (*context).client_step();
    }
}

#[no_mangle]
pub extern "C" fn orchestra_function_index(context: *mut Context) -> usize {
    unsafe { (*context).get_function() }
}

#[no_mangle]
pub extern "C" fn orchestra_num_args(context: *mut Context) -> usize {
    unsafe { (*context).get_num_args() }
}

#[no_mangle]
pub extern "C" fn orchestra_get_arg_len(context: *mut Context, argidx: usize) -> usize {
    unsafe { (*context).get_arg_len(argidx).expect("argument reference not found") }
}

#[no_mangle]
pub extern "C" fn orchestra_get_arg_ptr(context: *mut Context, argidx: usize) -> *const uint8_t {
    unsafe { (*context).get_arg_ptr(argidx).expect("argument reference not found") }
}

#[no_mangle]
pub extern "C" fn orchestra_get_obj_len(context: *mut Context, objref: u64) -> usize {
    unsafe { (*context).get_obj_len(objref).expect("object reference not found") }
}

#[no_mangle]
pub extern "C" fn orchestra_get_obj_ptr(context: *mut Context, objref: u64) -> *const uint8_t {
    unsafe { (*context).get_obj_ptr(objref).expect("object reference not found") }
}

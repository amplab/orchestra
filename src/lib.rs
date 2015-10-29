// This file is supposed to shield all the unsafe functionality from the rest of the program
#![crate_type = "dylib"]

#![feature(convert)]
#![feature(box_syntax)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate nanomsg;
extern crate protobuf;
extern crate libc;

mod comm;
mod client;
mod utils;

use libc::{size_t, c_char, c_int, uint64_t, uint8_t};
use std::slice;
use client::{Client, Context};
use std::ffi::CStr;
use std::mem::transmute;
use std::str;
use nanomsg::{Socket, Protocol};
use utils::{send_message, make_message};

pub fn string_from_c(string: *const c_char) -> String {
    let c_str: &CStr = unsafe { CStr::from_ptr(string) };
    let str_slice: &str = str::from_utf8(c_str.to_bytes()).unwrap();
    return str_slice.to_owned();
}

#[no_mangle]
pub fn hermes_create_context(server_addr: *const c_char, client_addr: *const c_char) -> *mut Context {
    let server_addr = string_from_c(server_addr);
    let client_addr = string_from_c(client_addr);
    unsafe { transmute(box Context::new(server_addr, client_addr)) }
}

#[no_mangle]
pub fn hermes_destroy_context(context: *mut Context) {
    let _drop_me: Box<Client> = unsafe { transmute(context) };
}

#[no_mangle]
pub fn hermes_register_type(context: *mut Context, name: *const c_char) {
    let name = string_from_c(name);
    unsafe { (*context).client().add_type(name) };
}

#[no_mangle]
pub fn hermes_get_type(context: *mut Context, name: *const c_char) -> c_int {
    let name = string_from_c(name);
    unsafe {
        match (*context).client().get_type(name) {
            Some(id) => return id as c_int,
            None => return -1
        }
    }
}

#[no_mangle]
pub fn hermes_register_function(context: *mut Context, name: *const c_char) -> usize {
    let name = string_from_c(name);
    unsafe { return (*context).client().add_function(name) };
}

#[no_mangle]
pub fn hermes_store_result(context: *mut Context, objref: size_t, data: *const uint8_t, datalen: size_t) {
    let data = unsafe { slice::from_raw_parts(data, datalen as usize) };
    unsafe { (*context).client().add_object(objref as usize, data.to_vec()) };
}

#[no_mangle]
pub fn hermes_call(context: *mut Context, name: *const c_char, arguments: *const usize, arglen: size_t) -> usize {
    let name = string_from_c(name);
    let args = unsafe { slice::from_raw_parts::<usize>(arguments, arglen as usize) };
    let socket = unsafe { (*context).socket() };
    unsafe { (*context).client().remote_call_function(socket, name, args) }
}

#[no_mangle]
pub fn hermes_step(context: *mut Context) -> usize {
    unsafe {
        loop {
            let socket = (*context).socket();
            (*context).client().finish_request(socket);
            let (ret, objref) = (*context).client().process_request(socket);
            if ret {
                return objref
            }
        }
    }
}

#[no_mangle]
pub fn hermes_function_index(context: *mut Context) -> usize {
    unsafe { (*context).client().get_function() }
}

#[no_mangle]
pub fn hermes_num_args(context: *mut Context) -> usize {
    unsafe { (*context).client().get_num_args() }
}

#[no_mangle]
pub fn hermes_get_arg_len(context: *mut Context, argidx: usize) -> usize {
    unsafe { (*context).client().get_arg_len(argidx).expect("argument reference not found") }
}

#[no_mangle]
pub fn hermes_get_arg_ptr(context: *mut Context, argidx: usize) -> *const uint8_t {
    unsafe { (*context).client().get_arg_ptr(argidx).expect("argument reference not found") }
}

#[test]
fn test_client() {

}

use spsc::*;
use std::thread;

fn main() {
    let (px, cx) = channel();

    thread::spawn(move || {
        px.send("Ping").unwrap();
    });

    println!("recv: {}", cx.recv().unwrap());
}

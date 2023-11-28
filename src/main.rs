// Uncomment this block to pass the first stage
use std::{net::TcpListener, io::{Write, BufReader, BufRead, Read}};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let s = "+PONG\r\n";
                loop {
                    let mut buffer = [0; 512];
                    if let Ok(count) = stream.read(&mut buffer) {
                        if count == 0 {
                            break;
                        }
                        stream.write(s.as_bytes()).unwrap();
                    } else {
                        break;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

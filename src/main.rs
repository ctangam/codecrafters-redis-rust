// Uncomment this block to pass the first stage

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

mod cmd;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    let mut buffer = BytesMut::with_capacity(4096);
                    loop {
                        match stream.read_buf(&mut buffer).await {
                            Ok(0) => break,
                            Ok(count) => {
                                let req = String::from_utf8_lossy(&buffer[0..count]);
                                println!("{}", req);
                                let s = {
                                    match &req[..4] {
                                        "PING" => "+PONG\r\n",
                                        "ECHO" => {
                                            let msg = &req[5..];
                                            println!("ECHO: {}", msg);
                                            msg
                                        }
                                        _ => "+OK\r\n",
                                    }
                                };
                                stream.write(s.as_bytes()).await.unwrap();
                            }
                            Err(e) => {
                                println!("error: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

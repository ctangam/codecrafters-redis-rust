// Uncomment this block to pass the first stage

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

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
                    let s = "+PONG\r\n";
                    let mut buffer = vec![0; 512];
                    loop {
                        match stream.read(&mut buffer).await {
                            Ok(0) => continue,
                            Ok(count) => {
                                let req = String::from_utf8_lossy(&buffer[0..count]);
                                println!("{}", req);
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

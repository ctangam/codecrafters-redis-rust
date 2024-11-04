// Uncomment this block to pass the first stage

use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let s = "+PONG\r\n";
                loop {
                    stream.readable().await.unwrap();
                    let mut buffer = [0; 512];
                    if let Ok(count) = stream.try_read(&mut buffer) {
                        if count == 0 {
                            break;
                        }
                        let req = String::from_utf8_lossy(&buffer[0..count]);
                        println!("{}", req);
                        stream.try_write(s.as_bytes()).unwrap();
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

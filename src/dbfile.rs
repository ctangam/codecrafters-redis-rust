use std::time::Duration;
use std::time::Instant;

use std::time::SystemTime;

use std::time::UNIX_EPOCH;

use bytes::Buf;
use bytes::BytesMut;

use crate::env;

pub(crate) async fn parse_dbfile(mut buf: BytesMut, db: env::DB) {
    println!("{:?}", String::from_utf8_lossy(&buf[..]));
    let header = &buf[..9];
    let header = str::from_utf8(header).unwrap();
    println!("header: {}", header);
    buf.advance(9);

    let mut metadatas = Vec::new();
    while buf[0] == 0xFA {
        buf.advance(1);

        let name = string_decode(&mut buf);
        let value = string_decode(&mut buf);

        metadatas.push((name, value));
    }
    println!("metadatas: {:?}", metadatas);

    let mut db = db.lock().unwrap();
    while buf[0] == 0xFE {
        buf.advance(1);

        let index = size_decode(&mut buf);
        println!("index: {}", index);
        if buf[0] == 0xFB {
            buf.advance(1);
            let size = size_decode(&mut buf);
            println!("size: {}", size);
            let expire_size = size_decode(&mut buf);
            println!("expire_size: {}", expire_size);

            for _ in 0..size {
                let expire = if buf[0] == 0xFC {
                    let value = u64::from_le_bytes(buf[1..][..8].try_into().unwrap());
                    println!("{value:?} millis");
                    buf.advance(9);
                    let time = UNIX_EPOCH + Duration::from_millis(value);

                    let earlier = SystemTime::now();
                    if time < earlier {
                        Some(Instant::now())
                    } else {
                        Some(Instant::now() + time.duration_since(earlier).unwrap())
                    }
                } else if buf[0] == 0xFD {
                    let value = u32::from_le_bytes(buf[1..][..4].try_into().unwrap());
                    println!("{value:?} secs");
                    buf.advance(5);
                    let time = UNIX_EPOCH + Duration::from_secs(value as u64);

                    let earlier = SystemTime::now();
                    if time < earlier {
                        Some(Instant::now())
                    } else {
                        Some(Instant::now() + time.duration_since(earlier).unwrap())
                    }
                } else {
                    None
                };
                let value_type = buf[0];
                assert_eq!(value_type, 0);
                buf.advance(1);
                let key = string_decode(&mut buf);
                let value = string_decode(&mut buf);
                db.insert(key, (value.into(), expire));
            }
        }
    }

    println!("db: {:?}", db);

    assert_eq!(buf[0], 0xFF);
    println!("end of file");
    buf.advance(1);

    let _crc = &buf[..8];
    buf.advance(8);
}

pub(crate) fn _list_decode(src: &mut BytesMut) -> Vec<String> {
    let size = size_decode(src);
    (0..size).map(|_| string_decode(src)).collect()
}

pub(crate) fn string_decode(src: &mut BytesMut) -> String {
    match src[0] {
        0xC0 => {
            let s = src[1].to_string();
            src.advance(2);
            s
        }

        0xC1 => {
            let s = u16::from_le_bytes([src[1], src[2]]).to_string();
            src.advance(3);
            s
        }

        0xC2 => {
            let s = u32::from_le_bytes([src[1], src[2], src[3], src[4]]).to_string();
            src.advance(5);
            s
        }

        _ => {
            let len = size_decode(src);
            let s = String::from_utf8(src[..len].to_vec()).unwrap();
            src.advance(len);
            s
        }
    }
}

pub(crate) fn size_decode(src: &mut BytesMut) -> usize {
    let indicator = (src[0] & 0b1100_0000) >> 6;
    let b0 = src[0] & 0b0011_1111;
    match indicator {
        0b00 => {
            let size = b0 as usize;
            src.advance(1);
            size
        }

        0b01 => {
            let size = (b0 as usize) << 8 | (src[1] as usize);
            src.advance(2);
            size
        }

        0b10 => {
            let size = (b0 as usize) << 32
                | (src[1] as usize) << 24
                | (src[2] as usize) << 16
                | (src[3] as usize) << 8
                | (src[4] as usize);

            src.advance(4);
            size
        }

        _ => unreachable!(),
    }
}

#[test]
pub(crate) fn test_expire() {
    let value = u32::from_le_bytes([0x52, 0xED, 0x2A, 0x66]);
    let time = UNIX_EPOCH + Duration::from_secs(value as u64);

    let expire = Some(Instant::now() + time.elapsed().unwrap());

    println!("{expire:?}");
}

#[test]
pub(crate) fn test_string_decode() {
    let encoded = vec![
        0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
    ];
    let mut buf = BytesMut::from(&encoded[..]);
    let s = string_decode(&mut buf);
    assert_eq!(s, "Hello, World!");

    let encoded = vec![0xC0, 0x7B];
    let mut buf = BytesMut::from(&encoded[..]);
    let s = string_decode(&mut buf);
    assert_eq!(s, "123");

    let encoded = vec![0xC1, 0x39, 0x30];
    let mut buf = BytesMut::from(&encoded[..]);
    let s = string_decode(&mut buf);
    assert_eq!(s, "12345");

    let encoded = vec![0xC2, 0x87, 0xD6, 0x12, 0x00];
    let mut buf = BytesMut::from(&encoded[..]);
    let s = string_decode(&mut buf);
    assert_eq!(s, "1234567");
}

#[test]
pub(crate) fn test_size_decode() {
    let mut buf = BytesMut::from(&0x0A_u8.to_be_bytes()[..]);
    let s = size_decode(&mut buf);
    assert_eq!(s, 10);

    let mut buf = BytesMut::from(&0x42BC_u16.to_be_bytes()[..]);
    let s = size_decode(&mut buf);
    assert_eq!(s, 700);

    let mut buf = BytesMut::from(&(0x8000004268_u64 << 24).to_be_bytes()[..]);
    let s = size_decode(&mut buf);
    assert_eq!(s, 17000);
}

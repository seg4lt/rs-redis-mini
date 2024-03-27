## IP

- When we send data to a network, the data is send as a multiple packets
- Each packet contains
  - header section
    - header contains source and destination address
  - data section
- Packets can get lots
- Packets get get to their destination out of order

## TCP

- TCP guarantees
  - Reliable delivery of packets
  - In order delivery of packets
- Reliable delivery of packets
  - If a packet is lost, TCP will resend it provided no ack is received
- In order delivery of packets

  - Adds a sequence number for each packets
  - Receiver reorders out-of-sequence package

- TCP is connection-oriented protocol

  - Connection is established before data is sent
  - Connection is closed after data is sent
  - So TCP has concept of server and client

- TCP is identified using unique combination of four values
  - Destination IP
  - Destination Port
  - Source IP
  - Source Port

### TCP Handshake

- Three way handshake
  - SYN
    - SYN packet is sent to server to establish connection
    - Packet has a sequence number to maintain order of the packets
  - SYN-ACK
    - Server sends SYN-ACK packet to client
  - ACK
    - Client sends ACK packet to server
    - After server receives ACK, connection is established

## Rust and TCP

- Rust has `std::net` module that provides TCP functionality
- We will look into `TcpListener` struct

  - `TcpListener::bind`
  - `TcpListener::incoming`
  - `TcpStream::connect`
  - `TcpStream::read`
  - `TcpStream::write_all`

- `TcpStream::connect` Used for outbound connections

```rust
let stream = std::net::TcpStream::connect("server:port")?;
```

- `TcpListener::bind` Used for inbound connections

```rust
let listener = std::net::TcpListener::bind("server:port")?;
```

- `TcpListener struct`

```rust
impl TcpListener {
    // accept waits for and returns the next connection to the listener
    pub fn accept(&self) -> Result<(TcpStream, SocketAddr)>
    // incoming returns an iterator over the connections being received on this listener
    pub fn incoming(&self) -> Incoming<TcpStream>
}
```

- `TcpListener::incoming`

  - `Incoming` is an iterator over the connections being received on this listener
  - this method yields `Result<TcpStream, std::io::Error>`

- `TcpStream struct`

```rust
impl TcpStream {
    // read reads bytes from the stream
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize>

    // write writes bytes to the stream and returns the number of bytes written.
    // It's often easier to use write_all instead of this method.
    pub fn write(&mut self, buf: &[u8]) -> Result<usize>

    // write_all writes all the bytes in buf to the stream
    pub fn write_all(&mut self, buf: &[u8]) -> Result<()>
}
```

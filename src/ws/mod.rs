pub mod client;

#[derive(Debug, Clone)]
pub enum WsFrame {
    Text(String),
    Binary(Vec<u8>),
}

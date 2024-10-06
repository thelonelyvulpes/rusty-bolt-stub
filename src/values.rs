pub mod graph;
pub mod spatial;
pub mod time;
pub mod value_receive;
pub mod value_send;

#[derive(Debug)]
pub struct ClientMessage {
    tag: u8,
    fields: Vec<value_receive::ValueReceive>,
}

impl ClientMessage {
    pub fn new(tag: u8, fields: Vec<value_receive::ValueReceive>) -> Self {
        Self { tag, fields }
    }
}

#[derive(Debug)]
pub struct ServerMessage {
    fields: value_send::ValueSend,
}

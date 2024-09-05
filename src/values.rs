use crate::values::value_send::ValueSend;

pub mod graph;
pub mod spatial;
pub mod time;
pub mod value_receive;
pub mod value_send;

pub struct ClientMessage {
    tag: u8,
    fields: Vec<value_receive::ValueReceive>,
}

pub struct ServerMessage {
    fields: ValueSend,
}

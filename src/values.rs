pub mod value_send;
pub mod value_receive;
pub mod spatial;
pub mod time;
pub mod graph;

pub struct ClientMessage {
    tag: u8,
    fields: Vec<value_receive::ValueReceive>
}

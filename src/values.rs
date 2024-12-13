use crate::bolt_version::BoltVersion;

pub mod graph;
pub mod spatial;
pub mod time;
pub mod value_receive;
pub mod value_send;

#[derive(Debug)]
pub struct ClientMessage {
    tag: u8,
    fields: Vec<value_receive::ValueReceive>,
    bolt_version: BoltVersion,
}

impl ClientMessage {
    pub fn new(
        tag: u8,
        fields: Vec<value_receive::ValueReceive>,
        bolt_version: BoltVersion,
    ) -> Self {
        Self {
            tag,
            fields,
            bolt_version,
        }
    }
}

#[derive(Debug)]
pub struct ServerMessage {
    fields: value_send::ValueSend,
}

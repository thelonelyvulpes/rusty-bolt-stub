use crate::bolt_version::BoltVersion;

pub mod graph;
pub mod spatial;
pub mod time;
pub mod value;

#[derive(Debug, Clone)]
pub struct BoltMessage {
    pub tag: u8,
    pub name: &'static str,
    pub fields: Vec<value::Value>,
}

impl BoltMessage {
    pub fn new(tag: u8, fields: Vec<value::Value>, bolt_version: BoltVersion) -> Self {
        Self {
            tag,
            name: "HELLO", // to be computed from bolt_version
            fields,
        }
    }
}

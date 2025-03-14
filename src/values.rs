use std::borrow::Cow;

use crate::bolt_version::BoltVersion;

pub mod graph;
pub mod spatial;
pub mod structs;
pub mod time;
pub mod value;

#[derive(Debug, Clone)]
pub struct BoltMessage {
    pub tag: u8,
    pub name: Cow<'static, str>,
    pub fields: Vec<value::Value>,
}

impl BoltMessage {
    pub fn new(tag: u8, fields: Vec<value::Value>, bolt_version: BoltVersion) -> Self {
        let name = match bolt_version.message_name_from_request(tag) {
            None => format!("UNKNOWN[{tag:#02X}]").into(),
            Some(name) => name.into(),
        };
        Self { tag, name, fields }
    }
}

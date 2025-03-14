use std::borrow::Cow;

use crate::bolt_version::BoltVersion;

pub mod bolt_value;
pub mod structs;

#[derive(Debug, Clone)]
pub struct BoltMessage {
    pub tag: u8,
    pub name: Cow<'static, str>,
    pub fields: Vec<bolt_value::PackStreamValue>,
}

impl BoltMessage {
    pub fn new(
        tag: u8,
        fields: Vec<bolt_value::PackStreamValue>,
        bolt_version: BoltVersion,
    ) -> Self {
        let name = match bolt_version.message_name_from_request(tag) {
            None => format!("UNKNOWN[{tag:#02X}]").into(),
            Some(name) => name.into(),
        };
        Self { tag, name, fields }
    }
}

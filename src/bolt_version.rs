use std::fmt::Display;
use std::str::FromStr;
use std::sync::atomic::AtomicI64;

use indexmap::{indexmap, IndexMap};

use crate::values::value::Value;

#[derive(Debug, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
pub enum BoltVersion {
    V1,
    V2,
    V3,
    V4_0,
    V4_1,
    V4_2,
    V4_3,
    V4_4,
    V5_0,
    V5_1,
    V5_2,
    V5_3,
    V5_4,
    V5_5,
    V5_6,
    V5_7,
    V5_8,
}

impl Display for BoltVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self <= &BoltVersion::V3 {
            return write!(f, "{}", self.major());
        }
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

impl BoltVersion {
    pub fn match_valid_version(major: u8, minor: &Option<u8>) -> Option<Self> {
        Some(match (major, minor) {
            (1, None) | (1, Some(0)) => BoltVersion::V1,
            (2, None) | (2, Some(0)) => BoltVersion::V2,
            (3, None) | (3, Some(0)) => BoltVersion::V3,
            (4, Some(x)) => match x {
                0 => BoltVersion::V4_0,
                1 => BoltVersion::V4_1,
                2 => BoltVersion::V4_2,
                3 => BoltVersion::V4_3,
                4 => BoltVersion::V4_4,
                _ => return None,
            },
            (5, Some(x)) => match x {
                0 => BoltVersion::V5_0,
                1 => BoltVersion::V5_1,
                2 => BoltVersion::V5_2,
                3 => BoltVersion::V5_3,
                4 => BoltVersion::V5_4,
                5 => BoltVersion::V5_5,
                6 => BoltVersion::V5_6,
                7 => BoltVersion::V5_7,
                8 => BoltVersion::V5_8,
                _ => return None,
            },
            _ => return None,
        })
    }

    pub fn major(&self) -> u8 {
        match self {
            BoltVersion::V1 => 1,
            BoltVersion::V2 => 2,
            BoltVersion::V3 => 3,
            BoltVersion::V4_0 => 4,
            BoltVersion::V4_1 => 4,
            BoltVersion::V4_2 => 4,
            BoltVersion::V4_3 => 4,
            BoltVersion::V4_4 => 4,
            BoltVersion::V5_0 => 5,
            BoltVersion::V5_1 => 5,
            BoltVersion::V5_2 => 5,
            BoltVersion::V5_3 => 5,
            BoltVersion::V5_4 => 5,
            BoltVersion::V5_5 => 5,
            BoltVersion::V5_6 => 5,
            BoltVersion::V5_7 => 5,
            BoltVersion::V5_8 => 5,
        }
    }

    pub fn minor(&self) -> u8 {
        match self {
            BoltVersion::V1 => 0,
            BoltVersion::V2 => 0,
            BoltVersion::V3 => 0,
            BoltVersion::V4_0 => 0,
            BoltVersion::V4_1 => 1,
            BoltVersion::V4_2 => 2,
            BoltVersion::V4_3 => 3,
            BoltVersion::V4_4 => 4,
            BoltVersion::V5_0 => 0,
            BoltVersion::V5_1 => 1,
            BoltVersion::V5_2 => 2,
            BoltVersion::V5_3 => 3,
            BoltVersion::V5_4 => 4,
            BoltVersion::V5_5 => 5,
            BoltVersion::V5_6 => 6,
            BoltVersion::V5_7 => 7,
            BoltVersion::V5_8 => 8,
        }
    }

    pub fn jolt_version(&self) -> JoltVersion {
        match self {
            BoltVersion::V1 => JoltVersion::V1,
            BoltVersion::V2 => JoltVersion::V1,
            BoltVersion::V3 => JoltVersion::V1,
            BoltVersion::V4_0 => JoltVersion::V1,
            BoltVersion::V4_1 => JoltVersion::V1,
            BoltVersion::V4_2 => JoltVersion::V1,
            BoltVersion::V4_3 => JoltVersion::V1,
            BoltVersion::V4_4 => JoltVersion::V1,
            BoltVersion::V5_0 => JoltVersion::V2,
            BoltVersion::V5_1 => JoltVersion::V2,
            BoltVersion::V5_2 => JoltVersion::V2,
            BoltVersion::V5_3 => JoltVersion::V2,
            BoltVersion::V5_4 => JoltVersion::V2,
            BoltVersion::V5_5 => JoltVersion::V2,
            BoltVersion::V5_6 => JoltVersion::V2,
            BoltVersion::V5_7 => JoltVersion::V2,
            BoltVersion::V5_8 => JoltVersion::V2,
        }
    }

    pub fn message_tag_from_request(&self, name: &str) -> Option<u8> {
        Some(match name {
            "INIT" if self < &BoltVersion::V3 => 0x01,
            "HELLO" if self >= &BoltVersion::V3 => 0x01,
            "LOGON" if self >= &BoltVersion::V5_1 => 0x6A,
            "LOGOFF" if self >= &BoltVersion::V5_1 => 0x6B,
            "TELEMETRY" if self >= &BoltVersion::V5_4 => 0x54,
            "GOODBYE" => 0x02,
            "ACK_FAILURE" if self < &BoltVersion::V3 => 0x0E,
            "RESET" => 0x0F,
            "RUN" => 0x10,
            "DISCARD_ALL" if self < &BoltVersion::V4_0 => 0x2F,
            "DISCARD" if self >= &BoltVersion::V4_0 => 0x2F,
            "PULL_ALL" if self < &BoltVersion::V4_0 => 0x3F,
            "PULL" if self >= &BoltVersion::V4_0 => 0x3F,
            "BEGIN" => 0x11,
            "COMMIT" => 0x12,
            "ROLLBACK" => 0x13,
            "ROUTE" if self >= &BoltVersion::V4_3 => 0x66,
            _ => return None,
        })
    }

    pub fn message_tag_from_response(&self, name: &str) -> Option<u8> {
        Some(match name {
            "SUCCESS" => 0x70,
            "IGNORED" => 0x7E,
            "FAILURE" => 0x7F,
            "RECORD" => 0x71,
            _ => return None,
        })
    }

    pub fn message_auto_response(&self, tag: u8) -> Option<(u8, Vec<Value>)> {
        const SUCCESS_TAG: u8 = 0x70;
        let success_meta = match tag {
            0x01 => {
                // INIT/HELLO
                let mut success_map = IndexMap::with_capacity(4);
                success_map.insert(
                    String::from("server"),
                    Value::String(String::from(self.server_agent())),
                );
                if self >= &BoltVersion::V3 {
                    static CONNECTION_ID: AtomicI64 = AtomicI64::new(1);
                    let connection_id =
                        CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    assert_ne!(connection_id, 0, "Connection ID overflowed");
                    success_map.insert(
                        String::from("connection_id"),
                        Value::String(format!("bolt-{connection_id}")),
                    );
                }
                if self >= &BoltVersion::V5_7 {
                    success_map.insert(
                        String::from("protocol_version"),
                        Value::String(self.to_string()),
                    );
                }
                if self >= &BoltVersion::V5_8 {
                    success_map.insert(
                        String::from("hints"),
                        Value::Map(indexmap! {String::from("ssr.enabled") => Value::Boolean(true)}),
                    );
                }
                success_map
            }
            _ => Default::default(),
        };
        Some((SUCCESS_TAG, vec![Value::Map(success_meta)]))
    }

    fn server_agent(&self) -> &'static str {
        match self {
            BoltVersion::V1 => "Neo4j/3.3.0",
            BoltVersion::V2 => "Neo4j/3.4.0",
            BoltVersion::V3 => "Neo4j/3.5.0",
            BoltVersion::V4_0 => "Neo4j/4.0.0",
            BoltVersion::V4_1 => "Neo4j/4.1.0",
            BoltVersion::V4_2 => "Neo4j/4.2.0",
            BoltVersion::V4_3 => "Neo4j/4.3.0",
            BoltVersion::V4_4 => "Neo4j/4.4.0",
            BoltVersion::V5_0 => "Neo4j/5.0.0",
            BoltVersion::V5_1 => "Neo4j/5.5.0",
            BoltVersion::V5_2 => "Neo4j/5.7.0",
            BoltVersion::V5_3 => "Neo4j/5.9.0",
            BoltVersion::V5_4 => "Neo4j/5.13.0",
            BoltVersion::V5_5 => "Neo4j/5.21.0",
            BoltVersion::V5_6 => "Neo4j/5.23.0",
            BoltVersion::V5_7 => "Neo4j/5.26.0",
            BoltVersion::V5_8 => "Neo4j/5.26.0",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum JoltVersion {
    V1,
    V2,
}

impl JoltVersion {
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
        let jolt_version =
            i32::from_str(s).map_err(|e| format!("Jolt version must be i32 (found {s:?}): {e}"))?;
        Ok(match jolt_version {
            1 => Self::V1,
            2 => Self::V2,
            _ => return Err(format!("Unknown jolt version: {s}")),
        })
    }
}

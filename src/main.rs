// TODO: remove when rapid PoC phase is over
#![allow(dead_code)]

mod scanner;

fn main() {
    // let input = "!: BOLT 5.5";
}

#[derive(Debug, PartialEq, Clone)]
enum BangLine {
    Version(u8, u8),
    AllowRestart,
    Auto(String),
    Concurrent,
    Handshake(Vec<u8>),
    HandshakeDelay(f64),
    Python(String),
}

#[derive(Debug, Eq, PartialEq, Clone)]
enum BoltVersion {
    V3_5,
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
}

impl BoltVersion {
    pub(crate) fn match_valid_version(major: u8, minor: u8) -> Option<Self> {
        Some(match (major, minor) {
            (3, 5) => BoltVersion::V3_5,
            (4, 0) => BoltVersion::V4_0,
            (4, 1) => BoltVersion::V4_1,
            (4, 2) => BoltVersion::V4_2,
            (4, 3) => BoltVersion::V4_3,
            (4, 4) => BoltVersion::V4_4,
            (5, 0) => BoltVersion::V5_0,
            (5, 1) => BoltVersion::V5_1,
            (5, 2) => BoltVersion::V5_2,
            (5, 3) => BoltVersion::V5_3,
            (5, 4) => BoltVersion::V5_4,
            (5, 5) => BoltVersion::V5_5,
            _ => return None,
        })
    }
}

#[derive(Debug)]
struct Script {
    name: String,
    bang_lines: Vec<BangLine>,
    body: Block,
}

#[derive(Debug, Eq, PartialEq)]
enum Block {
    List(Vec<Block>),
    Alt(Vec<Block>),
    Opt(Box<Block>),
    Repeat0(Box<Block>),
    Repeat1(Box<Block>),
    ClientMessage(String, Option<String>),
    ServerMessage(String, Option<String>),
    AutoMessage(String, Option<String>),
    UntaggedMessage(String, Option<String>),
    Comment,
    Python(String),
    Condition(CompositeConditionBlock),
}

#[derive(Debug, Eq, PartialEq)]
struct CompositeConditionBlock {
    if_: Box<ConditionBranch>,
    elif_: Vec<ConditionBranch>,
    else_: Option<Box<Block>>,
}

#[derive(Debug, Eq, PartialEq)]
struct ConditionBranch {
    condition: String,
    body: Block,
}

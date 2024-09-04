
#[derive(Debug, PartialEq, Clone)]
pub enum BangLine {
    Version(u8, Option<u8>),
    AllowRestart,
    Auto(String),
    Concurrent,
    Handshake(Vec<u8>),
    HandshakeDelay(f64),
    Python(String),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum BoltVersion {
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
    pub fn match_valid_version(major: u8, minor: u8) -> Option<Self> {
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
pub struct Script {
    pub(crate) name: String,
    pub(crate) bang_lines: Vec<BangLine>,
    pub(crate) body: ScanBlock,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ScanBlock {
    List(Vec<ScanBlock>),
    Alt(Vec<ScanBlock>),
    Parallel(Vec<ScanBlock>),
    Opt(Box<ScanBlock>),
    Repeat0(Box<ScanBlock>),
    Repeat1(Box<ScanBlock>),
    ClientMessage(String, Option<String>),
    ServerMessage(String, Option<String>),
    AutoMessage(String, Option<String>),
    Comment,
    Python(String),
    Condition(CompositeConditionBlock),
}

#[derive(Debug, Eq, PartialEq)]
pub struct CompositeConditionBlock {
    if_: Box<ConditionBranch>,
    elif_: Vec<ConditionBranch>,
    else_: Option<Box<ScanBlock>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConditionBranch {
    condition: String,
    body: ScanBlock,
}


pub mod actor_types {
    use tokio::net::TcpStream;
    use crate::values::ClientMessage;

    pub enum ValidationFault {
        Fault(String),
    }

    pub trait ClientMessageValidator {
        fn validate(&self, message: ClientMessage) -> anyhow::Result<()>;
    }

    pub trait ServerMessageSender {
        fn send(&self, stream: &mut TcpStream) -> anyhow::Result<()>;
    }

    pub trait AutoMessageHandler: ClientMessageValidator + ServerMessageSender {}

    pub enum ActorBlock {
        BlockList(Vec<ActorBlock>),
        ClientMessageValidate(Box<dyn ClientMessageValidator>),
        ServerMessageSend(Box<dyn ServerMessageSender>),
        Alt(Vec<ActorBlock>),
        Optional(Box<ActorBlock>),
        Repeat(Box<ActorBlock>, usize),
        AutoMessage(Box<dyn AutoMessageHandler>),
        NoOp,
    }
}

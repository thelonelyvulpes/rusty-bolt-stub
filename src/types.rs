#[derive(Debug, PartialEq, Clone)]
pub enum BangLine {
    Version(u8, Option<u8>),
    AllowRestart,
    Auto(String),
    Concurrent,
    Handshake(String),
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
    Optional(Box<ScanBlock>),
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
    use crate::values::ClientMessage;
    use tokio::net::TcpStream;
    pub trait ScriptLine {
        fn original_line(&self) -> &str;
    }

    pub trait ClientMessageValidator: ScriptLine {
        fn validate(&self, message: ClientMessage) -> anyhow::Result<()>;
    }

    pub trait ServerMessageSender: ScriptLine {
        fn send(&self, stream: &mut TcpStream) -> anyhow::Result<()>;
    }

    pub struct AutoMessageHandler {
        pub(crate) client_validator: Box<dyn ClientMessageValidator>,
        pub(crate) server_sender: Box<dyn ServerMessageSender>,
    }

    impl ClientMessageValidator for AutoMessageHandler {
        fn validate(&self, message: ClientMessage) -> anyhow::Result<()> {
            self.client_validator.validate(message)
        }
    }

    impl ServerMessageSender for AutoMessageHandler {
        fn send(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
            self.server_sender.send(stream)
        }
    }

    impl ScriptLine for AutoMessageHandler {
        fn original_line(&self) -> &str {
            self.client_validator.original_line()
        }
    }

    pub enum ActorBlock {
        BlockList(Vec<ActorBlock>),
        ClientMessageValidate(Box<dyn ClientMessageValidator>),
        ServerMessageSend(Box<dyn ServerMessageSender>),
        Alt(Vec<ActorBlock>),
        Optional(Box<ActorBlock>),
        Repeat(Box<ActorBlock>, usize),
        AutoMessage(AutoMessageHandler),
        NoOp,
    }
}

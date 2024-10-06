use std::cmp::{max, min};
use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
pub enum BangLine {
    Version(Context, u8, Option<u8>),
    AllowRestart(Context),
    Auto(Context, String),
    Concurrent(Context),
    Handshake(Context, String),
    HandshakeDelay(Context, f64),
    Python(Context, String),
}

impl BangLine {
    pub fn ctx(&self) -> &Context {
        match self {
            BangLine::Version(ctx, _, _) => ctx,
            BangLine::AllowRestart(ctx) => ctx,
            BangLine::Auto(ctx, _) => ctx,
            BangLine::Concurrent(ctx) => ctx,
            BangLine::Handshake(ctx, _) => ctx,
            BangLine::HandshakeDelay(ctx, _) => ctx,
            BangLine::Python(ctx, _) => ctx,
        }
    }

    pub fn ctx_mut(&mut self) -> &mut Context {
        match self {
            BangLine::Version(ctx, _, _) => ctx,
            BangLine::AllowRestart(ctx) => ctx,
            BangLine::Auto(ctx, _) => ctx,
            BangLine::Concurrent(ctx) => ctx,
            BangLine::Handshake(ctx, _) => ctx,
            BangLine::HandshakeDelay(ctx, _) => ctx,
            BangLine::Python(ctx, _) => ctx,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum BoltVersion {
    V3_5, // TODO: Fix bolt 3.5 -> 3.0
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

    pub fn major(&self) -> u8 {
        match self {
            BoltVersion::V3_5 => 0,
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
        }
    }

    pub fn minor(&self) -> u8 {
        match self {
            BoltVersion::V3_5 => 0,
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
        }
    }
}

#[derive(Debug)]
pub struct Script {
    pub(crate) name: String,
    pub(crate) bang_lines: Vec<BangLine>,
    pub(crate) body: ScanBlock,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct Context {
    pub(crate) start_line_number: usize,
    pub(crate) end_line_number: usize,
}

impl Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lines {}-{}",
            self.start_line_number, self.end_line_number
        )
    }
}

impl Context {
    pub fn fuse(&self, other: &Context) -> Context {
        Context {
            start_line_number: min(self.start_line_number, other.start_line_number),
            end_line_number: max(self.end_line_number, other.end_line_number),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ScanBlock {
    List(Context, Vec<ScanBlock>),
    Alt(Context, Vec<ScanBlock>),
    Parallel(Context, Vec<ScanBlock>),
    Optional(Context, Box<ScanBlock>),
    Repeat0(Context, Box<ScanBlock>),
    Repeat1(Context, Box<ScanBlock>),
    ClientMessage(Context, String, Option<String>),
    ServerMessage(Context, String, Option<String>),
    AutoMessage(Context, String, Option<String>),
    Comment(Context),
    Python(Context, String),
    Condition(Context, CompositeConditionBlock),
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
    use super::Context;
    use crate::values::ClientMessage;

    use std::fmt::Debug;

    pub trait ScriptLine: Debug + Send + Sync {
        fn original_line(&self) -> &str;
    }

    pub trait ClientMessageValidator: ScriptLine {
        fn validate(&self, message: &ClientMessage) -> anyhow::Result<()>;
    }

    pub trait ServerMessageSender: ScriptLine + Send {
        fn send(&self) -> anyhow::Result<Vec<u8>>;
    }

    // impl ScriptLine for () {
    //     fn original_line(&self) -> &str {
    //         ""
    //     }
    // }
    //
    // impl ClientMessageValidator for () {
    //     fn validate(&self, _message: ClientMessage) -> anyhow::Result<()> {
    //         Ok(())
    //     }
    // }
    //
    // impl ServerMessageSender for () {
    //     fn send(&self, _stream: &mut TcpStream) -> anyhow::Result<()> {
    //         Ok(())
    //     }
    // }

    #[derive(Debug)]
    pub struct AutoMessageHandler {
        pub(crate) client_validator: Box<dyn ClientMessageValidator>,
        pub(crate) server_sender: Box<dyn ServerMessageSender>,
    }

    impl ClientMessageValidator for AutoMessageHandler {
        fn validate(&self, message: &ClientMessage) -> anyhow::Result<()> {
            self.client_validator.validate(message)
        }
    }

    impl ServerMessageSender for AutoMessageHandler {
        fn send(&self) -> anyhow::Result<Vec<u8>> {
            self.server_sender.send()
        }
    }

    impl ScriptLine for AutoMessageHandler {
        fn original_line(&self) -> &str {
            self.client_validator.original_line()
        }
    }

    #[derive(Debug)]
    pub enum ActorBlock {
        BlockList(Context, Vec<ActorBlock>),
        ClientMessageValidate(Context, Box<dyn ClientMessageValidator>),
        ServerMessageSend(Context, Box<dyn ServerMessageSender>),
        Python(Context, String),
        Alt(Context, Vec<ActorBlock>),
        Optional(Context, Box<ActorBlock>),
        Repeat(Context, Box<ActorBlock>, usize),
        AutoMessage(Context, AutoMessageHandler),
        NoOp(Context),
    }

    impl ActorBlock {
        pub fn ctx(&self) -> &Context {
            match self {
                ActorBlock::BlockList(ctx, _) => ctx,
                ActorBlock::ClientMessageValidate(ctx, _) => ctx,
                ActorBlock::ServerMessageSend(ctx, _) => ctx,
                ActorBlock::Python(ctx, _) => ctx,
                ActorBlock::Alt(ctx, _) => ctx,
                ActorBlock::Optional(ctx, _) => ctx,
                ActorBlock::Repeat(ctx, _, _) => ctx,
                ActorBlock::AutoMessage(ctx, _) => ctx,
                ActorBlock::NoOp(ctx) => ctx,
            }
        }
    }
}

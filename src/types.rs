use crate::bang_line::BangLine;
use crate::context::Context;

#[derive(Debug)]
pub struct Script {
    pub(crate) name: String,
    pub(crate) bang_lines: Vec<BangLine>,
    pub(crate) body: ScanBlock,
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
    use crate::context::Context;
    use crate::values::BoltMessage;

    use std::fmt::Debug;

    pub trait ScriptLine: Debug + Send + Sync {
        fn original_line<'a>(&self, script: &'a str) -> &'a str;
    }

    pub trait ClientMessageValidator: ScriptLine {
        fn validate(&self, message: &BoltMessage) -> anyhow::Result<()>;
    }

    pub trait ServerMessageSender: ScriptLine + Send {
        fn send(&self) -> anyhow::Result<&[u8]>;
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
        fn validate(&self, message: &BoltMessage) -> anyhow::Result<()> {
            self.client_validator.validate(message)
        }
    }

    impl ServerMessageSender for AutoMessageHandler {
        fn send(&self) -> anyhow::Result<&[u8]> {
            self.server_sender.send()
        }
    }

    impl ScriptLine for AutoMessageHandler {
        fn original_line<'a>(&self, script: &'a str) -> &'a str {
            self.client_validator.original_line(script)
        }
    }

    #[derive(Debug)]
    pub enum ActorBlock {
        BlockList(Context, Vec<ActorBlock>),
        ClientMessageValidate(Context, Box<dyn ClientMessageValidator>),
        ServerMessageSend(Context, Box<dyn ServerMessageSender>),
        Python(Context, String),
        Alt(Context, Vec<ActorBlock>),
        Parallel(Context, Vec<ActorBlock>),
        Optional(Context, Box<ActorBlock>),
        Repeat(Context, Box<ActorBlock>, usize),
        AutoMessage(Context, AutoMessageHandler),
        NoOp(Context),
    }

    impl ActorBlock {
        pub fn ctx(&self) -> &Context {
            match self {
                ActorBlock::BlockList(ctx, _)
                | ActorBlock::ClientMessageValidate(ctx, _)
                | ActorBlock::ServerMessageSend(ctx, _)
                | ActorBlock::Python(ctx, _)
                | ActorBlock::Alt(ctx, _)
                | ActorBlock::Parallel(ctx, _)
                | ActorBlock::Optional(ctx, _)
                | ActorBlock::Repeat(ctx, _, _)
                | ActorBlock::AutoMessage(ctx, _)
                | ActorBlock::NoOp(ctx) => ctx,
            }
        }
    }
}

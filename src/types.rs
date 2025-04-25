use crate::bang_line::BangLine;
use crate::context::Context;

#[derive(Debug)]
pub struct Script<'a> {
    pub(crate) name: &'a str,
    pub(crate) bang_lines: Vec<BangLine>,
    pub(crate) body: ScanBlock,
    pub(crate) input: &'a str,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ScanBlock {
    List(Context, Vec<ScanBlock>),
    Alt(Context, Vec<ScanBlock>),
    Parallel(Context, Vec<ScanBlock>),
    Optional(Context, Box<ScanBlock>),
    Repeat0(Context, Box<ScanBlock>),
    Repeat1(Context, Box<ScanBlock>),
    ClientMessage(Context, (Context, String), Option<(Context, String)>),
    ServerMessage(Context, (Context, String), Option<(Context, String)>),
    ServerAction(Context, (Context, String), Option<(Context, String)>),
    AutoMessage(Context, (Context, String), Option<(Context, String)>),
    Comment(Context),
    Python(Context, (Context, String)),
    ConditionPart(Branch, Context, Option<(Context, String)>, Box<ScanBlock>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Branch {
    If,
    Else,
    ElseIf,
}

pub mod actor_types {
    use std::fmt::Debug;
    use std::time::Duration;

    use crate::context::Context;
    use crate::values::bolt_message::BoltMessage;

    pub trait ScriptLine: Debug + Send + Sync {
        fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> &'c str;
        fn line_number(&self) -> Option<usize>;
    }

    pub trait ClientMessageValidator: ScriptLine {
        fn validate(&self, message: &BoltMessage) -> anyhow::Result<()>;
    }

    pub trait ServerMessageSender: ScriptLine + Send {
        fn send(&self) -> anyhow::Result<&[u8]>;
    }

    pub trait ServerActionLine: ScriptLine + Send {
        fn get_action(&self) -> &ServerAction;
    }

    #[derive(Debug, Clone)]
    pub enum ServerAction {
        Exit,
        Noop,
        Raw(Vec<u8>),
        Sleep(Duration),
        AssertOrder(Duration),
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

    // impl ClientMessageValidator for AutoMessageHandler {
    //     fn validate(&self, message: &BoltMessage) -> anyhow::Result<()> {
    //         self.client_validator.validate(message)
    //     }
    // }
    //
    // impl ServerMessageSender for AutoMessageHandler {
    //     fn send(&self) -> anyhow::Result<&[u8]> {
    //         self.server_sender.send()
    //     }
    // }
    //
    // impl ScriptLine for AutoMessageHandler {
    //     fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> &'c str {
    //         self.client_validator.line_repr(script)
    //     }
    // }

    #[derive(Debug)]
    pub enum ActorBlock {
        BlockList(Context, Vec<ActorBlock>),
        ClientMessageValidate(Context, Box<dyn ClientMessageValidator>),
        ServerMessageSend(Context, Box<dyn ServerMessageSender>),
        ServerActionLine(Context, Box<dyn ServerActionLine>),
        Python(Context, String),
        Condition(Context, Option<String>),
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
                | ActorBlock::ServerActionLine(ctx, _)
                | ActorBlock::Python(ctx, _)
                | ActorBlock::Condition(ctx, _)
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

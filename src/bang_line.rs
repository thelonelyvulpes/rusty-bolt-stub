use crate::context::Context;

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

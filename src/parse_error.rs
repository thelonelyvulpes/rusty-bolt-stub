use std::error::Error;

use crate::context::Context;

#[derive(Debug)]
pub struct ParseError {
    pub message: String,
    pub ctx: Option<Context>,
}

impl ParseError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ctx: None,
        }
    }

    pub fn new_ctx(ctx: Context, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ctx: Some(ctx),
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(ctx) = &self.ctx {
            write!(f, "{}: {}", ctx, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl Error for ParseError {}

impl From<serde_json::Error> for ParseError {
    fn from(err: serde_json::Error) -> Self {
        Self::new(err.to_string())
    }
}

impl From<String> for ParseError {
    fn from(err: String) -> Self {
        Self::new(err)
    }
}

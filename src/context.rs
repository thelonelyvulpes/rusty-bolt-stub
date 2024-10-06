use std::fmt::Display;
use std::cmp::{max, min};

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
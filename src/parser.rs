use crate::{BangLine, Block, BoltVersion, Script};
use anyhow::Error;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_until, take_while};
use nom::character::complete::{digit1, multispace0, space0, space1};
use nom::character::streaming::alpha1;
use nom::combinator::{eof, map_res, opt};
use nom::IResult;
use nom::sequence::{preceded, terminated};

pub fn parse_script(input: &'static str, name: &str) -> Result<Script, Error> {
    let (input, bangs) = parse_bangs(input)?;

    let (input, tree) = parse_tree(input)?;

    Ok(Script {
        name: name.into(),
        bang_lines: bangs,
    })
}

fn parse_tree(input: &str) -> IResult<&str, Block> {
    let (input, _) = multispace0(input)?;
    // let (input, _) = alt((client))(input)?;
    Ok((input, Block::BlockList(vec![])))
}

fn client(input: &str) -> IResult<&str, Block> {
    let (input, _) = tag("C:")(input)?;
    let (input, _) = space0(input)?;
    let (input, message) = alpha1(input)?;
    let (input, follow) = opt(preceded(space1, rest_of_line))(input)?;

    Ok((input, Block::BlockList(vec![])))
}

fn parse_bangs(input: &str) -> IResult<&str, Vec<BangLine>> {
    let (input, bangs) = nom::multi::many1(parse_bang)(input)?;

    Ok((input, bangs))
}

fn parse_bang(input: &str) -> IResult<&str, BangLine> {
    alt((parse_version, parse_allow_restart, parse_concurrent))(input)
}

fn rest_of_line(input: &str) -> IResult<&str, &str> {
   take_while(|c: char| c != '\n' && c != '\r')(input)
}

fn parse_version(input: &str) -> IResult<&str, BangLine> {
    let (input, _) = bang_line("BOLT", input)?;
    let (input, _) = space1(input)?;
    let (input, major) = map_res(digit1, |s: &str| s.parse::<u8>())(input)?;
    let (input, _) = tag(".")(input)?;
    let (input, minor) = map_res(digit1, |s: &str| s.parse::<u8>())(input)?;
    let (input, _) = end_of_line(input)?;

    match BoltVersion::match_valid_version(major, minor) {
        Some(version) => Ok((input, BangLine::Version(version))),
        None => Err(nom::Err::Error(nom::error::Error {
            input,
            code: nom::error::ErrorKind::Permutation,
        })),
    }
}

fn bang_line<'a>(expect: &'static str, input: &'a str) -> IResult<&'a str, ()> {
    let (input, _) = tag("!:")(input)?;
    let (input, _) = space0(input)?;
    let (input, _) = tag(expect)(input)?;
    Ok((input, ()))
}

fn parse_allow_restart(input: &str) -> IResult<&str, BangLine> {
    let (input, _) = bang_line("ALLOW RESTART", input)?;
    let (input, _) = end_of_line(input)?;
    Ok((input, BangLine::AllowRestart))
}

fn parse_concurrent(input: &str) -> IResult<&str, BangLine> {
    let (input, _) = bang_line("ALLOW CONCURRENT", input)?;
    let (input, _) = end_of_line(input)?;
    Ok((input, BangLine::Concurrent))
}

fn parse_auto(input: &str) -> IResult<&str, BangLine> {
    let (input, _) = bang_line("AUTO", input)?;
    let (input, _) = space1(input)?;
    let (input, message) = alpha1(input)?;
    let (input, _) = end_of_line(input)?;
    Ok((input, BangLine::Auto(message.to_owned())))
}

fn end_of_line(input: &str) -> IResult<&str, ()> {
    let (input, _) = space0(input)?;
    let (input, _) = opt(tag("\r"))(input)?;
    alt((tag("\n"), eof))(input).map(|(input, _)| (input, ()))
}

#[cfg(test)]
mod tests {
    use crate::{parser, BangLine, BoltVersion};

    #[test]
    fn test_parse_script() {
        let input = "!: BOLT 5.4";
        let result = dbg!(parser::parse_script(input, "test.script"));

        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_multiple_bangs() {
        let input = "!: BOLT 5.4\n!: ALLOW RESTART\n!: ALLOW CONCURRENT\r\n";
        let result = dbg!(parser::parse_script(input, "test.script"));

        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(result.bang_lines.len(), 3);
        assert_eq!(
            result.bang_lines.get(0),
            Some(&BangLine::Version(BoltVersion::V5_4))
        );
        assert_eq!(result.bang_lines.get(1), Some(&BangLine::AllowRestart));
        assert_eq!(result.bang_lines.get(2), Some(&BangLine::Concurrent));
    }

    #[test]
    fn test_parse_concurrent_ok() {
        let input = "!: ALLOW CONCURRENT\n";
        let result = parser::parse_concurrent(input);
        assert!(result.is_ok());
        let (rem, bang) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(bang, BangLine::Concurrent);
    }


    #[test]
    fn test_rest_of_line() {
        let input = "ALLOW CONCURRENT\r\n";
        let result = parser::rest_of_line(input);
        assert!(result.is_ok());
        let (rem, taken) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(taken, "ALLOW CONCURRENT");
    }
}

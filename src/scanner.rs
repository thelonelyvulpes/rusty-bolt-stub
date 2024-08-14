use crate::{BangLine, Block, Script};
use anyhow::Error;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while};
use nom::character::complete::{alpha1, multispace0, multispace1, space0, space1, u8};
use nom::combinator::{eof, map, opt, value};
use nom::error::ParseError;
use nom::multi::many1;
use nom::sequence::{preceded, separated_pair, terminated};
use nom::IResult;

pub fn scan_script(input: &'static str, name: &str) -> Result<Script, Error> {
    let (input, bangs) = scan_bang_lines(input)?;

    let (input, _) = multispace0::<_, nom::error::Error<&str>>(input)?;

    if input.is_empty() && bangs.iter().any(|x| matches!(x, BangLine::Version(_, _))) {
        return Ok(Script {
            name: name.into(),
            bang_lines: bangs,
            body: Block::BlockList(vec![]),
        });
    };

    let (input, body) = scan_tree(input)?;
    let (input, _) = multispace0::<_, nom::error::Error<&str>>(input)?;
    if !input.is_empty() {
        return Err(Error::msg("Trailing input"));
    }

    Ok(Script {
        name: name.into(),
        bang_lines: bangs,
        body,
    })
}

fn scan_tree(input: &str) -> IResult<&str, Block> {
    let (input, blocks) = many1(scan_block)(input)?;
    Ok((input, Block::BlockList(blocks)))
}

fn scan_block(input: &str) -> IResult<&str, Block> {
    let (input, _) = multispace0(input)?;
    alt((client, server, auto))(input)
}

fn prefixed_line(prefix: &'static str) -> impl FnMut(&str) -> IResult<&str, (&str, Option<&str>)> {
    move |input: &str| -> IResult<&str, (&str, Option<&str>)> {
        let (input, _) = tag(prefix)(input)?;
        let (input, _) = space0(input)?;
        let (input, message) = alpha1(input)?;
        let (input, args) = opt(preceded(space1, rest_of_line))(input)?;
        let (input, args) = match args {
            None => {
                let (input, _) = space0(input)?;
                (input, None)
            }
            Some(args) => (input, Some(args)),
        };
        Ok((input, (message, args)))
    }
}

fn client(input: &str) -> IResult<&str, Block> {
    let (input, (message, args)) = prefixed_line("C:")(input)?;
    Ok((
        input,
        Block::ClientMessage(message.into(), args.map(Into::into)),
    ))
}

fn server(input: &str) -> IResult<&str, Block> {
    let (input, (message, args)) = prefixed_line("S:")(input)?;
    Ok((
        input,
        Block::ServerMessage(message.into(), args.map(Into::into)),
    ))
}

fn auto(input: &str) -> IResult<&str, Block> {
    let (input, (message, args)) = prefixed_line("A:")(input)?;
    Ok((
        input,
        Block::AutoMessage(message.into(), args.map(Into::into)),
    ))
}

fn optional(input: &str) -> IResult<&str, Block> {
    let (input, (message, args)) = prefixed_line("*:")(input)?;
    Ok((
        input,
        Block::Repeat0(Box::new(Block::AutoMessage(
            message.into(),
            args.map(Into::into),
        ))),
    ))
}

fn scan_bang_lines(input: &str) -> IResult<&str, Vec<BangLine>> {
    many1(alt((
        auto_bang_line,
        bolt_version_bang_line,
        simple_bang_line("ALLOW RESTART", BangLine::AllowRestart),
        simple_bang_line("ALLOW CONCURRENT", BangLine::Concurrent),
    )))(input)
}

fn rest_of_line(input: &str) -> IResult<&str, &str> {
    let (input, mut line) = take_while(|c: char| c != '\n' && c != '\r')(input)?;
    line = line.trim();
    if line.is_empty() {
        return Err(nom::Err::Error(nom::error::Error {
            input,
            code: nom::error::ErrorKind::Complete,
        }));
    }
    Ok((input, line))
}

fn bolt_version_bang_line(input: &str) -> IResult<&str, BangLine> {
    preceded(
        bang_line("BOLT"),
        preceded(
            space1,
            map(separated_pair(u8, tag("."), u8), |(major, minor)| {
                BangLine::Version(major, minor)
            }),
        ),
    )(input)
}

fn bang_line<'a>(expect: &'static str) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str> {
    preceded(tag("!: "), preceded(space0, tag(expect)))
}

fn simple_bang_line<'a>(
    expect: &'static str,
    res: BangLine,
) -> impl FnMut(&'a str) -> IResult<&'a str, BangLine> {
    value(res, preceded(bang_line(expect), end_of_line))
}

fn auto_bang_line(input: &str) -> IResult<&str, BangLine> {
    map(
        preceded(
            bang_line("AUTO"),
            preceded(space1, terminated(alpha1, end_of_line)),
        ),
        |message| BangLine::Auto(message.to_owned()),
    )(input)
}

fn end_of_line(input: &str) -> IResult<&str, &str> {
    alt((multispace1, eof))(input)
}

#[cfg(test)]
mod tests {
    use crate::{scanner, BangLine};

    use rstest::rstest;

    #[test]
    fn test_eol() {
        let (input, eol) = scanner::end_of_line("\t \r\njeff").unwrap();
        assert_eq!(input, "jeff");
        assert_eq!(eol, "\t \r\n");
    }

    #[test]
    fn test_parse_minimal_script() {
        let input = "!: BOLT 5.5\n";
        let result = dbg!(scanner::scan_script(input, "test.script"));
        assert!(result.is_ok());
    }

    #[test]
    fn should_scan_u8() {
        let input = "!: BOLT 16.5\n";
        let result = dbg!(scanner::bolt_version_bang_line(input));
        assert!(result.is_ok());
        let (rem, bl) = result.unwrap();
        assert_eq!(rem, "\n");
        assert_eq!(bl, BangLine::Version(16, 5));
    }


    #[test]
    fn test_parse_multiple_bangs() {
        let input = "!: BOLT 5.4\n!: AUTO Nonsense\n!: ALLOW RESTART\n";
        let result = dbg!(scanner::scan_script(input, "test.script"));
        let result = result.unwrap();
        assert_eq!(result.bang_lines.len(), 3);
        assert_eq!(result.bang_lines.get(0), Some(&BangLine::Version(5, 4)));
        assert_eq!(
            result.bang_lines.get(1),
            Some(&BangLine::Auto("Nonsense".into()))
        );
        assert_eq!(result.bang_lines.get(2), Some(&BangLine::AllowRestart));
    }

    #[test]
    fn test_auto_bang_line() {
        let (input, bl) = scanner::auto_bang_line("!: AUTO Nonsense\n").unwrap();
        assert_eq!(input, "");
        assert_eq!(bl, BangLine::Auto("Nonsense".into()));
    }

    #[test]
    fn test_parse_concurrent_ok() {
        let input = "!: ALLOW CONCURRENT\n";
        let result = scanner::simple_bang_line("ALLOW CONCURRENT", BangLine::Concurrent)(input);
        let (rem, bang) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(bang, BangLine::Concurrent);
    }

    #[test]
    fn test_rest_of_line_trims() {
        let input = "ALLOW CONCURRENT\t\r\n";
        let result = scanner::rest_of_line(input);
        let (rem, taken) = result.unwrap();
        assert_eq!(rem, "\r\n");
        assert_eq!(taken, "ALLOW CONCURRENT");
    }

    #[rstest]
    #[case::crlf("\r\n")]
    #[case::lf("\n")]
    fn test_rest_of_line(#[case] eol: &str) {
        let input = format!("ALLOW CONCURRENT{eol}");
        let result = scanner::rest_of_line(input.as_str());
        let (rem, taken) = result.unwrap();
        assert_eq!(rem, eol);
        assert_eq!(taken, "ALLOW CONCURRENT");
    }

    #[rstest]
    #[case::no_space("C:RUN")]
    #[case::clean("C: RUN")]
    #[case::trailing("C: RUN  ")]
    #[case::messy("C:   RUN  ")]
    fn test_client_message_with_no_args(#[case] input: &str) {
        let result = scanner::client(input);
        let (rem, block) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(block, crate::Block::ClientMessage("RUN".into(), None));
    }

    #[rstest]
    #[case::no_space("C:RUN foo bar")]
    #[case::no_space("C: RUN foo bar")]
    #[case::trailing("C: RUN foo bar  ")]
    #[case::messy("C:  RUN   foo bar  ")]
    fn test_client_message_con_args(#[case] input: &str) {
        let result = scanner::client(input);
        let (rem, block) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(
            block,
            crate::Block::ClientMessage("RUN".into(), Some("foo bar".into()))
        );
    }
}

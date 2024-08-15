use crate::{BangLine, Block, Script};
use anyhow::anyhow;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{
    alpha1, line_ending, multispace0, not_line_ending, space0, space1, u8,
};
use nom::combinator::{cond, eof, map, opt, value};
use nom::error::{context, FromExternalError, ParseError};
use nom::multi::many1;
use nom::sequence::{pair, preceded, separated_pair, terminated};
use nom_supreme::error::ErrorTree;

type PError<I> = ErrorTree<I>;
type IResult<'a, O> = nom::IResult<&'a str, O, PError<&'a str>>;

pub fn scan_script<'a>(input: &'a str, name: &str) -> Result<Script, nom::Err<PError<&'a str>>> {
    let (input, bangs) =
        terminated(context("Bang line headers", scan_bang_lines), multispace0)(input)?;

    if input.is_empty() && bangs.iter().any(|x| matches!(x, BangLine::Version(_, _))) {
        return Ok(Script {
            name: name.into(),
            bang_lines: bangs,
            body: Block::List(vec![]),
        });
    };

    let (input, body) = terminated(opt(scan_body), multispace0)(input)?;
    if !input.is_empty() {
        return Err(nom::Err::Failure(PError::from_external_error(
            input,
            nom::error::ErrorKind::Complete,
            anyhow!("Trailing input"),
        )));
    }

    Ok(Script {
        name: name.into(),
        bang_lines: bangs,
        body: body.unwrap_or(Block::List(vec![])),
    })
}

fn scan_body(input: &str) -> IResult<Block> {
    let (input, blocks) = many1(scan_block)(input)?;
    Ok((input, Block::List(blocks)))
}

fn scan_block(input: &str) -> IResult<Block> {
    preceded(
        multispace0,
        alt((
            context("client line", message(Some("C:"), Block::ClientMessage)),
            context("server line", message(Some("S:"), Block::ServerMessage)),
            context("auto line", message(Some("A:"), Block::AutoMessage)),
            comment,
            // untagged line has to go last because it will match pretty much anything
            context("untagged line", message(None, Block::UntaggedMessage)),
        )),
    )(input)
}

fn prefixed_line<'a>(
    prefix: Option<&'static str>,
) -> impl FnMut(&'a str) -> IResult<(&'a str, Option<&'a str>)> {
    preceded(
        cond(
            prefix.is_some(),
            terminated(tag(prefix.unwrap_or("")), space0),
        ),
        pair(
            alpha1,
            terminated(opt(preceded(space1, rest_of_line)), space0),
        ),
    )
}

fn message<'a>(
    tag: Option<&'static str>,
    block: fn(String, Option<String>) -> Block,
) -> impl FnMut(&'a str) -> IResult<Block> {
    map(prefixed_line(tag), move |(message, args)| {
        block(message.into(), args.map(Into::into))
    })
}

fn comment(input: &str) -> IResult<Block> {
    map(preceded(tag("#"), rest_of_line), |_| Block::Comment)(input)
}

fn optional(input: &str) -> IResult<Block> {
    let (input, (message, args)) = prefixed_line(Some("*:"))(input)?;
    Ok((
        input,
        Block::Repeat0(Box::new(Block::AutoMessage(
            message.into(),
            args.map(Into::into),
        ))),
    ))
}

fn scan_bang_lines(input: &str) -> IResult<Vec<BangLine>> {
    many1(alt((
        context("auto bang", auto_bang_line),
        context("bolt version bang", bolt_version_bang_line),
        context(
            "allow restart",
            simple_bang_line("ALLOW RESTART", BangLine::AllowRestart),
        ),
        context(
            "allow concurrent",
            simple_bang_line("ALLOW CONCURRENT", BangLine::Concurrent),
        ),
    )))(input)
}

fn rest_of_line<'a, E: ParseError<&'a str>>(input: &'a str) -> nom::IResult<&'a str, &'a str, E> {
    let (input, mut line) = terminated(not_line_ending, end_of_line)(input)?;
    line = line.trim();
    if line.is_empty() {
        return Err(nom::Err::Error(E::from_error_kind(
            input,
            nom::error::ErrorKind::Complete,
        )));
    }
    Ok((input, line))
}

fn bolt_version_bang_line(input: &str) -> IResult<BangLine> {
    preceded(
        bang_line("BOLT"),
        preceded(
            space1,
            terminated(
                map(separated_pair(u8, tag("."), u8), |(major, minor)| {
                    BangLine::Version(major, minor)
                }),
                end_of_line,
            ),
        ),
    )(input)
}

fn bang_line<'a>(expect: &'static str) -> impl FnMut(&'a str) -> IResult<'a, &'a str> {
    preceded(tag("!:"), preceded(space1, tag(expect)))
}

fn simple_bang_line<'a>(
    expect: &'static str,
    res: BangLine,
) -> impl FnMut(&'a str) -> IResult<'a, BangLine> {
    value(res, preceded(bang_line(expect), end_of_line))
}

fn auto_bang_line(input: &str) -> IResult<BangLine> {
    map(
        preceded(
            bang_line("AUTO"),
            preceded(space1, terminated(alpha1, end_of_line)),
        ),
        |message| BangLine::Auto(message.to_owned()),
    )(input)
}

fn end_of_line<'a, E: ParseError<&'a str>>(input: &'a str) -> nom::IResult<&'a str, (), E> {
    let eol = alt((line_ending, eof));
    value((), preceded(space0, eol))(input)
}

#[cfg(test)]
mod tests {
    use super::super::{scanner, BangLine};
    use super::message;
    use crate::Block;

    use rstest::rstest;

    fn call_end_of_line(input: &str) -> super::IResult<()> {
        scanner::end_of_line(input)
    }

    #[rstest]
    #[case::crlf("\r\n")]
    #[case::lf("\n")]
    fn test_eol(#[case] eol: &str) {
        let input = format!("\t {eol}jeff");
        let (rem, ()) = call_end_of_line(&input).unwrap();
        assert_eq!(rem, "jeff");
    }

    #[test]
    fn test_scan_minimal_script() {
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
        assert_eq!(rem, "");
        assert_eq!(bl, BangLine::Version(16, 5));
    }

    #[rstest]
    #[case::bolt_5_4("!: BOLT 5.4\n", BangLine::Version(5, 4))]
    #[case::bolt_5_0("!: BOLT 5.0\n", BangLine::Version(5, 0))]
    // #[case::bolt_5("!: BOLT 5\n", BangLine::Version(5, 0))]
    #[case::restart("!: ALLOW RESTART\n", BangLine::AllowRestart)]
    #[case::auto_reset("!: AUTO RESET\n", BangLine::Auto("RESET".into()))]
    #[case::auto_foo("!: AUTO foo\n", BangLine::Auto("foo".into()))]
    #[case::concurrent("!: ALLOW CONCURRENT\n", BangLine::Concurrent)]
    // #[case::handshake("!: HANDSHAKE FF 00\n", BangLine::Handshake(vec![0xFF, 0x00]))]
    // #[case::handshake_empty("!: HANDSHAKE\n", BangLine::Handshake(vec![]))]
    // #[case::handshake_delay_zero("!: HANDSHAKE DELAY 0\n", BangLine::HandshakeDelay(0.))]
    // #[case::handshake_delay_negative("!: HANDSHAKE DELAY -1\n", BangLine::HandshakeDelay(-1.))]
    // #[case::handshake_delay_neg_f("!: HANDSHAKE DELAY -1.5\n", BangLine::HandshakeDelay(-1.5))]
    // #[case::handshake_delay_pos_f(
    //     "!: HANDSHAKE DELAY 12345.6789\n",
    //     BangLine::HandshakeDelay(12345.6789)
    // )]
    // #[case::handshake_delay_pos_f("!: PY FOO bar\tBaz\n", BangLine::Python("FOO bar\tBaz".into()))]
    fn test_scan_bang_line(
        #[case] input: &'static str,
        #[case] expected: BangLine,
        #[values(1, 3)] repetition: usize,
    ) {
        let input = input.repeat(repetition);
        let result = dbg!(scanner::scan_script(input.as_str(), "test.script"));
        let result = result.unwrap();
        assert_eq!(result.bang_lines.len(), repetition);
        for bl in result.bang_lines.iter() {
            assert_eq!(bl, &expected);
        }
    }

    #[test]
    fn test_scan_multiple_bangs() {
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
    fn test_auto_bang_line_script() {
        let result = dbg!(scanner::scan_script("!: AUTO Nonsense\n", "test.script"));
        let result = result.unwrap();
        assert_eq!(
            result.bang_lines.get(0),
            Some(&BangLine::Auto("Nonsense".into()))
        );
    }

    #[test]
    fn test_scan_concurrent_ok() {
        let input = "!: ALLOW CONCURRENT\n";
        let mut f = scanner::simple_bang_line("ALLOW CONCURRENT", BangLine::Concurrent);
        let result = f(input);
        let (rem, bang) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(bang, BangLine::Concurrent);
    }

    fn call_rest_of_line<'a>(input: &'a str) -> super::IResult<&'a str> {
        scanner::rest_of_line(input)
    }

    #[test]
    fn test_rest_of_line_trims() {
        let input = "ALLOW CONCURRENT\t\r\n";
        let result = call_rest_of_line(input);
        let (rem, taken) = result.unwrap();
        assert_eq!(taken, "ALLOW CONCURRENT");
        assert_eq!(rem, "");
    }

    #[rstest]
    #[case::crlf("\r\n")]
    #[case::lf("\n")]
    fn test_rest_of_line(
        #[case] eol: &str,
        #[values("ALLOW CONCURRENT", "A C ", " A C", "\tA\tC\t")] content: &str,
    ) {
        let input = format!("{content}{eol}");
        let result = call_rest_of_line(input.as_str());
        let (rem, taken) = result.unwrap();
        assert_eq!(taken, content.trim());
        assert_eq!(rem, "");
    }

    #[rstest]
    #[case::no_space("C:RUN")]
    #[case::clean("C: RUN")]
    #[case::trailing("C: RUN  ")]
    #[case::messy("C:   RUN  ")]
    fn test_client_message_with_no_args(#[case] input: &str) {
        let result = message(Some("C:"), Block::ClientMessage)(input);
        let (rem, block) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(block, Block::ClientMessage("RUN".into(), None));
    }

    #[rstest]
    #[case::no_space("C:RUN foo bar")]
    #[case::no_space("C: RUN foo bar")]
    #[case::trailing("C: RUN foo bar  ")]
    #[case::messy("C:  RUN   foo bar  ")]
    fn test_client_message_con_args(#[case] input: &str) {
        let result = message(Some("C:"), Block::ClientMessage)(input);
        let (rem, block) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(
            block,
            Block::ClientMessage("RUN".into(), Some("foo bar".into()))
        );
    }

    #[rstest]
    #[case::no_space("#RUN foo bar")]
    #[case::no_space("# RUN foo bar")]
    #[case::trailing("# RUN foo bar  ")]
    #[case::messy("#  RUN   foo bar  ")]
    fn test_comment(#[case] input: &str) {
        let result = scanner::comment(input);
        let (rem, block) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(block, Block::Comment);
    }

    #[test]
    fn test_failing_scan() {
        let input = "!: BOLT 5.4\n\nF: NOPE foo\n";
        let result = dbg!(scanner::scan_script(input, "test.script"));
        assert!(result.is_err());
        println!("{:}", result.unwrap_err());
    }
}

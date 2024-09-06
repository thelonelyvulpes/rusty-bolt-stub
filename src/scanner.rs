use crate::types::{BangLine, Context, ScanBlock, Script};
use anyhow::anyhow;
use nom::branch::{alt, Alt};
use nom::bytes::complete::tag;
use nom::character::complete::{
    alpha1, line_ending, multispace0, not_line_ending, space0, space1, u8,
};
use nom::combinator::{cond, consumed, eof, map, opt, peek, value};
use nom::error::{context, FromExternalError, ParseError};
use nom::multi::{many1, many_till, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated};
use nom::{AsChar, Compare, InputIter, InputLength, InputTakeAtPosition, Parser, Slice};
use nom_span::Spanned;
use nom_supreme::error::ErrorTree;
use std::cmp::max;
use std::ops::{Deref, Range, RangeFrom, RangeTo};

type PError<I> = ErrorTree<I>;
type Input<'a> = Spanned<&'a str>;
type IResult<'a, O> = nom::IResult<Input<'a>, O, PError<Input<'a>>>;

impl From<Input<'_>> for Context {
    fn from(value: Input) -> Self {
        let start_line_number = value.line();
        Self {
            start_line_number,
            end_line_number: start_line_number + max(1, value.data().lines().count()) - 1,
        }
    }
}

pub fn scan_script(input: &str, name: String) -> Result<Script, nom::Err<PError<Input>>> {
    let span = Spanned::new(input, true);
    let (span, bangs) =
        terminated(context("Bang line headers", scan_bang_lines), multispace0)(span)?;

    if input.is_empty()
        && bangs
            .iter()
            .any(|x| matches!(x, BangLine::Version(_, _, _)))
    {
        return Ok(Script {
            name,
            bang_lines: bangs,
            body: ScanBlock::List(span.into(), vec![]),
        });
    };

    let (span, body) = terminated(opt(scan_body), multispace0)(span)?;
    if !span.is_empty() {
        return Err(nom::Err::Failure(PError::from_external_error(
            span.into(),
            nom::error::ErrorKind::Complete,
            anyhow!("Trailing input"),
        )));
    }

    Ok(Script {
        name,
        bang_lines: bangs,
        body: body.unwrap_or(ScanBlock::List(span.into(), vec![])),
    })
}

// #################
// Header/Bang Lines
// #################
fn scan_bang_lines(input: Input) -> IResult<Vec<BangLine>> {
    many1(alt((
        context("auto bang", auto_bang_line),
        context("bolt version bang", bolt_version_bang_line),
        context(
            "allow restart",
            simple_bang_line("ALLOW RESTART", |b| BangLine::AllowRestart(b)),
        ),
        context(
            "allow concurrent",
            simple_bang_line("ALLOW CONCURRENT", |b| BangLine::Concurrent(b)),
            // TODO: add delay handshake
            //       handshake bytes
            //       python line
        ),
    )))(input)
}

fn bolt_version_bang_line(input: Input) -> IResult<BangLine> {
    preceded(
        bang_line("BOLT"),
        preceded(
            space1,
            terminated(
                alt((
                    map(
                        consumed(separated_pair(u8, tag("."), u8)),
                        |(i, (major, minor))| BangLine::Version(i.into(), major, Some(minor)),
                    ),
                    map(consumed(u8), |(i, major)| {
                        BangLine::Version(i.into(), major, None)
                    }),
                )),
                end_of_line,
            ),
        ),
    )(input)
}

fn auto_bang_line(input: Input) -> IResult<BangLine> {
    map(
        consumed(preceded(
            bang_line("AUTO"),
            preceded(space1, terminated(alpha1, end_of_line)),
        )),
        |(i, message)| BangLine::Auto(i.into(), message.into()),
    )(input)
}

fn simple_bang_line<'a, 'b>(
    expect: &'static str,
    res: impl FnOnce(Context) -> BangLine,
) -> impl FnMut(Input<'a>) -> IResult<'a, BangLine> {
    map(
        consumed(preceded(bang_line(expect), end_of_line)),
        move |(i, _)| res(i.into()),
    )
}

fn bang_line<'a>(expect: &'static str) -> impl FnMut(Input<'a>) -> IResult<'a, Input<'a>> {
    preceded(tag("!:"), preceded(space1, tag(expect)))
}

// ##########
//    Body
// ##########
fn wrap_block_vec(context: Context, blocks: Vec<ScanBlock>) -> ScanBlock {
    match blocks.len() {
        1 => blocks.into_iter().next().unwrap(),
        _ => ScanBlock::List(context, blocks),
    }
}

fn scan_body(input: Input) -> IResult<ScanBlock> {
    let (input, (i, blocks)) = consumed(many1(scan_block))(input)?;
    Ok((input, ScanBlock::List(i.into(), blocks)))
}

fn scan_block(input: Input) -> IResult<ScanBlock> {
    preceded(
        multispace0,
        alt((
            context(
                "alternative block",
                map(consumed(multiblock("{{", "}}", "----")), |(i, b)| {
                    ScanBlock::Alt(i.into(), b)
                }),
            ),
            context(
                "parallel block",
                map(consumed(multiblock("{{", "}}", "++++")), |(i, b)| {
                    ScanBlock::Parallel(i.into(), b)
                }),
            ),
            context(
                "simple block",
                map(consumed(block("{{", "}}")), |(i, b)| {
                    wrap_block_vec(i.into(), b)
                }),
            ),
            context(
                "repeat 0 block",
                map(consumed(block("{*", "*}")), |(i, b)| {
                    ScanBlock::Repeat0(i.into(), Box::new(wrap_block_vec(i.into(), b)))
                }),
            ),
            context(
                "repeat 1 block",
                map(consumed(block("{+", "+}")), |(i, b)| {
                    ScanBlock::Repeat1(i.into(), Box::new(wrap_block_vec(i.into(), b)))
                }),
            ),
            context(
                "optional block",
                map(consumed(block("{?", "?}")), |(i, b)| {
                    ScanBlock::Optional(i.into(), Box::new(wrap_block_vec(i.into(), b)))
                }),
            ),
            context("auto line", message(Some("A:"), ScanBlock::AutoMessage)),
            context("auto repeat 0 line", auto_repeat_0),
            context("auto repeat 1 line", auto_repeat_1),
            context("auto optional", auto_optional),
            context("comment line", comment),
            context(
                "python lines",
                message_simple_content(Some("PY:"), ScanBlock::Python),
            ),
            // TODO: Add IF, ELSE and ELIF blocks
            context(
                "client lines",
                multi_message(Some("C:"), ScanBlock::ClientMessage),
            ),
            context(
                "server lines",
                multi_message(Some("S:"), ScanBlock::ServerMessage),
            ),
        )),
    )(input)
}

fn keyword(input: Input) -> IResult<()> {
    void(alt((
        tag("{{"),
        tag("}}"),
        tag("----"),
        tag("++++"),
        tag("{*"),
        tag("*}"),
        tag("{+"),
        tag("+}"),
        tag("{?"),
        tag("?}"),
        tag("C:"),
        tag("S:"),
        tag("PY:"),
        tag("A:"),
        tag("*:"),
        tag("+:"),
        tag("?:"),
    )))(input)
}

// ############
// Simple Lines
// ############
fn multi_message<'a>(
    message_tag: Option<&'static str>,
    block: impl FnMut(Context, String, Option<String>) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<ScanBlock> {
    move |input| {
        let (input, (i, (head, (tail, _)))) = consumed(pair(
            many1(context("explicit line", message(message_tag, &block))),
            context(
                "implicit line",
                map(
                    opt(many_till(
                        message(None, &block),
                        peek(preceded(multispace0, alt((void(eof), keyword)))),
                    )),
                    Option::unwrap_or_default,
                ),
            ),
        ))(input)?;

        let blocks = head.into_iter().chain(tail).collect();
        Ok((input, wrap_block_vec(i.into(), blocks)))
    }
}

fn message<'a>(
    tag: Option<&'static str>,
    block: impl FnMut(Context, String, Option<String>) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<ScanBlock> {
    map(prefixed_line(tag), move |(message, args)| {
        block(message.into(), message.into(), args.map(Into::into))
    })
}

fn prefixed_line<'a>(
    prefix: Option<&'static str>,
) -> impl FnMut(Input<'a>) -> IResult<(&'a str, Option<&'a str>)> {
    preceded(
        cond(
            prefix.is_some(),
            terminated(tag(prefix.unwrap_or("")), space0),
        ),
        pair(
            alpha1,
            terminated(opt(preceded(space1, rest_of_line)), multispace0),
        ),
    )
}

fn message_simple_content<'a>(
    tag: Option<&'static str>,
    mut block: impl FnMut(Context, String) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<ScanBlock> {
    map(prefixed_line_simple_content(tag), move |content| {
        block(context.into(), content.into())
    })
}

fn prefixed_line_simple_content<'a>(
    prefix: Option<&'static str>,
) -> impl FnMut(Input<'a>) -> IResult<&'a str> {
    preceded(
        cond(
            prefix.is_some(),
            terminated(tag(prefix.unwrap_or("")), space0),
        ),
        terminated(preceded(space0, rest_of_line), multispace0),
    )
}

fn comment(input: Input) -> IResult<ScanBlock> {
    map(preceded(tag("#"), rest_of_line), |t| {
        ScanBlock::Comment(t.into())
    })(input)
}

// #################
// Logic/Flow Blocks
// #################
fn block<'a>(
    opening: &'static str,
    closing: &'static str,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<ScanBlock>> {
    preceded(
        terminated(tag(opening), end_of_line),
        terminated(
            many1(scan_block),
            preceded(space0, terminated(tag(closing), end_of_line)),
        ),
    )
}

// TODO: add tests
fn multiblock<'a>(
    opening: &'static str,
    closing: &'static str,
    sep: &'static str,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<ScanBlock>> {
    preceded(
        terminated(tag(opening), end_of_line),
        terminated(
            separated_list1(
                terminated(tag(sep), end_of_line),
                map(consumed(many1(scan_block)), wrap_block_vec),
            ),
            preceded(space0, terminated(tag(closing), end_of_line)),
        ),
    )
}

// ###############
// Syntactic Sugar
// ###############
// TODO: add tests
fn auto_repeat_0(input: Input) -> IResult<ScanBlock> {
    let (input, (message, args)) = prefixed_line(Some("*:"))(input)?;
    Ok((
        input,
        ScanBlock::Repeat0(
            message.into(),
            Box::new(ScanBlock::AutoMessage(
                message.into(),
                message.into(),
                args.map(Into::into),
            )),
        ),
    ))
}

// TODO: add tests
fn auto_repeat_1(input: Input) -> IResult<ScanBlock> {
    let (input, (message, args)) = prefixed_line(Some("+:"))(input)?;
    Ok((
        input,
        ScanBlock::Repeat1(
            message.into(),
            Box::new(ScanBlock::AutoMessage(
                message.into(),
                message.into(),
                args.map(Into::into),
            )),
        ),
    ))
}

// TODO: add tests
fn auto_optional(input: Input) -> IResult<ScanBlock> {
    let (input, (message, args)) = prefixed_line(Some("?:"))(input)?;
    Ok((
        input,
        ScanBlock::Optional(
            message.into(),
            Box::new(ScanBlock::AutoMessage(
                message.into(),
                message.into(),
                args.map(Into::into),
            )),
        ),
    ))
}

// #########
// Utilities
// #########
fn rest_of_line<'a, I: Clone, E: ParseError<I>>(input: I) -> nom::IResult<I, I, E> {
    let (input, mut line) = delimited(
        multispace0,
        terminated(not_line_ending, end_of_line),
        multispace0,
    )(input)?;
    // TODO: Replace delimited combinator with trim below
    if line.is_empty() {
        return Err(nom::Err::Error(E::from_error_kind(
            input,
            nom::error::ErrorKind::Complete,
        )));
    }
    Ok((input, line))
}

fn end_of_line<'a, E: ParseError<Input<'a>>>(
    input: Input<'a>,
) -> nom::IResult<Input<'a>, Input<'a>, E> {
    // let choices: nom::IResult<Input<'a>, Input<'a>, E> = eof(input);
    // let eol: nom::IResult<Input<'a>, Input<'a>, E> = alt((line_ending, eof))(input);
    // void(preceded(space0, line_ending))(input)
    line_ending(input)
}

fn void<F, I, O, E: ParseError<I>>(f: F) -> impl FnMut(I) -> nom::IResult<I, (), E>
where
    F: Parser<I, O, E>,
{
    value((), f)
}

pub fn trim<'a, O, E: ParseError<Input<'a>>, F>(inner: F) -> impl Parser<Input<'a>, O, E>
where
    F: Parser<Input<'a>, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

#[cfg(test)]
mod tests {
    use super::{message, multi_message};

    use crate::scanner;
    use crate::types::{BangLine, ScanBlock};
    use rstest::rstest;

    #[test]
    fn test_scan_minimal_script() {
        let input = "!: BOLT 5.5\n";
        let result = dbg!(scanner::scan_script(input, "test.script".into()));
        assert!(result.is_ok());
    }

    #[test]
    fn test_failing_scan() {
        let input = "!: BOLT 5.4\n\nF: NOPE foo\n";
        let result = dbg!(scanner::scan_script(input, "test.script".into()));
        assert!(result.is_err());
        println!("{:}", result.unwrap_err());
    }

    // #################
    // Header/Bang Lines
    // #################
    #[test]
    fn should_scan_u8() {
        let input = "!: BOLT 16.5\n";
        let result = dbg!(scanner::bolt_version_bang_line(input));
        assert!(result.is_ok());
        let (rem, bl) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(bl, BangLine::Version(16, Some(5)));
    }

    #[rstest]
    #[case::bolt_5_4("!: BOLT 5.4\n", BangLine::Version(5, Some(4)))]
    #[case::bolt_5_0("!: BOLT 5.0\n", BangLine::Version(5, Some(0)))]
    #[case::bolt_5("!: BOLT 5\n", BangLine::Version(5, None))]
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
        let result = dbg!(scanner::scan_script(input.as_str(), "test.script".into()));
        let result = result.unwrap();
        assert_eq!(result.bang_lines.len(), repetition);
        for bl in result.bang_lines.iter() {
            assert_eq!(bl, &expected);
        }
    }

    #[test]
    fn test_scan_multiple_bangs() {
        let input = "!: BOLT 5.4\n!: AUTO Nonsense\n!: ALLOW RESTART\n";
        let result = dbg!(scanner::scan_script(input, "test.script".into()));
        let result = result.unwrap();
        assert_eq!(result.bang_lines.len(), 3);
        assert_eq!(
            result.bang_lines.get(0),
            Some(&BangLine::Version(5, Some(4)))
        );
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
        let result = dbg!(scanner::scan_script(
            "!: AUTO Nonsense\n",
            "test.script".into()
        ));
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

    // ##########
    //    Body
    // ##########

    // ############
    // Simple Lines
    // ############
    #[rstest]
    #[case::implicit("C: Foo a b \n   Bar lel lol ")]
    #[case::explicit("C: Foo a b \nC: Bar lel lol ")]
    fn test_multi_message(
        #[case] input: &'static str,
        #[values("", "\n", "\nS: Baz\n", "\n \n\n  S: Baz\n", "\n?}", "\n?}")] ending: &'static str,
    ) {
        let input = format!("{input}{ending}");
        let (rem, block) = multi_message(Some("C:"), ScanBlock::ClientMessage)(&input).unwrap();
        assert_eq!(rem, ending.trim_start());
        assert_eq!(
            block,
            ScanBlock::List(vec![
                ScanBlock::ClientMessage("Foo".into(), Some("a b".into())),
                ScanBlock::ClientMessage("Bar".into(), Some("lel lol".into()))
            ])
        );
    }

    #[rstest]
    #[case::no_space("C:RUN")]
    #[case::clean("C: RUN")]
    #[case::trailing("C: RUN  ")]
    #[case::messy("C:   RUN  ")]
    fn test_client_message_with_no_args(#[case] input: &str) {
        let result = message(Some("C:"), ScanBlock::ClientMessage)(input);
        let (rem, block) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(block, ScanBlock::ClientMessage("RUN".into(), None));
    }

    #[rstest]
    #[case::no_space("C:RUN foo bar")]
    #[case::no_space("C: RUN foo bar")]
    #[case::trailing("C: RUN foo bar  ")]
    #[case::messy("C:  RUN   foo bar  ")]
    fn test_client_message_con_args(#[case] input: &str) {
        let result = message(Some("C:"), ScanBlock::ClientMessage)(input);
        let (rem, block) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(
            block,
            ScanBlock::ClientMessage("RUN".into(), Some("foo bar".into()))
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
        assert_eq!(block, ScanBlock::Comment);
    }

    #[rstest]
    fn test_python_line() {
        let input = "PY: print('Hello, World!')";
        let result = dbg!(scanner::message_simple_content(
            Some("PY:"),
            ScanBlock::Python
        )(input));
        let (rem, block) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(block, ScanBlock::Python("print('Hello, World!')".into()));
    }

    // #################
    // Logic/Flow Blocks
    // #################
    #[test]
    fn test_simple_block() {
        let input = "{{\n    C: RUN\n    S: OK\n}}";
        let result = dbg!(scanner::block("{{", "}}")(input));
        let (rem, blocks) = result.unwrap();
        assert_eq!(rem, "");
        assert_eq!(
            blocks,
            vec![
                ScanBlock::ClientMessage("RUN".into(), None),
                ScanBlock::ServerMessage("OK".into(), None),
            ]
        );
    }

    // ###############
    // Syntactic Sugar
    // ###############

    // #########
    // Utilities
    // #########
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
}

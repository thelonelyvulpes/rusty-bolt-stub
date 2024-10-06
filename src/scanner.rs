use crate::types::{ScanBlock, Script};
use anyhow::anyhow;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{line_ending, multispace0, not_line_ending, space0, space1, u8};
use nom::combinator::{cond, consumed, eof, map, opt, peek, recognize, value};
use nom::error::{context, ErrorKind, FromExternalError, ParseError};
use nom::multi::{many1, many_till};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated};
use nom::{AsChar, InputLength, InputTakeAtPosition, Parser};
use nom_span::Spanned;
use nom_supreme::error::ErrorTree;
use std::cmp::max;
use crate::bang_line::BangLine;
use crate::context::Context;

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
    let (span, bangs) = preceded(multispace0, context("Bang line headers", scan_bang_lines))(span)?;

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

    let (span, body) = delimited(multispace0, opt(scan_body), multispace0)(span)?;
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
    let (input, (ctx, (major, minor))) = preceded(
        multispace0,
        consumed(preceded(
            bang_line("BOLT"),
            preceded(
                space1,
                terminated(
                    alt((
                        map(separated_pair(u8, tag("."), u8), |(major, minor)| {
                            (major, Some(minor))
                        }),
                        map(u8, |major| (major, None)),
                    )),
                    peek(end_of_line),
                ),
            ),
        )),
    )(input)?;
    Ok((input, BangLine::Version(ctx.into(), major, minor)))
}

fn auto_bang_line(input: Input) -> IResult<BangLine> {
    map(
        preceded(
            multispace0,
            consumed(preceded(
                bang_line("AUTO"),
                preceded(space1, terminated(message_name, peek(end_of_line))),
            )),
        ),
        |(ctx, message)| BangLine::Auto(ctx.into(), String::from(message.trim())),
    )(input)
}

fn simple_bang_line<'a, 'b>(
    expect: &'static str,
    mut res: impl FnMut(Context) -> BangLine,
) -> impl FnMut(Input<'a>) -> IResult<'a, BangLine> {
    map(
        preceded(
            multispace0,
            terminated(recognize(bang_line(expect)), peek(end_of_line)),
        ),
        move |ctx| res(ctx.into()),
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
        tag("#"),
    )))(input)
}

// ############
// Simple Lines
// ############
fn multi_message<'a, 'b>(
    message_tag: Option<&'static str>,
    mut block: impl FnMut(Context, String, Option<String>) -> ScanBlock + 'b,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> + 'b {
    move |input| {
        let (input, (ctx, blocks)) = consumed(multi_message_vec(message_tag, &mut block))(input)?;
        Ok((input, wrap_block_vec(ctx.into(), blocks)))
    }
}

fn multi_message_vec<'a>(
    message_tag: Option<&'static str>,
    mut block: impl FnMut(Context, String, Option<String>) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<ScanBlock>> {
    move |input| {
        let (input, head) =
            many1(context("explicit line", message(message_tag, &mut block)))(input)?;
        let (input, (tail, _)) = context(
            "implicit line",
            map(
                opt(many_till(
                    context("implicit line", message(None, &mut block)),
                    peek(preceded(multispace0, alt((void(eof), keyword)))),
                )),
                Option::unwrap_or_default,
            ),
        )(input)?;
        Ok((input, head.into_iter().chain(tail).collect()))
    }
}

fn message<'a>(
    tag: Option<&'static str>,
    mut block: impl FnMut(Context, String, Option<String>) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> {
    map(
        terminated(
            preceded(multispace0, consumed(prefixed_line(tag))),
            peek(end_of_line),
        ),
        move |(ctx, (message, arg))| {
            block(
                ctx.into(),
                String::from(message.trim()),
                arg.map(|arg| String::from(arg.trim())),
            )
        },
    )
}

fn prefixed_line<'a>(
    prefix: Option<&'static str>,
) -> impl FnMut(Input<'a>) -> IResult<(Input<'a>, Option<Input<'a>>)> {
    preceded(
        multispace0,
        preceded(
            cond(
                prefix.is_some(),
                terminated(tag(prefix.unwrap_or("")), space0),
            ),
            pair(
                context("message name", message_name),
                context(
                    "message body",
                    alt((
                        map(preceded(space1, rest_of_line), Some),
                        value(None, terminated(space0, peek(end_of_line))),
                    )),
                ),
            ),
        ),
    )
}

fn message_simple_content<'a>(
    tag: Option<&'static str>,
    mut block: impl FnMut(Context, String) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<ScanBlock> {
    map(
        preceded(
            multispace0,
            terminated(
                consumed(prefixed_line_simple_content(tag)),
                peek(end_of_line),
            ),
        ),
        move |(ctx, content)| block(ctx.into(), String::from(content.trim())),
    )
}

fn prefixed_line_simple_content<'a>(
    prefix: Option<&'static str>,
) -> impl FnMut(Input<'a>) -> IResult<Input<'a>> {
    preceded(
        cond(
            prefix.is_some(),
            terminated(tag(prefix.unwrap_or_default()), space0),
        ),
        preceded(space0, rest_of_line),
    )
}

fn comment(input: Input) -> IResult<ScanBlock> {
    map(
        preceded(
            multispace0,
            terminated(
                recognize(preceded(tag("#"), rest_of_line)),
                peek(end_of_line),
            ),
        ),
        |ctx| ScanBlock::Comment(ctx.into()),
    )(input)
}

// #################
// Logic/Flow Blocks
// #################
fn block<'a>(
    opening: &'static str,
    closing: &'static str,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<ScanBlock>> {
    preceded(
        multispace0,
        preceded(
            terminated(tag(opening), end_of_line),
            terminated(
                many1(scan_block),
                preceded(multispace0, terminated(tag(closing), end_of_line)),
            ),
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
        delimited(multispace0, tag(opening), end_of_line),
        terminated(
            separated_list2(
                delimited(multispace0, tag(sep), end_of_line),
                map(
                    preceded(multispace0, consumed(many1(scan_block))),
                    |(ctx, blocks)| wrap_block_vec(ctx.into(), blocks),
                ),
            ),
            delimited(multispace0, tag(closing), end_of_line),
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
                String::from(message.trim()),
                args.map(|s| String::from(s.trim())),
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
                String::from(message.trim()),
                args.map(|s| String::from(s.trim())),
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
                String::from(message.trim()),
                args.map(|s| String::from(s.trim())),
            )),
        ),
    ))
}

// #########
// Utilities
// #########
fn message_name<T, E: ParseError<T>>(input: T) -> nom::IResult<T, T, E>
where
    T: InputTakeAtPosition,
    <T as InputTakeAtPosition>::Item: AsChar + Clone,
{
    input.split_at_position1_complete(
        |item| !(item.clone().is_alphanum() || item.as_char() == '_'),
        ErrorKind::Alpha,
    )
}

fn rest_of_line<'a, E: ParseError<Input<'a>>>(
    input: Input<'a>,
) -> nom::IResult<Input<'a>, Input<'a>, E> {
    let (input, line) = terminated(not_line_ending, peek(end_of_line))(input)?;
    if line.is_empty() {
        return Err(nom::Err::Error(E::from_error_kind(
            input,
            nom::error::ErrorKind::Complete,
        )));
    }
    Ok((input, line))
}

fn end_of_line<'a, E: ParseError<Input<'a>>>(input: Input<'a>) -> nom::IResult<Input<'a>, (), E> {
    let eol = alt((line_ending, eof));
    void(preceded(space0, eol))(input)
}

fn void<F, I, O, E: ParseError<I>>(f: F) -> impl FnMut(I) -> nom::IResult<I, (), E>
where
    F: Parser<I, O, E>,
{
    value((), f)
}

pub fn separated_list2<I, O, O2, E, F, G>(
    mut sep: G,
    mut f: F,
) -> impl FnMut(I) -> nom::IResult<I, Vec<O>, E>
where
    I: Clone + InputLength,
    F: Parser<I, O, E>,
    G: Parser<I, O2, E>,
    E: ParseError<I>,
{
    move |mut i: I| {
        let mut res = Vec::new();

        // Parse the first element
        match f.parse(i.clone()) {
            Err(e) => return Err(e),
            Ok((i1, o)) => {
                res.push(o);
                i = i1;
            }
        }

        loop {
            let len = i.input_len();
            match sep.parse(i.clone()) {
                Err(nom::Err::Error(e)) => {
                    if res.len() == 1 {
                        return Err(nom::Err::Error(e));
                    }
                    return Ok((i, res));
                }
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    // infinite loop check: the parser must always consume
                    if i1.input_len() == len {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            ErrorKind::SeparatedList,
                        )));
                    }

                    match f.parse(i1.clone()) {
                        Err(nom::Err::Error(_)) => return Ok((i, res)),
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            res.push(o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use nom_span::Spanned;
    use rstest::rstest;
    use crate::bang_line::BangLine;
    use crate::context::Context;
    use super::super::scanner;
    use super::{message, multi_message, Input};
    use crate::types::{ScanBlock};

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

    fn wrap_input<'a>(input: &'a str) -> Input<'a> {
        Spanned::new(input, true)
    }

    fn new_ctx(start_line_number: usize, end_line_number: usize) -> Context {
        Context {
            start_line_number,
            end_line_number,
        }
    }

    // #################
    // Header/Bang Lines
    // #################
    #[test]
    fn should_scan_u8() {
        let input = wrap_input("!: BOLT 16.5\n");
        let result = dbg!(scanner::bolt_version_bang_line(input));
        assert!(result.is_ok());
        let (rem, bl) = result.unwrap();
        assert_eq!(*rem, "\n");
        assert_eq!(bl, BangLine::Version(new_ctx(1, 1), 16, Some(5)));
    }

    #[rstest]
    #[case::bolt_5_4("!: BOLT 5.4\n", BangLine::Version(new_ctx(1, 1), 5, Some(4)))]
    #[case::bolt_5_0("!: BOLT 5.0\n", BangLine::Version(new_ctx(1, 1), 5, Some(0)))]
    #[case::bolt_5("!: BOLT 5\n", BangLine::Version(new_ctx(1, 1), 5, None))]
    #[case::restart("!: ALLOW RESTART\n", BangLine::AllowRestart(new_ctx(1, 1)))]
    #[case::auto_reset("!: AUTO RESET\n", BangLine::Auto(new_ctx(1, 1),"RESET".into()))]
    #[case::auto_foo("!: AUTO foo\n", BangLine::Auto(new_ctx(1, 1),"foo".into()))]
    #[case::concurrent("!: ALLOW CONCURRENT\n", BangLine::Concurrent(new_ctx(1, 1)))]
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
        #[case] mut expected: BangLine,
        #[values(1, 3)] repetition: usize,
    ) {
        let input = input.repeat(repetition);
        let result = dbg!(scanner::scan_script(input.as_str(), "test.script".into()));
        let result = result.unwrap();
        assert_eq!(result.bang_lines.len(), repetition);
        for (bl, line) in result.bang_lines.iter().zip(1..repetition + 1) {
            *(expected.ctx_mut()) = new_ctx(line, line);
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
            Some(&BangLine::Version(new_ctx(1, 1), 5, Some(4)))
        );
        assert_eq!(
            result.bang_lines.get(1),
            Some(&BangLine::Auto(new_ctx(2, 2), "Nonsense".into()))
        );
        assert_eq!(
            result.bang_lines.get(2),
            Some(&BangLine::AllowRestart(new_ctx(3, 3)))
        );
    }

    #[rstest]
    fn test_auto_bang_line(#[values("", "\n", "\t", " ", "\t\n\n\t  ")] ending: &'static str) {
        let input = format!("!: AUTO Nonsense{ending}");
        let (input, bl) = scanner::auto_bang_line(wrap_input(&input)).unwrap();
        assert_eq!(*input, ending);
        assert_eq!(bl, BangLine::Auto(new_ctx(1, 1), "Nonsense".into()));
    }

    #[rstest]
    fn test_auto_bang_line_script(
        #[values("", "\n", "\t", " ", "\t\n\n\t  ")] ending: &'static str,
    ) {
        let input = format!("!: AUTO Nonsense{ending}");
        let result = dbg!(scanner::scan_script(&input, "test.script".into()));
        let result = result.unwrap();
        assert_eq!(
            result.bang_lines.get(0),
            Some(&BangLine::Auto(new_ctx(1, 1), "Nonsense".into()))
        );
    }

    #[rstest]
    fn test_scan_concurrent_ok(#[values("", "\n", "\t", " ", "\t\n\n\t  ")] ending: &'static str) {
        let input = format!("!: ALLOW CONCURRENT{ending}");
        let input = wrap_input(&input);
        let mut f = scanner::simple_bang_line("ALLOW CONCURRENT", BangLine::Concurrent);
        let result = f(input);
        let (rem, bang) = result.unwrap();
        assert_eq!(*rem, ending);
        assert_eq!(bang, BangLine::Concurrent(new_ctx(1, 1)));
    }

    // ##########
    //    Body
    // ##########

    // ############
    // Simple Lines
    // ############
    #[rstest]
    #[case::implicit("C: Foo a b\n   Bar lel lol")]
    #[case::explicit("C: Foo a b\nC: Bar lel lol")]
    #[case::implicit_space_post("C: Foo a b \n   Bar lel lol ")]
    #[case::explicit_space_post("C: Foo a b \nC: Bar lel lol ")]
    #[case::implicit_space_pre("C:  Foo a b \n    Bar lel lol")]
    #[case::explicit_space_pre("C:  Foo a b \nC:  Bar lel lol")]
    fn test_multi_message(
        #[case] input: &'static str,
        #[values("", "\n", "\nS: Baz\n", "\n \n\n  S: Baz\n", "\n?}", "\n?}")] ending: &'static str,
    ) {
        let input = format!("{input}{ending}");
        let (rem, block) = dbg!(multi_message(Some("C:"), ScanBlock::ClientMessage)(
            wrap_input(&input)
        ))
        .unwrap();
        assert_eq!(*rem, ending);
        assert_eq!(
            block,
            ScanBlock::List(
                new_ctx(1, 2),
                vec![
                    ScanBlock::ClientMessage(new_ctx(1, 1), "Foo".into(), Some("a b".into())),
                    ScanBlock::ClientMessage(new_ctx(2, 2), "Bar".into(), Some("lel lol".into()))
                ]
            )
        );
    }

    #[test]
    fn test_foo() {
        let input = "C: Foo a b\nC: Bar lel lol";
        let ending = "\nS:Baz\n";
        let input = format!("{input}{ending}");
        let (rem, block) = dbg!(multi_message(Some("C:"), ScanBlock::ClientMessage)(
            wrap_input(&input)
        ))
        .unwrap();
        assert_eq!(*rem, ending);
        assert_eq!(
            block,
            ScanBlock::List(
                new_ctx(1, 2),
                vec![
                    ScanBlock::ClientMessage(new_ctx(1, 1), "Foo".into(), Some("a b".into())),
                    ScanBlock::ClientMessage(new_ctx(2, 2), "Bar".into(), Some("lel lol".into()))
                ]
            )
        );
    }

    #[rstest]
    #[case::no_space("C:RUN")]
    #[case::clean("C: RUN")]
    #[case::trailing("C: RUN  ")]
    #[case::messy("C:   RUN  ")]
    fn test_client_message_with_no_args(#[case] input: &str) {
        let result = message(Some("C:"), ScanBlock::ClientMessage)(wrap_input(input));
        let (rem, block) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            block,
            ScanBlock::ClientMessage(new_ctx(1, 1), "RUN".into(), None)
        );
    }

    #[rstest]
    #[case::no_space("C:RUN foo bar")]
    #[case::no_space("C: RUN foo bar")]
    #[case::trailing("C: RUN foo bar  ")]
    #[case::messy("C:  RUN   foo bar  ")]
    fn test_client_message_con_args(#[case] input: &str) {
        let result = message(Some("C:"), ScanBlock::ClientMessage)(wrap_input(input));
        let (rem, block) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            block,
            ScanBlock::ClientMessage(new_ctx(1, 1), "RUN".into(), Some("foo bar".into()))
        );
    }

    #[rstest]
    #[case::no_space("#C:RUN foo bar")]
    #[case::no_space("#C: RUN foo bar")]
    #[case::trailing("#C: RUN foo bar  ")]
    #[case::messy("#C:  RUN   foo bar  ")]
    fn test_comment(#[case] input: &str) {
        let result = scanner::comment(wrap_input(input));
        let (rem, block) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(block, ScanBlock::Comment(new_ctx(1, 1)));
    }

    #[rstest]
    fn test_python_line() {
        let input = wrap_input("PY: print('Hello, World!')");
        let result = dbg!(scanner::message_simple_content(
            Some("PY:"),
            ScanBlock::Python
        )(input));
        let (rem, block) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            block,
            ScanBlock::Python(new_ctx(1, 1), "print('Hello, World!')".into())
        );
    }

    // #################
    // Logic/Flow Blocks
    // #################
    #[test]
    fn test_simple_block() {
        let input = wrap_input("{{\n    C: RUN\n    S: OK\n}}");
        let result = dbg!(scanner::block("{{", "}}")(input));
        let (rem, blocks) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            blocks,
            vec![
                ScanBlock::ClientMessage(new_ctx(2, 2), "RUN".into(), None),
                ScanBlock::ServerMessage(new_ctx(3, 3), "OK".into(), None),
            ]
        );
    }

    #[test]
    fn test_multi_block() {
        let input = wrap_input("{{\n    C: RUN1\n    S: OK\n----\n    C: RUN2\n}}");
        let result = dbg!(scanner::multiblock("{{", "}}", "----")(input));
        let (rem, blocks) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            blocks,
            vec![
                ScanBlock::List(
                    new_ctx(2, 3),
                    vec![
                        ScanBlock::ClientMessage(new_ctx(2, 2), "RUN1".into(), None),
                        ScanBlock::ServerMessage(new_ctx(3, 3), "OK".into(), None),
                    ]
                ),
                ScanBlock::ClientMessage(new_ctx(5, 5), "RUN2".into(), None),
            ]
        );
    }

    // ###############
    // Syntactic Sugar
    // ###############

    // #########
    // Utilities
    // #########
    fn call_message_name(input: &str) -> super::IResult<Input> {
        scanner::message_name(wrap_input(input))
    }

    #[rstest]
    #[case::trailing_space("FOO ", "FOO", " ")]
    #[case::trailing_tab("FOO\t", "FOO", "\t")]
    #[case::trailing_lf("FOO\n", "FOO", "\n")]
    #[case::trailing_crlf("FOO\r\n", "FOO", "\r\n")]
    #[case::simple("FOO", "FOO", "")]
    #[case::alphanum("F2OO3", "F2OO3", "")]
    #[case::alphanum_underscore("F2O_O3", "F2O_O3", "")]
    #[case::dash("ABC-DEF", "ABC", "-DEF")]
    #[case::slash("ABC/DEF", "ABC", "/DEF")]
    fn test_message_name(
        #[case] input: &str,
        #[case] expected_taken: &str,
        #[case] expected_rem: &str,
    ) {
        let result = dbg!(call_message_name(input));
        let (rem, taken) = result.unwrap();
        assert_eq!(*rem, expected_rem);
        assert_eq!(*taken, expected_taken);
    }

    #[rstest]
    #[case::empty("")]
    #[case::leading_space(" FOO")]
    #[case::leading_tab("\tFOO")]
    #[case::leading_lf("\nFOO")]
    #[case::leading_crlf("\r\nFOO")]
    fn test_not_message_name(#[case] input: &str) {
        let result = dbg!(call_message_name(input));
        result.unwrap_err();
    }

    fn call_end_of_line(input: &str) -> super::IResult<()> {
        scanner::end_of_line(wrap_input(input))
    }

    #[rstest]
    #[case::crlf("\r\n")]
    #[case::lf("\n")]
    fn test_eol(#[case] eol: &str) {
        let input = format!("\t {eol}jeff");
        let (rem, ()) = call_end_of_line(&input).unwrap();
        assert_eq!(*rem, "jeff");
    }

    fn call_rest_of_line(input: &str) -> super::IResult<Input> {
        scanner::rest_of_line(wrap_input(input))
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
        assert_eq!(*taken, content);
        assert_eq!(*rem, eol);
    }

    #[test]
    fn test_full_script() {
        let input = indoc! {r#"
            !: BOLT 5.6

            C: HELLO {"{}": "*"}
            S: SUCCESS {"server": "Neo4j/5.23.0, "connection_id": "bolt-123456789"}
            A: LOGON {"{}": "*"}
            *: RESET
            {?
                ?: TELEMETRY {"{}": "*"}
                C: RUN {"U": "*"} {"{}": "*"} {"{}": "*"}
                S: SUCCESS {"fields": ["n.name"]}
                {{
                    C: PULL {"n": {"Z": "*"}}
                ----
                    C: DISCARD {"n": {"Z": "*"}}
                }}
                S: SUCCESS {"type": "w"}
            ?}

            *: RESET
            ?: GOODBYE"#};

        dbg!(scanner::scan_script(input, "test.script".into())).unwrap();
    }
}

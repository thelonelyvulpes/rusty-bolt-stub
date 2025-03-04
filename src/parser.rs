use std::cell::LazyCell;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::result::Result as StdResult;
use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use itertools::Itertools;
use nom::{Parser, Slice};
use regex::Regex;
use serde_json::{Deserializer, Value as JsonValue};

use crate::bang_line::BangLine;
use crate::bolt_encode;
use crate::bolt_version::BoltVersion;
use crate::parse_error::ParseError;
use crate::str_byte;
use crate::types::actor_types::{
    ActorBlock, AutoMessageHandler, ClientMessageValidator, ScriptLine, ServerMessageSender,
};
use crate::types::{ScanBlock, Script};
use crate::values::value::Value;
use crate::values::BoltMessage;

#[derive(Debug)]
pub struct ActorScript {
    pub config: ActorConfig,
    pub tree: ActorBlock,
    pub script: Script,
}

#[derive(Debug, Default)]
pub struct ActorConfig {
    pub bolt_version: BoltVersion,
    pub allow_restart: bool,
    pub allow_concurrent: bool,
    pub handshake_delay: Option<Duration>,
    pub handshake: Option<Vec<u8>>,
}

type Result<T> = StdResult<T, ParseError>;
type ValidateValueFn = Box<dyn Fn(&Value) -> anyhow::Result<()> + 'static + Send + Sync>;
type ValidateValuesFn = Box<dyn Fn(&[Value]) -> anyhow::Result<()> + 'static + Send + Sync>;

pub fn contextualize_res<T>(res: Result<T>, script: &str) -> anyhow::Result<T> {
    fn script_excerpt(script: &str, start: usize, end: usize) -> String {
        const CONTEXT_LINES: usize = 3;

        let line_num_max = script.lines().count();
        let line_num_width = line_num_max.to_string().len();
        let lines = script.lines().enumerate();
        let mut excerpt_lines = Vec::<String>::with_capacity(end - start + 3 + 2 * CONTEXT_LINES);
        if start.saturating_sub(CONTEXT_LINES) > 1 {
            excerpt_lines.push(format!("  {: >width$} ...", "", width = line_num_width).into());
        }
        for (mut line_num, line) in lines {
            line_num += 1;
            if line_num < start.saturating_sub(CONTEXT_LINES) {
                // pre context
                continue;
            }
            if line_num < start {
                // context before the excerpt
                excerpt_lines.push(
                    format!("  {: >width$} {}", line_num, line, width = line_num_width).into(),
                );
                continue;
            }
            if line_num <= end {
                // the excerpt
                excerpt_lines.push(
                    format!("> {: >width$} {}", line_num, line, width = line_num_width).into(),
                );
                continue;
            }
            // context after the excerpt
            excerpt_lines
                .push(format!("  {: >width$} {}", line_num, line, width = line_num_width).into());
            if line_num >= end.saturating_add(CONTEXT_LINES) {
                // past context
                break;
            }
        }
        if end.saturating_add(CONTEXT_LINES) < line_num_max {
            excerpt_lines.push(format!("  {: >width$} ...", "", width = line_num_width).into());
        }
        excerpt_lines.join("\n")
    }

    match res {
        Ok(t) => Ok(t),
        Err(mut e) => {
            let ctx = e.ctx.take();
            Err(match ctx {
                None => anyhow::anyhow!("Error parsing script: {}", e.message),
                Some(ctx) => {
                    if ctx.start_line_number == ctx.end_line_number {
                        anyhow::anyhow!(
                            "Error parsing script on line ({}): {}\n{}",
                            ctx.start_line_number,
                            e.message,
                            script_excerpt(script, ctx.start_line_number, ctx.end_line_number)
                        )
                    } else {
                        anyhow::anyhow!(
                            "Error parsing script on lines ({}-{}): {}\n{}",
                            ctx.start_line_number,
                            ctx.end_line_number,
                            e.message,
                            script_excerpt(script, ctx.start_line_number, ctx.end_line_number)
                        )
                    }
                }
            })
        }
    }
}

pub fn parse(script: Script) -> Result<ActorScript> {
    let config = parse_config(&script.bang_lines)?;
    let tree = parse_block(&script.body, &config)?;

    Ok(ActorScript {
        config,
        tree,
        script,
    })
}

fn parse_config(bang_lines: &[BangLine]) -> Result<ActorConfig> {
    let mut bolt_version: Option<BoltVersion> = None;
    let mut allow_restart: Option<bool> = None;
    let mut allow_concurrent: Option<bool> = None;
    let mut handshake: Option<Vec<u8>> = None;
    let mut handshake_delay: Option<Duration> = None;

    for bang_line in bang_lines {
        match bang_line {
            BangLine::Version(ctx, major, minor) => {
                if bolt_version.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple BOLT version bang lines found",
                    ));
                }
                let bolt = BoltVersion::match_valid_version(*major, minor)
                    .ok_or(ParseError::new_ctx(*ctx, "Invalid BOLT version"))?;
                bolt_version = Some(bolt);
            }
            BangLine::AllowRestart(ctx) => {
                if allow_restart.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple allow restart bang lines found",
                    ));
                }
                allow_restart = Some(true);
            }
            BangLine::Auto(_, _) => {
                todo!("Auto blocks are not yet supported in the actor")
            }
            BangLine::Concurrent(ctx) => {
                if allow_concurrent.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple allow concurrent bang lines found",
                    ));
                }
                allow_concurrent = Some(true);
            }
            BangLine::Handshake(ctx, byte_str) => {
                if handshake.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple handshake bang lines found",
                    ));
                }

                match str_byte::str_to_data(byte_str) {
                    Ok(data) => {
                        handshake = Some(data);
                    }
                    Err(e) => {
                        return Err(ParseError::new(e.to_string()));
                    }
                }
            }
            BangLine::HandshakeDelay(_, _) => {
                todo!("Parse handshake delay")
            }
            BangLine::Python(_, _) => {
                todo!("Python blocks are not yet supported in the actor")
            }
        }
    }

    if handshake.is_some() && bolt_version.is_some() {
        todo!("Report err")
    }

    let bolt_version = bolt_version.ok_or(ParseError::new("Bolt version not specified"))?;
    let allow_restart = allow_restart.unwrap_or(false);
    let allow_concurrent = allow_concurrent.unwrap_or(false);
    Ok(ActorConfig {
        bolt_version,
        allow_restart,
        allow_concurrent,
        handshake,
        handshake_delay,
    })
}

fn parse_block(block: &ScanBlock, config: &ActorConfig) -> Result<ActorBlock> {
    match block {
        ScanBlock::List(ctx, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for scan_block in scan_blocks {
                let b = parse_block(scan_block, config)?;
                if matches!(b, ActorBlock::NoOp(_)) {
                    continue;
                }
                actor_blocks.push(b);
            }
            validate_list_children(&actor_blocks)?;

            match actor_blocks.len() {
                0 => Ok(ActorBlock::NoOp(*ctx)),
                1 => Ok(actor_blocks.remove(0)),
                _ => {
                    let actor_blocks = condense_actor_blocks(actor_blocks);
                    Ok(ActorBlock::BlockList(*ctx, actor_blocks))
                }
            }
        }
        ScanBlock::Alt(ctx, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for block in scan_blocks {
                let b = parse_block(block, config)?;
                validate_alt_child(&b)?;
                actor_blocks.push(b);
            }
            Ok(ActorBlock::Alt(*ctx, actor_blocks))
        }
        ScanBlock::Parallel(_, _) => {
            todo!("Parallel blocks are not yet supported in the actor")
        }
        ScanBlock::Optional(ctx, optional_scan_block) => {
            // TODO: Handle bad optional blocks
            let b = parse_block(optional_scan_block, config)?;
            validate_non_action(&b, Some("inside an optional block"))?;
            validate_non_empty(&b, Some("inside an optional block"))?;
            Ok(ActorBlock::Optional(*ctx, Box::new(b)))
        }
        ScanBlock::Repeat0(ctx, b) => {
            let b = parse_block(b, config)?;
            validate_non_action(&b, Some("inside a repeat block"))?;
            validate_non_empty(&b, Some("inside a repeat block"))?;
            Ok(ActorBlock::Repeat(*ctx, Box::new(b), 0))
        }
        ScanBlock::Repeat1(ctx, b) => {
            let b = parse_block(b, config)?;
            validate_non_action(&b, Some("inside a repeat block"))?;
            validate_non_empty(&b, Some("inside a repeat block"))?;
            Ok(ActorBlock::Repeat(*ctx, Box::new(b), 1))
        }
        ScanBlock::ClientMessage(ctx, message_name, body_string) => {
            let validator = create_validator(message_name, body_string.as_deref(), config)?;
            Ok(ActorBlock::ClientMessageValidate(*ctx, validator))
        }
        ScanBlock::ServerMessage(ctx, message_name, body_string) => {
            // TODO: Implement parser for the special messages like S: <Sleep 2>
            let server_message_sender = create_message_sender(message_name, body_string, config)?;
            Ok(ActorBlock::ServerMessageSend(*ctx, server_message_sender))
        }
        ScanBlock::AutoMessage(ctx, client_message_name, client_body_string) => {
            let validator =
                create_validator(client_message_name, client_body_string.as_deref(), config)?;
            let server_message_sender = create_auto_message_sender(client_message_name, config)?;
            Ok(ActorBlock::AutoMessage(
                *ctx,
                AutoMessageHandler {
                    client_validator: validator,
                    server_sender: server_message_sender,
                },
            ))
        }
        ScanBlock::Comment(ctx) => Ok(ActorBlock::NoOp(*ctx)),
        ScanBlock::Python(_, _) => {
            todo!("Python blocks are not yet supported in the actor")
        }
        ScanBlock::Condition(_, _) => {
            todo!("Python blocks are not yet supported in the actor")
        }
    }
}

fn condense_actor_blocks(p0: Vec<ActorBlock>) -> Vec<ActorBlock> {
    todo!()
}

fn create_auto_message_sender(
    client_message_tag: &str,
    config: &ActorConfig,
) -> Result<Box<dyn ServerMessageSender>> {
    // return Ok(Box::new(()));
    todo!("Create the auto message sending component, composed with a validator by caller")
}

fn create_message_sender(
    message_name: &str,
    message_body: &Option<String>,
    config: &ActorConfig,
) -> Result<Box<dyn ServerMessageSender>> {
    // return Ok(Box::new(()));
    let data = vec![];

    Ok(Box::new(SenderBytes::new(data, message_name, message_body)))
}

struct SenderBytes {
    pub data: Vec<u8>,
    pub message_name: String,
    pub message_body: Option<String>,
}

impl SenderBytes {
    pub fn new(data: Vec<u8>, message_name: &str, message_body: &Option<String>) -> Self {
        Self {
            data,
            message_name: message_name.to_string(),
            message_body: message_body.clone(),
        }
    }
}

impl ScriptLine for SenderBytes {
    fn original_line(&self) -> &str {
        ""
    }
}

impl Debug for SenderBytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.message_body {
            None => write!(f, "{}", self.message_name),
            Some(body) => write!(f, "{} {}", self.message_name, body),
        }
    }
}

impl ServerMessageSender for SenderBytes {
    fn send(&self) -> anyhow::Result<&[u8]> {
        Ok(&self.data)
    }
}

struct ValidatorImpl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> {
    pub func: T,
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> Debug for ValidatorImpl<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> ScriptLine for ValidatorImpl<T> {
    fn original_line(&self) -> &str {
        todo!()
    }
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> ClientMessageValidator
    for ValidatorImpl<T>
{
    fn validate(&self, message: &BoltMessage) -> anyhow::Result<()> {
        (self.func)(message)
    }
}

fn create_validator(
    expected_tag: &str,
    expected_body_string: Option<&str>,
    config: &ActorConfig,
) -> Result<Box<dyn ClientMessageValidator>> {
    let expected_tag = message_tag_from_name(expected_tag, config)?;
    let expected_body_behavior = build_fields_validator(expected_body_string, config)?;
    Ok(Box::new(ValidatorImpl {
        func: move |client_message| {
            if client_message.tag != expected_tag {
                return Err(anyhow!("the tags did not match."));
            }
            expected_body_behavior(&client_message.fields)
        },
    }))
}

fn build_no_fields_validator(message: &[Value]) -> anyhow::Result<()> {
    match message.len() {
        0 => Err(anyhow!("Expected 0 fields")),
        _ => Ok(()),
    }
}

fn build_field_validator(field: JsonValue, config: &ActorConfig) -> Result<ValidateValueFn> {
    Ok(match field {
        JsonValue::Null => Box::new(|msg| match msg {
            Value::Null => Ok(()),
            _ => Err(anyhow!("Expected null")),
        }),
        JsonValue::Bool(expected) => Box::new(move |msg| match msg {
            Value::Boolean(received) if received == &expected => Ok(()),
            _ => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
        }),
        JsonValue::Number(expected) if expected.is_i64() => {
            let expected = expected.as_i64().expect("checked in match arm");
            Box::new(move |msg| match msg {
                Value::Integer(received) if received == &expected => Ok(()),
                _ => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
            })
        }
        JsonValue::Number(expected) if expected.is_f64() => {
            let expected = expected.as_f64().expect("checked in match arm");
            assert!(
                expected.is_finite(),
                "json does not allow for NaN or Inf. Therefore, we don't handle those here",
            );
            Box::new(move |msg| match msg {
                Value::Float(received) if received == &expected => Ok(()),
                _ => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
            })
        }
        JsonValue::Number(expected) => {
            return Err(ParseError::new(format!(
                "Can't parse number (must be i64 or f64) {:?}",
                expected
            )));
        }
        JsonValue::String(expected) => Box::new(move |msg| match msg {
            _ if expected == "*" => Ok(()),
            Value::String(received) => validate_str_field_eq(&expected, received),
            _ => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
        }),
        JsonValue::Array(expected) => {
            let validators = expected
                .into_iter()
                .map(|value| build_field_validator(value, config))
                .collect::<Result<Vec<_>>>()?;
            Box::new(move |msg| {
                let Value::List(received) = msg else {
                    return Err(anyhow!("Expected list, found {:?}", msg));
                };
                if validators.len() != received.len() {
                    return Err(anyhow!(
                        "Expected {} fields, found {}",
                        validators.len(),
                        received.len()
                    ));
                }
                for (validator, received) in validators.iter().zip(received.iter()) {
                    validator(received)?;
                }
                Ok(())
            })
        }
        JsonValue::Object(expected) => {
            let mut required_keys = HashSet::new();
            let mut unique_keys = HashSet::new();
            let validators = expected
                .into_iter()
                .map(|(key, expect_for_key)| {
                    let (key, validator, req) =
                        build_map_entry_validator(&key, expect_for_key, config)?;
                    if req {
                        required_keys.insert(key.clone());
                    }
                    if !unique_keys.insert(key.clone()) {
                        return Err(ParseError::new(format!(
                            "contains same unescaped key twice {key}"
                        )));
                    }
                    Ok((key, validator))
                })
                .collect::<Result<HashMap<_, _>>>()?;

            Box::new(move |msg| {
                let Value::Map(received) = msg else {
                    return Err(anyhow!("Expected map, found {:?}", msg));
                };

                let mut matched_keys = HashSet::new();

                for (key, value) in received {
                    let Some(validator) = validators.get(key) else {
                        return Err(anyhow!("Unexpected Key:{key} was received."));
                    };
                    validator(value)?;
                    matched_keys.insert(key.clone());
                }

                match matched_keys.is_superset(&required_keys) {
                    true => Ok(()),
                    false => {
                        let mut missing_keys = required_keys.difference(&matched_keys);
                        Err(anyhow!(
                            "Received message with missing keys: {:?}",
                            missing_keys.join(", ")
                        ))
                    }
                }
            })
            // try to build jolt matcher (incl. Structs like datetime)
            //   * if has 1 key-value pair
            //   * && key is known sigil
            //   * Special string "*" applies: E.g., `C: RUN {"Z": "*"}` will match any integer: `C: RUN 1` and `C: RUN 2`, but not `C: RUN 1.2` or `C: RUN "*"`.
        }
    })
}

/// # Returns
///  * String: the map key this validator should be applied to with markers stripped from it.
///  * ValidateValueFn: the validator function
///  * bool: whether the key is required (`true`), or optional (`false`).
fn build_map_entry_validator(
    key: &str,
    expected: JsonValue,
    config: &ActorConfig,
) -> Result<(String, ValidateValueFn, bool)> {
    //  JSON object keys:
    //    * The key will be **unescaped** before it is matched:
    //      `\\`, `\[`, `\]`, `\{`, and `\}` are turned into `\`, `[`, `]`, `{`, and `}` respectively.
    //    * If the escaped key starts with `[` and ends with `]`, the corresponding key/value pair is **optional**.
    //      E.g., `C: PULL {"[n]": 1000}` matches `C: PULL {}` and `C: PULL {"n": 1000}`, but not `C: PULL {"n": 1000, "m": 1001}`,  `C: PULL {"n": 1}`, or  `C: PULL null`.
    //    * If the escaped key ends on `{}` after potential optional-brackets (s. above) have been stripped, the corresponding value will be compared **sorted** if it's a list.
    //      E.g., `C: MSG {"foo{}": [1, 2]}` will match `C: MSG {"foo": [1, 2]}` and `C: MSG {"foo": [2, 1]}`, but `C: MSG {"foo{}": "ba"}` will not match `C: MSG {"foo": "ab"}`.
    //    * Example for **optional** and **sorted**: `C: MSG {"[foo{}]": [1, 2]}`.

    let expected = match build_jolt_validator(key, expected, config)? {
        IsJoltValidator::Yes(jolt_validator) => return Ok((key.to_string(), jolt_validator, true)),
        IsJoltValidator::No(expected) => expected,
    };

    let key = ParsedMapKey::parse(key);
    let required = !key.is_optional;
    let ordered = key.is_ordered;
    let key = key.unescaped;
    let validator: ValidateValueFn = match expected {
        JsonValue::Array(expected) if ordered => {
            let validators = expected
                .into_iter()
                .map(|value| build_field_validator(value, config))
                .collect::<Result<Vec<_>>>()?;
            Box::new(move |msg| {
                let Value::List(received) = msg else {
                    return Err(anyhow!("Expected list, found {:?}", msg));
                };
                if validators.len() != received.len() {
                    return Err(anyhow!(
                        "Expected {} fields, found {}",
                        validators.len(),
                        received.len()
                    ));
                }
                let mut left_validators = HashSet::new();
                left_validators.extend((0..validators.len()).into_iter());
                'values: for value_received in received.iter() {
                    for validator_idx in &left_validators {
                        let validator_idx = *validator_idx;
                        let validator = &validators[validator_idx];
                        if let Ok(()) = validator(value_received) {
                            left_validators.remove(&validator_idx);
                            continue 'values;
                        }
                    }
                    return Err(anyhow!(
                        "Unexpected value in any order array: {:?}",
                        value_received
                    ));
                }
                Ok(())
            })
        }
        _ => build_field_validator(expected, config)?,
    };
    Ok((key, validator, required))
}

enum IsJoltValidator {
    Yes(ValidateValueFn),
    No(JsonValue),
}

fn build_jolt_validator(
    key: &str,
    expected: JsonValue,
    config: &ActorConfig,
) -> Result<IsJoltValidator> {
    // https://docs.google.com/document/d/1QK4OcC0tZ08lKqVr-3-z8HpPY9jFeEh6zZPhMm15D-w/edit?tab=t.0
    let JsonValue::Object(expected) = expected else {
        return Ok(IsJoltValidator::No(expected));
    };
    if expected.len() != 1 {
        return Ok(IsJoltValidator::No(JsonValue::Object(expected)));
    }
    let (sigil, expected) = expected.into_iter().next().expect("non-empty check above");
    let (versionless_sigil, jolt_version) = parse_jolt_sigil(&sigil, config)?;
    Ok(match versionless_sigil {
        "?" => {
            if !expected.is_boolean() {
                return Err(ParseError::new(format!(
                    "Expected bool after sigil \"?\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        "Z" => {
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"Z\", but found {expected:?}",
                )));
            };
            let expected = i64::from_str(&expected).map_err(|e| {
                ParseError::new(format!(
                    "Failed to parse i64 after sigil \"Z\": {expected:?} because {e}",
                ))
            })?;
            IsJoltValidator::Yes(build_field_validator(
                JsonValue::Number(expected.into()),
                config,
            )?)
        }
        "R" => {
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"R\", but found {expected:?}",
                )));
            };
            let expected = f64::from_str(&expected).map_err(|e| {
                ParseError::new(format!(
                    "Failed to parse f64 after sigil \"R\": {expected:?} because {e}",
                ))
            })?;
            IsJoltValidator::Yes(Box::new(move |msg| match msg == &Value::Float(expected) {
                true => Ok(()),
                false => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
            }))
        }
        "U" => {
            if !expected.is_string() {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"U\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        "#" => {
            let bytes = match expected {
                JsonValue::String(expected) => parse_hex_string(&expected)?,
                JsonValue::Array(expected) => {
                    let mut bytes = Vec::with_capacity(expected.len());
                    for (i, b) in expected.into_iter().enumerate() {
                        let Some(b) = b.as_i64() else {
                            return Err(ParseError::new(format!(
                                "Array after sigil \"#\" must contain only ints, but found {b:?} (at {i})",
                            )));
                        };
                        let b: u8 = b.try_into().map_err(|_| {
                            ParseError::new(format!(
                                "Array after sigil \"#\" must contain only u8, but found {b} (at {i})",
                            ))
                        })?;
                        bytes.push(b);
                    }
                    bytes
                }
                _ => {
                    return Err(ParseError::new(format!(
                        "Expected string or array after sigil \"#\", but found {expected:?}",
                    )));
                }
            };
            IsJoltValidator::Yes(Box::new(move |msg| match msg {
                Value::Bytes(received) if &bytes == received => Ok(()),
                _ => Err(anyhow!("Expected bytes {:?} found {:?}", &bytes, msg)),
            }))
        }
        "[]" => {
            if !expected.is_array() {
                return Err(ParseError::new(format!(
                    "Expected array after sigil \"[]\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        "T" => {
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected temporal string after sigil \"T\", but found {expected:?}",
                )));
            };
            todo!("Implement temporal string parsing :-!")
        }
        "@" => {
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected spatial string after sigil \"@\", but found {expected:?}",
                )));
            };
            thread_local! {
                static SPATIAL_RE: LazyCell<Regex> = LazyCell::new(|| {
                    Regex::new(concat!(
                        r"^(?:SRID=([^;]+);)?\s*",
                        r"POINT\s*\(((?:(?:\S+)?\s+){1,2}\S+)\)$",
                    )).unwrap()
                });
            }

            let captures = SPATIAL_RE.with(|re| re.captures(&expected));
            let Some(captures) = captures else {
                return Err(ParseError::new(format!(
                    "Expected valid spatial string after sigil \"@\", \
                    e.g., \"SRID=7203;POINT(1 2)\" found: {expected:?}"
                )));
            };
            let Some(srid_match) = captures.get(1) else {
                return Err(ParseError::new(format!(
                    "Spatial string (after sigil \"@\") requires an SRID, \
                    e.g., \"SRID=7203;POINT(1 2)\" found: {expected:?}"
                )));
            };
            let srid = srid_match.as_str();
            let srid = i64::from_str(srid).map_err(|e| {
                ParseError::new(format!(
                    "Spatial string (after sigil \"@\") contained non-i64 srid {srid:?}): {e}"
                ))
            })?;
            let coords = dbg!(&captures[2]);
            let coords = coords
                .split_whitespace()
                .enumerate()
                .map(|(i, c)| {
                    f64::from_str(c).map_err(|e| {
                        ParseError::new(format!(
                            "Spatial string (after sigil \"@\") contained non-f64 coordinate \
                            {c:?} (at {i}): {e}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let expected = Value::Struct(match coords.as_slice() {
                [x, y] => bolt_encode::read_point_2d(srid, *x, *y),
                [x, y, z] => bolt_encode::read_point_3d(srid, *x, *y, *z),
                _ => panic!("Regex asserts exactly 2 or 3 coordinates"),
            });
            IsJoltValidator::Yes(Box::new(move |msg| match msg == &expected {
                true => Ok(()),
                false => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
            }))
        }
        "()" => match jolt_version {
            JoltVersion::V1 => {
                todo!("implement node parsing")
            }
            JoltVersion::V2 => {
                todo!("implement node parsing")
            }
        },
        "->" => match jolt_version {
            JoltVersion::V1 => {
                todo!("implement relationship parsing")
            }
            JoltVersion::V2 => {
                todo!("implement relationship parsing")
            }
        },
        "<-" => match jolt_version {
            JoltVersion::V1 => {
                todo!("implement relationship parsing")
            }
            JoltVersion::V2 => {
                todo!("implement relationship parsing")
            }
        },
        ".." => match jolt_version {
            JoltVersion::V1 => {
                todo!("implement path parsing")
            }
            JoltVersion::V2 => {
                todo!("implement path parsing")
            }
        },
        _ => IsJoltValidator::No(expected),
    })
}

fn parse_jolt_sigil<'a>(sigil: &'a str, config: &ActorConfig) -> Result<(&'a str, JoltVersion)> {
    thread_local! {
        static SIGIL_VERSION_RE: LazyCell<Regex> = LazyCell::new(|| {
            Regex::new(r"^(.+?)(?:v(\d+))?$").unwrap()
        });
    }
    SIGIL_VERSION_RE.with(|re| {
        let jolt_version = if config.bolt_version >= BoltVersion::V5_0 {
            JoltVersion::V2
        } else {
            JoltVersion::V1
        };
        let Some(captures) = re.captures(sigil) else {
            return Ok((sigil, jolt_version));
        };
        let Some(version_overwrite) = captures.get(1) else {
            return Ok((sigil, jolt_version));
        };
        let version_overwrite = version_overwrite.as_str();
        let jolt_version = JoltVersion::parse(version_overwrite)?;
        Ok((
            captures
                .get(0)
                .expect("regex enforces one group to exist")
                .as_str(),
            jolt_version,
        ))
    })
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum JoltVersion {
    V1,
    V2,
}

impl JoltVersion {
    fn parse(s: &str) -> Result<Self> {
        let jolt_version = i32::from_str(s).map_err(|e| {
            ParseError::new(format!("Jolt version must be i32 (found {s:?}): {e}",))
        })?;
        Ok(match jolt_version {
            1 => Self::V1,
            2 => Self::V2,
            _ => return Err(ParseError::new(format!("Unknown jolt version: {s}"))),
        })
    }
}

fn parse_hex_string(s: &str) -> Result<Vec<u8>> {
    if s.is_empty() {
        return Ok(vec![]);
    }
    if s.chars()
        .next()
        .expect("checked for empty above")
        .is_whitespace()
    {
        return Err(ParseError::new("Hex string may not start with whitespace"));
    }
    let mut result = Vec::new();
    let mut start_offset = None;
    let mut start_idx = None;
    for (i, (offset, ch)) in s.char_indices().enumerate() {
        if ch.is_whitespace() {
            if start_offset.is_none() {
                continue;
            }
            return Err(ParseError::new(format!(
                "Hex sting contains unexpected whitespace {ch} (at {i}): \
                whitespace may only occur between pairs of characters"
            )));
        }
        if !ch.is_ascii_hexdigit() {
            return Err(ParseError::new(format!(
                "Hex sting contains invalid hex character {ch} (at {i})"
            )));
        }
        let start_offset_val = *start_offset.get_or_insert(offset);
        let start_idx_val = *start_idx.get_or_insert(i);
        if i == start_idx_val + 1 {
            result.push(
                u8::from_str_radix(&s[start_offset_val..offset + ch.len_utf8()], 16)
                    .expect("checked for valid hex u8 above"),
            );
            start_offset = None;
            start_idx = None;
        }
    }
    if let Some(start_offset) = start_offset {
        return Err(ParseError::new(format!(
            "Hex string has non-paired trailing character(s) {:?}",
            &s[start_offset..]
        )));
    }
    Ok(result)
}

struct ParsedMapKey {
    pub is_optional: bool,
    pub is_ordered: bool,
    pub unescaped: String,
}

impl ParsedMapKey {
    fn parse(key: &str) -> Self {
        thread_local! {
            static UNESCAPE_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(\[)?(?:\\[\\\{}\}\[\]]|[^\\])*?(\{\})?(\])?$").unwrap()
            });
        }
        let unescaped = UNESCAPE_RE.with(|re| re.replace_all(key, r"$1"));

        let mut unescaped_ref = unescaped.as_ref();
        let is_optional = key.starts_with('[') && key.ends_with(']');
        let is_ordered = if is_optional {
            key.ends_with("{}]")
        } else {
            key.ends_with("{}")
        };

        if is_optional {
            unescaped_ref = &unescaped_ref[1..unescaped.len() - 1]
        }
        if is_ordered {
            unescaped_ref = &unescaped_ref[..unescaped_ref.len() - 2]
        }

        Self {
            is_optional,
            is_ordered,
            unescaped: unescaped_ref.to_string(),
        }
    }
}

fn validate_str_field_eq(expected: &str, received: &str) -> anyhow::Result<()> {
    #[inline]
    fn match_(expected: &str, received: &str) -> anyhow::Result<()> {
        match expected == received {
            true => Ok(()),
            false => Err(anyhow!("Expected {:?} found {:?}", expected, received)),
        }
    }
    if !expected.contains(r"\") {
        match_(expected, received)
    } else {
        let escaped = expected.replace(r"\\", r"\");
        match_(&escaped, received)
    }
}

fn build_fields_validator(
    msg_body: Option<&str>,
    config: &ActorConfig,
) -> Result<ValidateValuesFn> {
    let Some(line) = msg_body else {
        return Ok(Box::new(|msg| build_no_fields_validator(msg)));
    };
    // RUN "RETURN $n AS n" {"n": 1}
    let fields: Vec<_> = Deserializer::from_str(line)
        .into_iter()
        .collect::<StdResult<Vec<JsonValue>, _>>()?
        .into_iter()
        .map(|field| build_field_validator(field, config))
        .collect::<Result<_>>()?;
    let field_validators = fields;

    todo!();

    // Ok(Box::new(move |msg| {
    //     for field in fields {
    //
    //     }
    // })

    // for field in Deserializer::from_str(line).into_iter() {
    //     let field = field?;
    //     validate_field(field)?;
    // }
    // let v: Vec<StdResult<Value, _>> =
    //     Deserializer::from_str("[1, 2] [3] null 1 false {\"a\": null}")
    //         .into_iter()
    //         .collect();
    // dbg!(v);
    // [1, 2] "h ello" {"a": false}
    // let v: Value = serde_json::from_str(line)?;
    Ok(Box::new(|_| Ok(())))
}

fn message_tag_from_name(tag_name: &str, config: &ActorConfig) -> Result<u8> {
    todo!("Take a message name, and return the byte dependent on the bolt version, as pull and pullall both use the same tag byte.")
}

fn is_skippable(block: &ActorBlock) -> bool {
    match block {
        ActorBlock::BlockList(_, blocks) | ActorBlock::Alt(_, blocks) => {
            blocks.iter().all(is_skippable)
        }
        ActorBlock::Repeat(_, block, count) => *count == 0 || is_skippable(block),
        ActorBlock::Optional(..) | ActorBlock::NoOp(..) => true,
        ActorBlock::ClientMessageValidate(..)
        | ActorBlock::ServerMessageSend(..)
        | ActorBlock::Python(..)
        | ActorBlock::AutoMessage(..) => false,
    }
}

fn has_deterministic_end(block: &ActorBlock) -> bool {
    match block {
        ActorBlock::BlockList(_, blocks) => blocks
            .iter()
            .rev()
            .filter(|b| !matches!(b, ActorBlock::NoOp(_)))
            .map(has_deterministic_end)
            .next()
            .unwrap_or(true),
        ActorBlock::ClientMessageValidate(..)
        | ActorBlock::ServerMessageSend(..)
        | ActorBlock::Python(..) => true,
        ActorBlock::Alt(_, blocks) => blocks.iter().all(has_deterministic_end),
        ActorBlock::Optional(..) | ActorBlock::Repeat(..) => false,
        ActorBlock::AutoMessage(..) => true,
        ActorBlock::NoOp(..) => true,
    }
}

fn is_action_block(block: &ActorBlock) -> bool {
    match block {
        ActorBlock::Python(..) | ActorBlock::ServerMessageSend(..) => true,
        ActorBlock::BlockList(_, blocks) => 'arm: {
            for block in blocks {
                if is_action_block(block) {
                    break 'arm true;
                }
                if !is_skippable(block) {
                    break;
                }
            }
            break 'arm false;
        }
        ActorBlock::Alt(_, blocks) => blocks.iter().any(is_action_block),
        ActorBlock::Optional(_, block) => is_action_block(block),
        ActorBlock::Repeat(_, block, _) => is_action_block(block),
        ActorBlock::ClientMessageValidate(..)
        | ActorBlock::AutoMessage(..)
        | ActorBlock::NoOp(..) => false,
    }
}

fn validate_non_action(block: &ActorBlock, context: Option<&str>) -> Result<()> {
    if is_action_block(block) {
        Err(ParseError::new_ctx(
            *block.ctx(),
            format!(
                "Cannot have a block that requires server initiative (ambiguous) {}",
                context.unwrap_or("here")
            ),
        ))
    } else {
        Ok(())
    }
}

fn validate_non_empty(block: &ActorBlock, context: Option<&str>) -> Result<()> {
    if matches!(block, ActorBlock::NoOp(_)) {
        Err(ParseError::new_ctx(
            *block.ctx(),
            format!("Cannot have an empty block {}", context.unwrap_or("here")),
        ))
    } else {
        Ok(())
    }
}

fn validate_list_children(blocks: &[ActorBlock]) -> Result<()> {
    let mut previous_has_deterministic_end = false;
    for block in blocks {
        if !previous_has_deterministic_end {
            validate_non_action(block, Some("after a block with non-deterministic end"))?;
        }
        previous_has_deterministic_end = has_deterministic_end(block);
    }
    Ok(())
}

fn validate_alt_child(b: &ActorBlock) -> Result<()> {
    validate_non_empty(b, Some("as an Alt block branch"))?;
    validate_non_action(b, Some("as an Alt block child"))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::bolt_version::BoltVersion;
    use crate::parser::{create_validator, ActorConfig};
    use crate::values::BoltMessage;
    use serde_json::Number;

    #[test]
    fn should_validate_any_hello() {
        let tag = "HELLO";
        let body = "{\"{}\": \"*\"}";
        let cfg = ActorConfig {
            bolt_version: BoltVersion::V5_0,
            allow_restart: false,
            allow_concurrent: false,
            handshake: None,
            handshake_delay: None,
        };
        let validator = create_validator(tag, Some(body), &cfg).unwrap();

        let cm = BoltMessage::new(0x00, vec![], BoltVersion::V5_0);

        todo!();
        // assert_!(validator.validate(&cm).unwrap());
    }

    #[test]
    fn json_test() {
        let mut v: Vec<StdResult<JsonValue, _>> = Deserializer::from_str("1").into_iter().collect();
        let JsonValue::Number(n) = v.pop().unwrap().unwrap() else {
            panic!("Expected number");
        };
        dbg!(&n);
        match n {
            Number { .. } => {}
        }
        dbg!(n.as_i64());
    }

    #[test]
    fn remove_optional_marker() {
        let key = "[jeff]";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff")
    }

    #[test]
    fn remove_ordered_marker() {
        let key = "jeff{}";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff")
    }

    #[test]
    fn remove_both_marker() {
        let key = "[jeff{}]";

        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff")
    }

    #[test]
    fn remove_optional_marker_with_escaped() {
        let key = r"[jeff\{\}]";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff{}")
    }

    #[test]
    fn handle_doubled_slash() {
        let key = r"jeff\{}";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff")
    }
}

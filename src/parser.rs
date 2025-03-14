use std::borrow::Cow;
use std::cell::LazyCell;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::result::Result as StdResult;
use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use itertools::Itertools;
use regex::Regex;
use serde_json::{Deserializer, Map as JsonMap, Value as JsonValue};

use crate::bang_line::BangLine;
use crate::bolt_version::{BoltVersion, JoltVersion};
use crate::context::Context;
use crate::parse_error::ParseError;
use crate::serde_json_ext;
use crate::str_byte;
use crate::types::actor_types::{
    ActorBlock, AutoMessageHandler, ClientMessageValidator, ScriptLine, ServerMessageSender,
};
use crate::types::{ScanBlock, Script};
use crate::util::opt_res_ret;
use crate::values::structs::{
    BoltDate, BoltDateTime, BoltDuration, BoltNode, BoltPath, BoltPoint, BoltRelationship,
    BoltTime, TAG_DATE, TAG_DURATION, TAG_LOCAL_TIME, TAG_POINT_2D, TAG_POINT_3D, TAG_TIME,
};
use crate::values::value::{Struct, Value};
use crate::values::BoltMessage;

#[derive(Debug)]
pub struct ActorScript {
    pub config: ActorConfig,
    pub tree: ActorBlock,
    pub script: Script,
}

#[derive(Debug)]
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
            excerpt_lines.push(format!("  {: >width$} ...", "", width = line_num_width));
        }
        for (mut line_num, line) in lines {
            line_num += 1;
            if line_num < start.saturating_sub(CONTEXT_LINES) {
                // pre context
                continue;
            }
            if line_num < start {
                // context before the excerpt
                excerpt_lines.push(format!(
                    "  {line_num: >width$} {line}",
                    width = line_num_width
                ));
                continue;
            }
            if line_num <= end {
                // the excerpt
                excerpt_lines.push(format!(
                    "> {line_num: >width$} {line}",
                    width = line_num_width
                ));
                continue;
            }
            // context after the excerpt
            excerpt_lines.push(format!(
                "  {line_num: >width$} {line}",
                width = line_num_width
            ));
            if line_num >= end.saturating_add(CONTEXT_LINES) {
                // past context
                break;
            }
        }
        if end.saturating_add(CONTEXT_LINES) < line_num_max {
            excerpt_lines.push(format!("  {: >width$} ...", "", width = line_num_width));
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
    let handshake_delay: Option<Duration> = None;

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
                todo!("Auto bang lines are not yet supported in the actor")
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

    let bolt_version = bolt_version.ok_or(ParseError::new("Bolt version bang line missing"))?;
    let allow_concurrent = allow_concurrent.unwrap_or(false);
    let allow_restart = if allow_concurrent {
        true // allow concurrent implies allow restart
    } else {
        allow_restart.unwrap_or(false)
    };
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
            let mut actor_blocks = condense_actor_blocks(
                scan_blocks
                    .iter()
                    .map(|b| parse_block(b, config))
                    .collect::<Result<_>>()?,
            );
            validate_list_children(&actor_blocks)?;

            match actor_blocks.len() {
                0 => Ok(ActorBlock::NoOp(*ctx)),
                1 => Ok(actor_blocks.remove(0)),
                _ => Ok(ActorBlock::BlockList(*ctx, actor_blocks)),
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
        ScanBlock::Parallel(ctx, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for block in scan_blocks {
                let b = parse_block(block, config)?;
                validate_parallel_child(&b)?;
                actor_blocks.push(b);
            }
            Ok(ActorBlock::Alt(*ctx, actor_blocks))
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
            let validator = create_validator(message_name, body_string.as_deref(), *ctx, config)
                .map_err(|mut e| {
                    e.ctx.get_or_insert(*ctx);
                    e
                })?;
            Ok(ActorBlock::ClientMessageValidate(*ctx, validator))
        }
        ScanBlock::ServerMessage(ctx, message_name, body_string) => {
            // TODO: Implement parser for the special messages like S: <Sleep 2>
            let server_message_sender =
                create_message_sender(message_name, body_string.as_deref(), *ctx, config).map_err(
                    |mut e| {
                        e.ctx.get_or_insert(*ctx);
                        e
                    },
                )?;
            Ok(ActorBlock::ServerMessageSend(*ctx, server_message_sender))
        }
        ScanBlock::AutoMessage(ctx, client_message_name, client_body_string) => {
            Ok(ActorBlock::AutoMessage(
                *ctx,
                create_auto_message_handler(
                    client_message_name,
                    client_body_string.as_deref(),
                    *ctx,
                    config,
                )?,
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

fn condense_actor_blocks(blocks: Vec<ActorBlock>) -> Vec<ActorBlock> {
    let mut res = Vec::with_capacity(blocks.len());
    for block in blocks.into_iter() {
        let last = res.last_mut();
        match (block, last) {
            (ActorBlock::NoOp(_), _) => continue,
            (
                ActorBlock::BlockList(ctx, list),
                Some(ActorBlock::BlockList(ctx_last, list_last)),
            ) => {
                *ctx_last = ctx_last.fuse(&ctx);
                list_last.extend(list)
            }
            (block, _) => res.push(block),
        }
    }
    res
}

fn create_auto_message_handler(
    client_message_name: &str,
    client_body_string: Option<&str>,
    ctx: Context,
    config: &ActorConfig,
) -> Result<AutoMessageHandler> {
    let client_validator = create_validator(client_message_name, client_body_string, ctx, config)?;
    let client_message_tag = config
        .bolt_version
        .message_tag_from_request(client_message_name)
        .expect("checked in create_validator");
    let (server_message_tag, server_message_fields) = config
        .bolt_version
        .message_auto_response(client_message_tag)
        .ok_or_else(|| {
            ParseError::new(format!(
                "Bolt version {} has not auto-response for message {client_message_name}",
                config.bolt_version,
            ))
        })?;
    let data = message_data(server_message_tag, server_message_fields);
    let server_sender = Box::new(SenderBytes::new(
        data,
        "AUTO".into(), // TODO: get the actual name of the server message
        None,          // TODO: get the body string representation of the server message
        ctx,
    ));
    Ok(AutoMessageHandler {
        client_validator,
        server_sender,
    })
}

fn create_message_sender(
    message_name: &str,
    message_body: Option<&str>,
    ctx: Context,
    config: &ActorConfig,
) -> Result<Box<dyn ServerMessageSender>> {
    let tag = config
        .bolt_version
        .message_tag_from_response(message_name)
        .ok_or_else(|| {
            ParseError::new(format!(
                "Unknown message name {message_name:?} for BOLT version {}",
                config.bolt_version
            ))
        })?;
    let data = message_data(tag, transcode_body(message_body, config)?);

    Ok(Box::new(SenderBytes::new(
        data,
        message_name.into(),
        message_body.map(Into::into),
        ctx,
    )))
}

fn message_data(tag: u8, fields: Vec<Value>) -> Vec<u8> {
    Value::Struct(Struct { tag, fields }).to_data()
}

struct SenderBytes {
    pub data: Vec<u8>,
    pub message_name: String,
    pub message_body: Option<String>,
    pub ctx: Context,
}

impl SenderBytes {
    pub fn new(
        data: Vec<u8>,
        message_name: Cow<str>,
        message_body: Option<Cow<str>>,
        ctx: Context,
    ) -> Self {
        Self {
            data,
            message_name: message_name.into_owned(),
            message_body: message_body.map(Cow::into_owned),
            ctx,
        }
    }
}

impl ScriptLine for SenderBytes {
    fn original_line<'a>(&self, script: &'a str) -> &'a str {
        self.ctx.original_line(script)
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

fn transcode_body(msg: Option<&str>, config: &ActorConfig) -> Result<Vec<Value>> {
    let Some(msg) = msg else {
        return Ok(vec![]);
    };
    Deserializer::from_str(msg)
        .into_iter()
        .collect::<StdResult<Vec<JsonValue>, _>>()?
        .into_iter()
        .map(|field| transcode_field(field, config))
        .collect()
}

pub(crate) fn transcode_field(field: JsonValue, config: &ActorConfig) -> Result<Value> {
    Ok(match field {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(value),
        JsonValue::Number(value) if value.is_i64() => Value::Integer(value.as_i64().unwrap()),
        JsonValue::Number(value) if value.is_f64() => Value::Float(value.as_f64().unwrap()),
        JsonValue::Number(value) => {
            return Err(ParseError::new(format!(
                "Can't parse number (must be i64 or f64) {value:?}",
            )));
        }
        JsonValue::String(value) => Value::String(value),
        JsonValue::Array(value) => Value::List(
            value
                .into_iter()
                .map(|v| transcode_field(v, config))
                .collect::<Result<Vec<_>>>()?,
        ),
        JsonValue::Object(value) => {
            let value = match transcode_jolt_value(value, config)? {
                IsJoltValue::Yes(jolt_value) => return Ok(jolt_value),
                IsJoltValue::No(value) => value,
            };

            Value::Map(
                value
                    .into_iter()
                    .map(|(k, v)| transcode_field(v, config).map(|v| (k, v)))
                    .collect::<Result<_>>()?,
            )
        }
    })
}

enum IsJoltValue {
    Yes(Value),
    No(JsonMap<String, JsonValue>),
}

fn transcode_jolt_value(
    value: JsonMap<String, JsonValue>,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    if value.len() != 1 {
        return Ok(IsJoltValue::No(value));
    }
    let (sigil, value) = value.into_iter().next().expect("non-empty check above");
    let (versionless_sigil, jolt_version) = parse_jolt_sigil(&sigil, config)?;
    Ok(match versionless_sigil {
        "?" => {
            if !value.is_boolean() {
                return Err(ParseError::new(format!(
                    "Expected bool after sigil \"?\", but found {value:?}",
                )));
            }
            IsJoltValue::Yes(transcode_field(value, config)?)
        }
        "Z" => {
            let JsonValue::String(value) = value else {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"Z\", but found {value:?}",
                )));
            };
            let value = i64::from_str(&value).map_err(|e| {
                ParseError::new(format!(
                    "Failed to parse i64 after sigil \"Z\": {value:?} because {e}",
                ))
            })?;
            IsJoltValue::Yes(transcode_field(JsonValue::Number(value.into()), config)?)
        }
        "R" => {
            let JsonValue::String(value) = value else {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"R\", but found {value:?}",
                )));
            };
            let value = f64::from_str(&value).map_err(|e| {
                ParseError::new(format!(
                    "Failed to parse f64 after sigil \"R\": {value:?} because {e}",
                ))
            })?;
            IsJoltValue::Yes(Value::Float(value))
        }
        "U" => {
            if !value.is_string() {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"U\", but found {value:?}",
                )));
            }
            IsJoltValue::Yes(transcode_field(value, config)?)
        }
        "#" => {
            let bytes = parse_jolt_bytes(value)?;
            IsJoltValue::Yes(Value::Bytes(bytes))
        }
        "[]" => {
            if !value.is_array() {
                return Err(ParseError::new(format!(
                    "Expected array after sigil \"[]\", but found {value:?}",
                )));
            }
            IsJoltValue::Yes(transcode_field(value, config)?)
        }
        "{}" => {
            if !value.is_object() {
                return Err(ParseError::new(format!(
                    "Expected object after sigil \"{{}}\", but found {value:?}",
                )));
            };
            IsJoltValue::Yes(transcode_field(value, config)?)
        }
        "T" => {
            let JsonValue::String(value) = value else {
                return Err(ParseError::new(format!(
                    "Expected temporal string after sigil \"T\", but found {value:?}",
                )));
            };
            let value = transcode_date_value(&value)
                .or_else(|| transcode_time_value(&value))
                .or_else(|| transcode_date_time_value(&value, jolt_version))
                .or_else(|| transcode_duration_value(&value))
                .transpose()?
                .ok_or_else(|| {
                    ParseError::new(format!(
                        "Expected temporal string after sigil \"T\", but found {value:?}",
                    ))
                })?;
            IsJoltValue::Yes(value)
        }
        "@" => {
            let JsonValue::String(value) = value else {
                return Err(ParseError::new(format!(
                    "Expected spatial string after sigil \"@\", but found {value:?}",
                )));
            };
            let bolt_point = BoltPoint::parse(&value)?;
            IsJoltValue::Yes(Value::Struct(bolt_point.as_struct()))
        }
        "()" => {
            let bolt_node = BoltNode::parse(value, jolt_version, config)?;
            IsJoltValue::Yes(Value::Struct(bolt_node.into_struct()))
        }
        "->" => {
            let bolt_relationship = BoltRelationship::parse(value, jolt_version, config)?;
            IsJoltValue::Yes(Value::Struct(bolt_relationship.into_struct()))
        }
        "<-" => {
            let mut bolt_relationship = BoltRelationship::parse(value, jolt_version, config)?;
            bolt_relationship.flip_direction();
            IsJoltValue::Yes(Value::Struct(bolt_relationship.into_struct()))
        }
        ".." => {
            let bolt_path = BoltPath::parse(value, jolt_version, config)?;
            IsJoltValue::Yes(Value::Struct(bolt_path.into_struct()))
        }
        _ => IsJoltValue::No([(sigil, value)].into_iter().collect()),
    })
}

fn transcode_date_value(s: &str) -> Option<Result<Value>> {
    let bolt_date = opt_res_ret!(BoltDate::parse(s));
    let value_struct = opt_res_ret!(bolt_date.as_struct());
    Some(Ok(Value::Struct(value_struct)))
}

fn transcode_time_value(s: &str) -> Option<Result<Value>> {
    let bolt_time = opt_res_ret!(BoltTime::parse(s));
    let value_struct = opt_res_ret!(bolt_time.as_struct());
    Some(Ok(Value::Struct(value_struct)))
}

fn transcode_date_time_value(s: &str, jolt_version: JoltVersion) -> Option<Result<Value>> {
    let bolt_date_time = opt_res_ret!(BoltDateTime::parse(s));
    let value_struct = opt_res_ret!(bolt_date_time.as_struct(jolt_version));
    Some(Ok(Value::Struct(value_struct)))
}

fn transcode_duration_value(s: &str) -> Option<Result<Value>> {
    let bolt_duration = opt_res_ret!(BoltDuration::parse(s));
    let value_struct = opt_res_ret!(bolt_duration.as_struct());
    Some(Ok(Value::Struct(value_struct)))
}

struct ValidatorImpl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> {
    pub func: T,
    pub ctx: Context,
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> Debug for ValidatorImpl<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidatorImpl")
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> ScriptLine for ValidatorImpl<T> {
    fn original_line<'a>(&self, script: &'a str) -> &'a str {
        self.ctx.original_line(script)
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
    message_name: &str,
    expected_body_string: Option<&str>,
    ctx: Context,
    config: &ActorConfig,
) -> Result<Box<dyn ClientMessageValidator>> {
    let expected_tag = config
        .bolt_version
        .message_tag_from_request(message_name)
        .ok_or_else(|| {
            ParseError::new(format!(
                "Unknown message name {message_name:?} for BOLT version {}",
                config.bolt_version
            ))
        })?;
    let expected_body_behavior = build_fields_validator(expected_body_string, config)?;
    Ok(Box::new(ValidatorImpl {
        func: move |client_message| {
            if client_message.tag != expected_tag {
                return Err(anyhow!("the tags did not match."));
            }
            expected_body_behavior(&client_message.fields)
        },
        ctx,
    }))
}

fn build_no_fields_validator(message: &[Value]) -> anyhow::Result<()> {
    match message.len() {
        0 => Ok(()),
        _ => Err(anyhow!("Expected 0 fields")),
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
                "Can't parse number (must be i64 or f64) {expected:?}",
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
            let expected = match build_jolt_validator(expected, config)? {
                IsJoltValidator::Yes(jolt_validator) => return Ok(jolt_validator),
                IsJoltValidator::No(expected) => expected,
            };

            build_map_validator(expected, config)?
            // try to build jolt matcher (incl. Structs like datetime)
            //   * if has 1 key-value pair
            //   * && key is known sigil
            //   * Special string "*" applies: E.g., `C: RUN {"Z": "*"}` will match any integer: `C: RUN 1` and `C: RUN 2`, but not `C: RUN 1.2` or `C: RUN "*"`.
        }
    })
}

fn build_map_validator(
    expected: JsonMap<String, JsonValue>,
    config: &ActorConfig,
) -> Result<ValidateValueFn> {
    let mut required_keys = HashSet::new();
    let mut unique_keys = HashSet::new();
    let validators = expected
        .into_iter()
        .map(|(key, expect_for_key)| {
            let (key, validator, req) = build_map_entry_validator(&key, expect_for_key, config)?;
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

    Ok(Box::new(move |msg| {
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
    }))
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
                left_validators.extend(0..validators.len());
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
    No(JsonMap<String, JsonValue>),
}

fn build_jolt_validator(
    expected: JsonMap<String, JsonValue>,
    config: &ActorConfig,
) -> Result<IsJoltValidator> {
    fn is_match_all(value: &JsonValue) -> bool {
        value.as_str().map(|s| s == "*").unwrap_or_default()
    }

    macro_rules! match_any {
        ($typ:expr, $pattern:pat) => {
            return Ok(IsJoltValidator::Yes(Box::new(|msg| match msg {
                $pattern => Ok(()),
                _ => Err(anyhow!("Expected any {:} found {:?}", $typ, msg)),
            })))
        };
    }

    // https://docs.google.com/document/d/1QK4OcC0tZ08lKqVr-3-z8HpPY9jFeEh6zZPhMm15D-w/edit?tab=t.0
    if expected.len() != 1 {
        return Ok(IsJoltValidator::No(expected));
    }
    let (sigil, expected) = expected.into_iter().next().expect("non-empty check above");
    let (versionless_sigil, jolt_version) = parse_jolt_sigil(&sigil, config)?;
    Ok(match versionless_sigil {
        "?" => {
            if is_match_all(&expected) {
                match_any!("bool", Value::Boolean(_));
            }
            if !expected.is_boolean() {
                return Err(ParseError::new(format!(
                    "Expected bool after sigil \"?\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        "Z" => {
            if is_match_all(&expected) {
                match_any!("integer", Value::Integer(_));
            }
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
            if is_match_all(&expected) {
                match_any!("float", Value::Float(_));
            }
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
            if is_match_all(&expected) {
                match_any!("string", Value::String(_));
            }
            if !expected.is_string() {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"U\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        "#" => {
            if is_match_all(&expected) {
                match_any!("bytes", Value::Bytes(_));
            }
            let bytes = parse_jolt_bytes(expected)?;
            IsJoltValidator::Yes(Box::new(move |msg| match msg {
                Value::Bytes(received) if &bytes == received => Ok(()),
                _ => Err(anyhow!("Expected bytes {:?} found {:?}", &bytes, msg)),
            }))
        }
        "[]" => {
            if is_match_all(&expected) {
                match_any!("list", Value::List(_));
            }
            if !expected.is_array() {
                return Err(ParseError::new(format!(
                    "Expected array after sigil \"[]\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        "{}" => {
            if is_match_all(&expected) {
                match_any!("map", Value::Map(_));
            }
            let JsonValue::Object(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected object after sigil \"{{}}\", but found {expected:?}",
                )));
            };
            IsJoltValidator::Yes(build_map_validator(expected, config)?)
        }
        "T" => {
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected temporal string after sigil \"T\", but found {expected:?}",
                )));
            };
            let validator = build_date_validator(&expected)
                .or_else(|| build_time_validator(&expected))
                .or_else(|| build_date_time_validator(&expected, jolt_version))
                .or_else(|| build_duration_validator(&expected))
                .transpose()?
                .ok_or_else(|| {
                    ParseError::new(format!(
                        "Expected temporal string after sigil \"T\", but found {expected:?}",
                    ))
                })?;
            IsJoltValidator::Yes(validator)
        }
        "@" => {
            if is_match_all(&expected) {
                return Ok(IsJoltValidator::Yes(Box::new(|msg| match msg {
                    Value::Struct(Struct { tag, fields }) => match (tag, fields.as_slice()) {
                        (&TAG_POINT_2D, [Value::Integer(_), Value::Float(_), Value::Float(_)]) => {
                            Ok(())
                        }
                        (
                            &TAG_POINT_3D,
                            [Value::Integer(_), Value::Float(_), Value::Float(_), Value::Float(_)],
                        ) => Ok(()),
                        _ => Err(anyhow!("Expected any date struct found {msg:?}")),
                    },
                    _ => Err(anyhow!("Expected any spatial struct found {msg:?}")),
                })));
            }
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected spatial string after sigil \"@\", but found {expected:?}",
                )));
            };
            let bolt_point = BoltPoint::parse(&expected)?;
            let expected = Value::Struct(bolt_point.as_struct());
            IsJoltValidator::Yes(Box::new(move |msg| match msg == &expected {
                true => Ok(()),
                false => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
            }))
        }
        "()" => {
            let fixed_syntax = format!(
                r#"{{"\{sigil}": {}}}"#,
                serde_json_ext::compact_pretty_print(&expected)
                    .expect("JsonValue cannot fail Json serialization")
            );
            return Err(ParseError::new(format!(
                "Node structs cannot be received by the server, only sent. \
                If you meant to match a map, use `{fixed_syntax}` instead.",
            )));
        }
        "->" | "<-" => {
            let fixed_syntax = format!(
                r#"{{"\{sigil}": {}}}"#,
                serde_json_ext::compact_pretty_print(&expected)
                    .expect("JsonValue cannot fail Json serialization")
            );
            return Err(ParseError::new(format!(
                "Relationship structs cannot be received by the server, only sent. \
                If you meant to match a map, use `{fixed_syntax}` instead.",
            )));
        }
        ".." => {
            let fixed_syntax = format!(
                r#"{{"\{sigil}": {}}}"#,
                serde_json_ext::compact_pretty_print(&expected)
                    .expect("JsonValue cannot fail Json serialization")
            );
            return Err(ParseError::new(format!(
                "Path structs cannot be received by the server, only sent. \
                If you meant to match a map, use `{fixed_syntax}` instead.",
            )));
        }
        _ => IsJoltValidator::No([(sigil, expected)].into_iter().collect()),
    })
}

fn parse_jolt_sigil<'a>(sigil: &'a str, config: &ActorConfig) -> Result<(&'a str, JoltVersion)> {
    thread_local! {
        static SIGIL_VERSION_RE: LazyCell<Regex> = LazyCell::new(|| {
            Regex::new(r"^(.+?)(?:v(\d+))?$").unwrap()
        });
    }
    SIGIL_VERSION_RE.with(|re| {
        let Some(captures) = re.captures(sigil) else {
            return Ok((sigil, config.bolt_version.jolt_version()));
        };
        let Some(version_overwrite) = captures.get(2) else {
            return Ok((sigil, config.bolt_version.jolt_version()));
        };
        let version_overwrite = version_overwrite.as_str();
        let jolt_version = JoltVersion::parse(version_overwrite)?;
        Ok((
            captures
                .get(1)
                .expect("regex enforces one group to exist")
                .as_str(),
            jolt_version,
        ))
    })
}

fn parse_jolt_bytes(expected: JsonValue) -> Result<Vec<u8>> {
    Ok(match expected {
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
    })
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

/// yes:
/// 2020-01-01
/// 2020-01
/// 2020
///
/// no:
/// 2020-1-1
/// --1
fn build_date_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            Value::Struct(Struct { tag, fields }) => match (tag, fields.as_slice()) {
                (&TAG_DATE, [Value::Integer(_)]) => Ok(()),
                _ => Err(anyhow!("Expected any date struct found {msg:?}")),
            },
            _ => Err(anyhow!("Expected any date struct found {msg:?}")),
        })));
    }
    let bolt_date = opt_res_ret!(BoltDate::parse(s));
    let expected_struct = opt_res_ret!(bolt_date.as_struct());
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// 12:00:00.000000000+0000
/// 12:00:00.000+0000
/// 12:00:00+00:00
/// 12:00:00+00
/// 12:00:00Z
/// 12:00Z
/// 12Z
/// 12:00:00-01
///
/// no:
/// 12:00:00.0000000000Z
/// 12:00:00-0000
/// 12:0:0Z
/// 12:00:00+01:02:03
/// 12:00:00+00:00\[Europe/Berlin]
fn build_time_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            Value::Struct(Struct { tag, fields }) => match (tag, fields.as_slice()) {
                (&TAG_LOCAL_TIME, [Value::Integer(_)]) => Ok(()),
                (&TAG_TIME, [Value::Integer(_), Value::Integer(_)]) => Ok(()),
                _ => Err(anyhow!("Expected any time struct found {msg:?}")),
            },
            _ => Err(anyhow!("Expected any time struct found {msg:?}")),
        })));
    }
    let bolt_time = opt_res_ret!(BoltTime::parse(s));
    let expected_struct = opt_res_ret!(bolt_time.as_struct());
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// `<date_re>T<time_re><timezone_name>`
/// where `<date_re>` is anything that works for [`build_date_validator`], `<time_re>`
/// is anything that works for [`build_time_validator`] and `<timezone_name>` (optional) is any
/// timezone name in square brackets, e.g., `[Europe/Stockholm]`. `<timezone_name>` may only be
/// present, when `<time_re>` has a time offset.
///
/// no:
/// anything else
fn build_date_time_validator(
    s: &str,
    jolt_version: JoltVersion,
) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(move |msg| match msg {
            Value::Struct(Struct { tag, fields }) => match (jolt_version, tag, fields.as_slice()) {
                (
                    JoltVersion::V1,
                    0x66,
                    [Value::Integer(_), Value::Integer(_), Value::String(_)],
                ) => Ok(()),
                (
                    JoltVersion::V2,
                    0x69,
                    [Value::Integer(_), Value::Integer(_), Value::String(_)],
                ) => Ok(()),
                (
                    JoltVersion::V1,
                    0x46,
                    [Value::Integer(_), Value::Integer(_), Value::Integer(_)],
                ) => Ok(()),
                (
                    JoltVersion::V2,
                    0x49,
                    [Value::Integer(_), Value::Integer(_), Value::Integer(_)],
                ) => Ok(()),
                (_, 0x64, [Value::Integer(_), Value::Integer(_)]) => Ok(()),
                _ => Err(anyhow!("Expected any date time struct found {msg:?}")),
            },
            _ => Err(anyhow!("Expected any date time struct found {msg:?}")),
        })));
    }
    let bolt_date_time = opt_res_ret!(BoltDateTime::parse(s));
    let expected_struct = opt_res_ret!(bolt_date_time.as_struct(jolt_version));
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// P12Y13M40DT10H70M80.000000000S
/// P12Y-13M40DT-10H70M80.000000000S
/// P12Y
/// PT70M
/// P12T10H70M
///
/// no:
/// P12Y13M40DT10H70M80.0000000000S
/// P12Y13M40DT10H70.1M10S
/// P5W
fn build_duration_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            Value::Struct(Struct { tag, fields }) => match (tag, fields.as_slice()) {
                (
                    &TAG_DURATION,
                    [Value::Integer(_), Value::Integer(_), Value::Integer(_), Value::Integer(_)],
                ) => Ok(()),
                _ => Err(anyhow!("Expected any duration struct found {msg:?}")),
            },
            _ => Err(anyhow!("Expected any duration struct found {msg:?}")),
        })));
    }
    let BoltDuration {
        months,
        days,
        seconds,
        nanos,
    } = opt_res_ret!(BoltDuration::parse(s));
    let total_nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    Some(Ok(Box::new(move |msg| match msg {
        Value::Struct(received) => {
            if received.tag != TAG_DURATION {
                return Err(anyhow!(
                    "Expected duration (tag {TAG_DURATION:#X}), found {msg:?}",
                ));
            }
            fn get_int_field(i: usize, name: &str, received: &Struct) -> anyhow::Result<i64> {
                match received.fields.get(i) {
                    None => Err(anyhow!(
                        "Received invalid duration: {:?} (missing {name})",
                        received
                    )),
                    Some(Value::Integer(value)) => Ok(*value),
                    Some(_) => Err(anyhow!(
                        "Received invalid duration: {:?} ({name} not integer)",
                        received
                    )),
                }
            }
            let received_months = get_int_field(0, "months", received)?;
            let received_days = get_int_field(1, "days", received)?;
            let received_seconds = get_int_field(2, "seconds", received)?;
            let received_nanos = get_int_field(3, "nanoseconds", received)?;
            let received_total_nanos =
                i128::from(received_seconds) * 1_000_000_000 + i128::from(received_nanos);
            if received_months != months {
                return Err(anyhow!(
                    "Expected duration months: {months}, found {received_months} in {received:?}",
                ));
            }
            if received_days != days {
                return Err(anyhow!(
                    "Expected duration days: {days}, found {received_days} in {received:?}",
                ));
            }
            if received_total_nanos != total_nanos {
                return Err(anyhow!(
                    "Expected duration with total nanoseconds: {total_nanos}, \
                    found {received_total_nanos} in {received:?}",
                ));
            }
            Ok(())
        }
        _ => Err(anyhow!("Expected duration, found {:?}", msg)),
    })))
}

fn build_struct_match_validator(expected: Struct) -> ValidateValueFn {
    let expected = Value::Struct(expected);
    Box::new(move |msg| match msg == &expected {
        true => Ok(()),
        false => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
    })
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
                Regex::new(r"\\([\{\}\[\]\\])").unwrap()
            });
        }
        let unescaped = UNESCAPE_RE.with(|re| re.replace_all(key, r"$1"));
        let mut unescaped_ref = unescaped.as_ref();

        thread_local! {
            static FLAGS_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(\[)?(?:\\[\\\{}\}\[\]]|[^\\])*?(\{\})?(\])?$").unwrap()
            });
        }
        let flags = FLAGS_RE.with(|re| re.captures(key).expect("regex matches anything"));
        let is_optional = flags.get(1).is_some() && flags.get(3).is_some();
        let is_ordered = flags.get(2).is_some();

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
        return Ok(Box::new(build_no_fields_validator));
    };
    // RUN "RETURN $n AS n" {"n": 1}
    let field_validators: Vec<_> = Deserializer::from_str(line)
        .into_iter()
        .collect::<StdResult<Vec<JsonValue>, _>>()?
        .into_iter()
        .map(|field| build_field_validator(field, config))
        .collect::<Result<_>>()?;

    Ok(Box::new(move |fields| {
        if fields.len() != field_validators.len() {
            return Err(anyhow!(
                "Expected {} fields, but found {}",
                fields.len(),
                field_validators.len()
            ));
        }
        for (field, field_validator) in fields.iter().zip(&field_validators) {
            field_validator(field)?;
        }
        Ok(())
    }))
}

fn is_skippable(block: &ActorBlock) -> bool {
    match block {
        ActorBlock::BlockList(_, blocks)
        | ActorBlock::Alt(_, blocks)
        | ActorBlock::Parallel(_, blocks) => blocks.iter().all(is_skippable),
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
        ActorBlock::Alt(_, blocks) | ActorBlock::Parallel(_, blocks) => {
            blocks.iter().all(has_deterministic_end)
        }
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
        ActorBlock::Alt(_, blocks) | ActorBlock::Parallel(_, blocks) => {
            blocks.iter().any(is_action_block)
        }
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
    validate_non_action(b, Some("as an Alt block branch"))?;
    Ok(())
}

fn validate_parallel_child(b: &ActorBlock) -> Result<()> {
    validate_non_empty(b, Some("as an Parallel block branch"))?;
    validate_non_action(b, Some("as an Parallel block branch"))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use indexmap::indexmap;
    use serde_json::Number;

    #[test]
    fn should_validate_any_hello() {
        let tag = "HELLO";
        let body = r#"{"{}": "*"}"#;
        let ctx = Context {
            start_line_number: 0,
            end_line_number: 0,
        };
        let cfg = ActorConfig {
            bolt_version: BoltVersion::V5_0,
            allow_restart: false,
            allow_concurrent: false,
            handshake: None,
            handshake_delay: None,
        };
        let validator = create_validator(tag, Some(body), ctx, &cfg).unwrap();

        let cm = BoltMessage::new(
            0x01,
            vec![Value::Map(
                indexmap! {String::from("foo") => Value::String(String::from("bar"))},
            )],
            BoltVersion::V5_0,
        );

        validator.validate(&cm).unwrap()
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
        assert_eq!(escaped.unescaped, "jeff{}")
    }
}

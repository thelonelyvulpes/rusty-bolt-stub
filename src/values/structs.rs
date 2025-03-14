use std::cell::LazyCell;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;
use std::rc::Rc;
use std::str::FromStr;

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike};
use indexmap::IndexMap;
use itertools::Itertools;
use regex::{Captures, Regex};
use serde_json::Value as JsonValue;

use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::parser::{transcode_field, ActorConfig};
use crate::util::opt_res_ret;
use crate::values::bolt_value::{PackStreamValue, Struct};

pub(crate) const TAG_POINT_2D: u8 = 0x58;
pub(crate) const TAG_POINT_3D: u8 = 0x59;
pub(crate) const TAG_DATE: u8 = 0x44;
pub(crate) const TAG_LOCAL_TIME: u8 = 0x74;
pub(crate) const TAG_TIME: u8 = 0x54;
pub(crate) const TAG_DURATION: u8 = 0x45;
pub(crate) const TAG_NODE: u8 = 0x4E;
pub(crate) const TAG_RELATIONSHIP: u8 = 0x52;
pub(crate) const TAG_UNBOUND_RELATIONSHIP: u8 = 0x72;
pub(crate) const TAG_PATH: u8 = 0x50;

#[derive(Debug, Copy, Clone)]
pub(crate) struct BoltPoint {
    pub(crate) srid: i64,
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) z: Option<f64>,
}

impl BoltPoint {
    pub(crate) fn new(srid: i64, x: f64, y: f64, z: Option<f64>) -> Self {
        Self { srid, x, y, z }
    }

    pub(crate) fn parse(s: &str) -> Result<Self, ParseError> {
        thread_local! {
            static SPATIAL_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^(?:SRID=([^;]+);)?\s*",
                    r"POINT\s*\(((?:(?:\S+)?\s+){1,2}\S+)\)$",
                )).unwrap()
            });
        }

        let captures = SPATIAL_RE.with(|re| re.captures(s));
        let Some(captures) = captures else {
            return Err(ParseError::new(format!(
                "Expected valid spatial string after sigil \"@\", \
                    e.g., \"SRID=7203;POINT(1 2)\" found: {s:?}"
            )));
        };
        let Some(srid_match) = captures.get(1) else {
            return Err(ParseError::new(format!(
                "Spatial string (after sigil \"@\") requires an SRID, \
                    e.g., \"SRID=7203;POINT(1 2)\" found: {s:?}"
            )));
        };
        let srid = srid_match.as_str();
        let srid = i64::from_str(srid).map_err(|e| {
            format!("Spatial string (after sigil \"@\") contained non-i64 srid {srid:?}): {e}")
        })?;
        let coords = dbg!(&captures[2]);
        let coords = coords
            .split_whitespace()
            .enumerate()
            .map(|(i, c)| {
                f64::from_str(c).map_err(|e| {
                    format!(
                        "Spatial string (after sigil \"@\") contained non-f64 coordinate \
                            {c:?} (at {i}): {e}"
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(match coords.as_slice() {
            [x, y] => BoltPoint::new(srid, *x, *y, None),
            [x, y, z] => BoltPoint::new(srid, *x, *y, Some(*z)),
            _ => panic!("Regex asserts exactly 2 or 3 coordinates"),
        })
    }

    pub(crate) fn as_struct(&self) -> Struct {
        let mut fields = Vec::with_capacity(4);
        fields.extend([
            PackStreamValue::Integer(self.srid),
            PackStreamValue::Float(self.x),
            PackStreamValue::Float(self.y),
        ]);
        match self.z {
            Some(z) => fields.push(PackStreamValue::Float(z)),
            None => fields.push(PackStreamValue::Null),
        }
        let tag = match self.z {
            None => TAG_POINT_2D,
            Some(_) => TAG_POINT_3D,
        };
        Struct { tag, fields }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct BoltDate {
    pub(crate) date: NaiveDate,
}

impl BoltDate {
    pub(crate) fn parse(s: &str) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static DATE_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?$").unwrap()
            });
        }
        let captures = DATE_RE.with(|re| re.captures(s))?;
        let year = i32::from_str(
            captures
                .get(1)
                .expect("regex enforces existence of year")
                .as_str(),
        )
        .expect("regex enforces year to be i32");
        let month = captures
            .get(2)
            .map(|m| u32::from_str(m.as_str()).expect("regex enforces month to be u32"))
            .unwrap_or(1);
        let day = captures
            .get(3)
            .map(|d| u32::from_str(d.as_str()).expect("regex enforces day to be u32"))
            .unwrap_or(1);
        let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
            return Some(Err(ParseError::new(format!("Unparsable date {s:?}"))));
        };
        Some(Ok(Self { date }))
    }

    pub(crate) fn as_struct(&self) -> Option<Result<Struct, ParseError>> {
        let days_since_epoch = self.date - NaiveDate::from_ymd_opt(0, 1, 1).unwrap();
        Some(Ok(Struct {
            tag: TAG_DATE,
            fields: vec![PackStreamValue::Integer(days_since_epoch.num_days())],
        }))
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct BoltTime<'a> {
    pub(crate) time: NaiveTime,
    pub(crate) utc_offset_seconds: Option<i32>,
    pub(crate) time_zone_id: Option<&'a str>,
}

impl<'a> BoltTime<'a> {
    pub(crate) fn parse(s: &'a str) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static TIME_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^(\d{2}):(\d{2})(?::(\d{2}))?(?:\.(\d{1,9}))?",
                    r"(?:(Z)|(?:(\+00(?::?00)?|[+-]00:?(?:[0-5][1-9]|[1-5]\d)|",
                    r"(?:[+-](?:0[1-9]|1\d|2[0-3]):?[0-5]\d)))(?:\[([^\]]+)\])?)?$"
                )).unwrap()
            });
        }
        let captures = TIME_RE.with(|re| re.captures(s))?;
        let hours = u32::from_str(
            captures
                .get(1)
                .expect("regex enforces existence of hours")
                .as_str(),
        )
        .expect("regex enforces hours to be u32");
        let minutes = captures
            .get(2)
            .map(|m| u32::from_str(m.as_str()).expect("regex enforces minutes to be u32"))
            .unwrap_or_default();
        let seconds = captures
            .get(3)
            .map(|m| u32::from_str(m.as_str()).expect("regex enforces seconds to be u32"))
            .unwrap_or_default();
        if seconds >= 60 {
            return Some(Err(ParseError::new(format!(
                "Unparsable time: {s:?} (leap seconds are not supported)"
            ))));
        }
        let nanos = captures
            .get(4)
            .map(|m| {
                // left align to fill in omitted decimal places
                u32::from_str(&format!("{:<09}", m.as_str()))
                    .expect("regex enforces nanos to be u32")
            })
            .unwrap_or_default();
        let utc_offset_seconds = match captures.get(5) {
            None => captures.get(6).map(|m| {
                // ±XY[[:]ZT]
                let s = m.as_str();
                let sign = match &s[..1] {
                    "+" => 1,
                    "-" => -1,
                    _ => unreachable!("regex enforces offset sign to be + or -"),
                };
                let hours =
                    i32::from_str(&s[1..=2]).expect("regex enforces offset hours to be i32");
                let minutes = match s.len() {
                    3 => 0,
                    5 => i32::from_str(&s[3..=4]).expect("regex enforces offset minutes to be i32"),
                    6 => i32::from_str(&s[4..=5]).expect("regex enforces offset minutes to be i32"),
                    _ => unreachable!(
                        "regex enforces offset to have length 3, 5, or six, found {s:?}"
                    ),
                };
                sign * (hours * 60 + minutes) * 60
            }),
            Some(_) => Some(0), // Z
        };
        let time_zone_id = captures.get(7).map(|m| m.as_str());
        let Some(time) = NaiveTime::from_hms_nano_opt(hours, minutes, seconds, nanos) else {
            return Some(Err(ParseError::new(format!("Unparsable time: {s:?}"))));
        };
        Some(Ok(Self {
            time,
            utc_offset_seconds,
            time_zone_id,
        }))
    }

    pub(crate) fn as_struct(&self) -> Option<Result<Struct, ParseError>> {
        if self.time_zone_id.is_some() {
            // times only accept UTC offsets, not time zone ids
            return None;
        }
        let nanos_since_midnight = i64::from(self.time.num_seconds_from_midnight()) * 1_000_000_000
            + i64::from(self.time.nanosecond());
        Some(Ok(match self.utc_offset_seconds {
            // local time
            None => Struct {
                tag: TAG_LOCAL_TIME,
                fields: vec![PackStreamValue::Integer(nanos_since_midnight)],
            },
            // time
            Some(offset) => Struct {
                tag: TAG_TIME,
                fields: vec![
                    PackStreamValue::Integer(nanos_since_midnight),
                    PackStreamValue::Integer(offset.into()),
                ],
            },
        }))
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct BoltDateTime<'a> {
    pub(crate) date: BoltDate,
    pub(crate) time: BoltTime<'a>,
}
impl<'a> BoltDateTime<'a> {
    pub(crate) fn parse(s: &'a str) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static DATE_TIME_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(.*?)T(.*)$").unwrap()
            });
        }
        let captures = DATE_TIME_RE.with(|re| re.captures(s))?;
        let date = opt_res_ret!(BoltDate::parse(&captures[1]));
        let time = opt_res_ret!(BoltTime::parse(captures.get(2).unwrap().as_str()));
        Some(Ok(Self { date, time }))
    }

    pub(crate) fn as_struct(
        &self,
        jolt_version: JoltVersion,
    ) -> Option<Result<Struct, ParseError>> {
        let BoltDate { date } = self.date;
        let BoltTime {
            time,
            utc_offset_seconds,
            time_zone_id,
        } = self.time;
        let date_time = NaiveDateTime::new(date, time);
        Some(Ok(match (utc_offset_seconds, time_zone_id) {
            (Some(utc_offset_seconds), Some(time_zone_id)) => {
                // date time zone id

                // let tz = match Tz::from_str(time_zone_id) {
                //     Ok(tz) => tz,
                //     Err(e) => {
                //         return Some(Err(ParseError::new(format!(
                //             "Failed to load time zone id {time_zone_id:?}: {e}"
                //         ))))
                //     }
                // };
                // let date_time = date_time_utc.with_timezone(&tz);
                // let found_offset_seconds = date_time.offset().fix().local_minus_utc();
                // if found_offset_seconds != utc_offset_seconds {
                //     // "Timezone database for {s} does not agree with the offset \
                //     // {utc_offset_seconds} seconds, found {found_offset_seconds}. \
                //     // Either there is a typo or the timezone database is outdated."
                //     todo!("Emit warning, possibly a typo in the script");
                // }
                match jolt_version {
                    JoltVersion::V1 => {
                        let date_time = date_time.and_utc();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        Struct {
                            tag: 0x66,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::String(String::from(time_zone_id)),
                            ],
                        }
                    }
                    JoltVersion::V2 => {
                        let date_time_utc = date_time.and_utc()
                            - TimeDelta::new(utc_offset_seconds.into(), 0).unwrap();
                        let seconds = date_time_utc.timestamp();
                        let nanos = date_time_utc.timestamp_subsec_nanos();
                        Struct {
                            tag: 0x69,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::String(String::from(time_zone_id)),
                            ],
                        }
                    }
                }
            }
            (Some(utc_offset_seconds), None) => {
                // date time
                match jolt_version {
                    JoltVersion::V1 => {
                        let date_time = date_time.and_utc();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        Struct {
                            tag: 0x46,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::Integer(utc_offset_seconds.into()),
                            ],
                        }
                    }
                    JoltVersion::V2 => {
                        let tz = FixedOffset::east_opt(utc_offset_seconds)
                            .expect("regex enforced offset in bounds");
                        let date_time = date_time.and_local_timezone(tz).unwrap();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        Struct {
                            tag: 0x49,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::Integer(utc_offset_seconds.into()),
                            ],
                        }
                    }
                }
            }
            (None, Some(_)) => return Some(Err(ParseError::new(
                "DateTime with named zone requires an explicit time offset to avoid ambiguity. \
                E.g., `2025-03-06T18:05:02+01:00[Europe/Stockholm]` instead of \
                `2025-03-06T18:05:02[Europe/Stockholm]`",
            ))),
            (None, None) => {
                // local date time
                let date_time = date_time.and_utc();
                let seconds = date_time.timestamp();
                let nanos = date_time.timestamp_subsec_nanos();
                Struct {
                    tag: 0x64,
                    fields: vec![
                        PackStreamValue::Integer(seconds),
                        PackStreamValue::Integer(nanos.into()),
                    ],
                }
            }
        }))
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct BoltDuration {
    pub(crate) months: i64,
    pub(crate) days: i64,
    pub(crate) seconds: i64,
    pub(crate) nanos: i64,
}

impl BoltDuration {
    pub(crate) fn parse(s: &str) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static DURATION_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)D)",
                    r"(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-)?(\d+)(?:\.(\d{1,9}))?S)?)?"
                )).unwrap()
            });
        }
        let captures = DURATION_RE.with(|re| re.captures(s))?;
        let years = match i64::from_str(&captures[1]) {
            Ok(years) => years,
            Err(e) => {
                return Some(Err(ParseError::new(format!(
                    "Failed to parse duration years {}: {e}",
                    &captures[0]
                ))))
            }
        };

        fn i64_capture(
            i: usize,
            name: &str,
            captures: &Captures,
        ) -> Option<Result<i64, ParseError>> {
            Some(match captures.get(i) {
                None => Ok(0),
                Some(c) => i64::from_str(c.as_str()).map_err(|e| {
                    ParseError::new(format!(
                        "Failed to parse duration {name} {}: {e}",
                        c.as_str()
                    ))
                }),
            })
        }

        let months = opt_res_ret!(i64_capture(2, "months", &captures));
        let days = opt_res_ret!(i64_capture(3, "days", &captures));
        let hours = opt_res_ret!(i64_capture(4, "hours", &captures));
        let minutes = opt_res_ret!(i64_capture(5, "minutes", &captures));
        let seconds_sign = captures.get(6).map(|_| -1).unwrap_or(1);
        let mut seconds = opt_res_ret!(i64_capture(6, "seconds", &captures)) * seconds_sign;
        let mut nanos = captures
            .get(7)
            .map(|m| {
                // left align to fill in omitted decimal places
                i64::from_str(&format!("{:<09}", m.as_str()))
                    .expect("regex enforces nanos to be i64")
            })
            .unwrap_or_default();
        assert!(
            (0..=999_999_999).contains(&nanos),
            "regex enforces nanos to not overflow into seconds"
        );
        if seconds_sign < 0 && nanos > 0 {
            // sub-seconds being negative
            seconds -= 1;
            nanos = 1_000_000_000 - nanos;
        }

        let Some(months) = years
            .checked_mul(12)
            .and_then(|years_as_months| months.checked_add(years_as_months))
        else {
            return Some(Err(ParseError::new(
                "Duration months (together with years) are overflowing",
            )));
        };
        let Some(seconds) = hours
            .checked_mul(60)
            .and_then(|hours_as_minutes| minutes.checked_add(hours_as_minutes))
            .and_then(|minutes| seconds.checked_add(minutes * 60))
        else {
            return Some(Err(ParseError::new(
                "Duration seconds (together with hours and minutes) are overflowing",
            )));
        };
        Some(Ok(Self {
            months,
            days,
            seconds,
            nanos,
        }))
    }

    pub(crate) fn as_struct(&self) -> Option<Result<Struct, ParseError>> {
        Some(Ok(Struct {
            tag: TAG_DURATION,
            fields: vec![
                PackStreamValue::Integer(self.months),
                PackStreamValue::Integer(self.days),
                PackStreamValue::Integer(self.seconds),
                PackStreamValue::Integer(self.nanos),
            ],
        }))
    }
}

#[derive(Debug, Clone, Eq)]
pub(crate) struct BoltNode {
    pub(crate) id: i64,
    pub(crate) labels: Vec<String>,
    pub(crate) properties: IndexMap<String, PackStreamValue>,
    pub(crate) element_id: Option<String>,
}

impl PartialEq for BoltNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.properties == other.properties
            && self.element_id == other.element_id
            && self.labels.len() == other.labels.len()
            && self.labels.iter().sorted().eq(other.labels.iter().sorted())
    }
}

impl BoltNode {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"()\", but found {v:?}"
            )));
        };
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

        let (i, id) = next_field(&mut fields, "node id", i, "()", config)?;
        let (i, labels) = next_field(&mut fields, "labels", i, "()", config)?;
        let (i, properties) = next_field(&mut fields, "properties", i, "()", config)?;
        let (i, element_id) = match jolt_version {
            JoltVersion::V1 => (i, None),
            JoltVersion::V2 => {
                let (i, element_id) = next_field(&mut fields, "element id", i, "()", config)?;
                (i, Some(element_id))
            }
        };
        check_last_field(&mut fields, i, "()")?;

        Ok(Self {
            id,
            labels,
            properties,
            element_id,
        })
    }

    pub(crate) fn into_struct(self) -> Struct {
        let mut fields = Vec::with_capacity(4);
        fields.extend([
            PackStreamValue::Integer(self.id),
            PackStreamValue::List(
                self.labels
                    .into_iter()
                    .map(PackStreamValue::String)
                    .collect(),
            ),
            PackStreamValue::Dict(self.properties),
        ]);
        if let Some(element_id) = self.element_id {
            fields.push(PackStreamValue::String(element_id));
        }
        Struct {
            tag: TAG_NODE,
            fields,
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub(crate) struct BoltRelationship {
    pub(crate) id: i64,
    pub(crate) start_node_id: i64,
    pub(crate) rel_type: String,
    pub(crate) end_node_id: i64,
    pub(crate) properties: IndexMap<String, PackStreamValue>,
    pub(crate) element_id_ext: Option<BoltRelationshipElementIdExt>,
}

impl PartialEq for BoltRelationship {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.start_node_id == other.start_node_id
            && self.rel_type == other.rel_type
            && self.end_node_id == other.end_node_id
            && self.element_id_ext == other.element_id_ext
            && self.properties == other.properties
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct BoltRelationshipElementIdExt {
    pub(crate) element_id: String,
    pub(crate) start_node_element_id: String,
    pub(crate) end_node_element_id: String,
}

impl BoltRelationship {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"{{}}\", but found {v:?}"
            )));
        };
        let mut fields = fields.into_iter().enumerate();
        let i = 0;

        let (i, id) = next_field(&mut fields, "id", i, "{}", config)?;
        let (i, start_node_id) = next_field(&mut fields, "start node id", i, "{}", config)?;
        let (i, rel_type) = next_field(&mut fields, "relation ship type", i, "{}", config)?;
        let (i, end_node_id) = next_field(&mut fields, "end node it", i, "{}", config)?;
        let (i, properties) = next_field(&mut fields, "properties", i, "{}", config)?;
        let (i, element_id_ext) = match jolt_version {
            JoltVersion::V1 => (i, None),
            JoltVersion::V2 => {
                let (i, element_id) = next_field(&mut fields, "element id", i, "{}", config)?;
                let (i, start_node_element_id) =
                    next_field(&mut fields, "start node element id", i, "{}", config)?;
                let (i, end_node_element_id) =
                    next_field(&mut fields, "end node element id", i, "{}", config)?;
                (
                    i,
                    Some(BoltRelationshipElementIdExt {
                        element_id,
                        start_node_element_id,
                        end_node_element_id,
                    }),
                )
            }
        };
        check_last_field(&mut fields, i, "{}")?;

        Ok(Self {
            id,
            start_node_id,
            rel_type,
            end_node_id,
            properties,
            element_id_ext,
        })
    }

    pub(crate) fn flip_direction(&mut self) {
        mem::swap(&mut self.start_node_id, &mut self.end_node_id);
        if let Some(ext) = self.element_id_ext.as_mut() {
            mem::swap(&mut ext.start_node_element_id, &mut ext.end_node_element_id);
        }
    }

    pub(crate) fn into_struct(self) -> Struct {
        let mut fields = Vec::with_capacity(8);
        fields.extend([
            PackStreamValue::Integer(self.id),
            PackStreamValue::Integer(self.start_node_id),
            PackStreamValue::Integer(self.end_node_id),
            PackStreamValue::String(self.rel_type),
            PackStreamValue::Dict(self.properties),
        ]);
        if let Some(element_id_ext) = self.element_id_ext {
            fields.push(PackStreamValue::String(element_id_ext.element_id));
            fields.push(PackStreamValue::String(
                element_id_ext.start_node_element_id,
            ));
            fields.push(PackStreamValue::String(element_id_ext.end_node_element_id));
        }
        Struct {
            tag: TAG_RELATIONSHIP,
            fields,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BoltPath {
    pub(crate) nodes: Vec<BoltNode>,
    pub(crate) relationships: Vec<BoltUnboundRelationship>,
    pub(crate) indices: Vec<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct BoltUnboundRelationship {
    pub(crate) id: i64,
    pub(crate) rel_type: String,
    pub(crate) properties: IndexMap<String, PackStreamValue>,
    pub(crate) element_id: Option<String>,
}

impl From<BoltRelationship> for BoltUnboundRelationship {
    fn from(value: BoltRelationship) -> Self {
        Self {
            id: value.id,
            rel_type: value.rel_type,
            properties: value.properties,
            element_id: value.element_id_ext.map(|ext| ext.element_id),
        }
    }
}

impl BoltUnboundRelationship {
    pub(crate) fn into_struct(self) -> Struct {
        let mut fields = Vec::with_capacity(4);
        fields.extend([
            PackStreamValue::Integer(self.id),
            PackStreamValue::String(self.rel_type),
            PackStreamValue::Dict(self.properties),
        ]);
        if let Some(element_id) = self.element_id {
            fields.push(PackStreamValue::String(element_id));
        }
        Struct {
            tag: TAG_UNBOUND_RELATIONSHIP,
            fields,
        }
    }
}

impl BoltPath {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"..\", but found {v:?}"
            )));
        };
        if fields.len() % 2 != 1 {
            return Err(ParseError::new(format!(
                "Expected odd number of fields after sigil \"..\", but found {}",
                fields.len()
            )));
        }
        let mut fields = fields.into_iter().enumerate().peekable();
        let mut nodes = Vec::with_capacity(fields.len() / 2 + 1);
        let mut relationships = Vec::with_capacity(fields.len() / 2);

        let (i, field) = fields.next().expect("checked non-empty above");
        nodes.push(Self::next_node(i, field, jolt_version, config)?);

        while let Some((i, field)) = fields.next() {
            let relationship = Self::next_relationship(i, field, jolt_version, config)?;
            relationships.push(relationship);
            let (i, field) = fields.next().expect("checked uneven size above");
            let node = Self::next_node(i, field, jolt_version, config)?;
            nodes.push(node);
        }
        match jolt_version {
            JoltVersion::V1 => Self::encode_indices::<IdPathIndexer>(nodes, relationships),
            JoltVersion::V2 => Self::encode_indices::<ElementIdPathIndexer>(nodes, relationships),
        }
    }

    fn next_node(
        i: usize,
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<BoltNode, ParseError> {
        fn not_node(i: usize, v: impl Debug) -> ParseError {
            ParseError::new(format!(
                "Expected path entry at index {i} to be a Jolt node, but found {v:?}"
            ))
        }

        let JsonValue::Object(v) = v else {
            return Err(not_node(i, v));
        };
        if v.len() != 1 {
            return Err(not_node(i, v));
        }
        if v.keys().next().expect("checked not-empty above") != "()" {
            return Err(not_node(i, v));
        }
        BoltNode::parse(
            v.into_values().next().expect("checked non-empty above"),
            jolt_version,
            config,
        )
    }

    fn next_relationship(
        i: usize,
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<BoltRelationship, ParseError> {
        fn not_relationship(i: usize, v: impl Debug) -> ParseError {
            ParseError::new(format!(
                "Expected path entry at index {i} to be a Jolt relationship, but found {v:?}"
            ))
        }

        let JsonValue::Object(v) = v else {
            return Err(not_relationship(i, v));
        };
        if v.len() != 1 {
            return Err(not_relationship(i, v));
        }
        let reverse = match &**v.keys().next().expect("checked non-empty above") {
            "->" => false,
            "<-" => true,
            _ => return Err(not_relationship(i, v)),
        };
        let mut relationship = BoltRelationship::parse(
            v.into_values().next().expect("checked non-empty above"),
            jolt_version,
            config,
        )?;
        if reverse {
            relationship.flip_direction();
        }
        Ok(relationship)
    }

    fn encode_indices<P: PathIndexer>(
        nodes: Vec<BoltNode>,
        relationships: Vec<BoltRelationship>,
    ) -> Result<Self, ParseError> {
        assert_eq!(relationships.len() + 1, nodes.len());
        i64::try_from(nodes.len() + relationships.len())
            .expect("How does even fit into your memory?!?");

        let nodes = nodes.into_iter().map(Rc::new).collect::<Vec<_>>();
        let relationships = relationships.into_iter().map(Rc::new).collect::<Vec<_>>();
        let mut indices = Vec::with_capacity(nodes.len() - 1 + relationships.len());

        let mut unique_nodes = IndexMap::with_capacity(nodes.len());
        for (i, node) in nodes.iter().enumerate() {
            let idx = P::node_index(node);
            if let Some(other_node) = unique_nodes.get(idx) {
                if other_node != node {
                    return Err(ParseError::new(format!(
                        "Path contains multiple nodes with the same {} {idx:?} \
                        but different values",
                        P::name()
                    )));
                }
            } else {
                unique_nodes.insert(idx.clone(), Rc::clone(node));
            }
            if i != 0 {
                indices.push(unique_nodes.get_index_of(idx).expect("inserted above") as i64);
            }
        }

        let mut unique_relationships = IndexMap::with_capacity(relationships.len());
        for (i, relationship) in relationships.iter().enumerate() {
            let idx = P::relationship_index(relationship);
            if let Some(other_relationship) = unique_relationships.get(idx) {
                if other_relationship != relationship {
                    return Err(ParseError::new(format!(
                        "Path contains multiple relationships with the same {} {idx:?} \
                        but different values",
                        P::name()
                    )));
                }
            } else {
                unique_relationships.insert(idx.clone(), Rc::clone(relationship));
            }
            let prev_node = &nodes[i];
            let prev_node_idx = P::node_index(prev_node);
            let start_node_idx = P::relationship_start_index(relationship);
            let next_node = &nodes[i + 1];
            let next_node_idx = P::node_index(next_node);
            let end_node_idx = P::relationship_end_index(relationship);
            let positive_index = unique_relationships
                .get_index_of(idx)
                .expect("inserted above") as i64
                + 1;
            if (start_node_idx, end_node_idx) == (prev_node_idx, next_node_idx) {
                indices.push(positive_index);
            } else if (start_node_idx, end_node_idx) == (next_node_idx, prev_node_idx) {
                indices.push(-positive_index);
            } else {
                return Err(ParseError::new(format!(
                    "Path relationship at position {} does not connect the previous and next nodes",
                    i * 2 + 1,
                )));
            }
        }

        let nodes = unique_nodes
            .into_values()
            .map(|rc| Rc::into_inner(rc).expect("dropped all other owners"))
            .collect();
        let relationships = unique_relationships
            .into_values()
            .map(|rc| Rc::into_inner(rc).expect("dropped all other owners"))
            .map(Into::into)
            .collect();

        Ok(Self {
            nodes,
            relationships,
            indices,
        })
    }

    pub(crate) fn into_struct(self) -> Struct {
        let nodes = PackStreamValue::List(
            self.nodes
                .into_iter()
                .map(BoltNode::into_struct)
                .map(PackStreamValue::Struct)
                .collect(),
        );
        let relationships = PackStreamValue::List(
            self.relationships
                .into_iter()
                .map(BoltUnboundRelationship::into_struct)
                .map(PackStreamValue::Struct)
                .collect(),
        );
        let indices = PackStreamValue::List(
            self.indices
                .into_iter()
                .map(PackStreamValue::Integer)
                .collect(),
        );
        Struct {
            tag: TAG_PATH,
            fields: vec![nodes, relationships, indices],
        }
    }
}

trait PathIndexer {
    type Index: Hash + Eq + Clone + Debug;
    fn name() -> &'static str;
    fn node_index(node: &BoltNode) -> &Self::Index;
    fn relationship_index(relationship: &BoltRelationship) -> &Self::Index;
    fn relationship_start_index(relationship: &BoltRelationship) -> &Self::Index;
    fn relationship_end_index(relationship: &BoltRelationship) -> &Self::Index;
}

struct IdPathIndexer;
impl PathIndexer for IdPathIndexer {
    type Index = i64;
    fn name() -> &'static str {
        "id"
    }
    fn node_index(node: &BoltNode) -> &Self::Index {
        &node.id
    }
    fn relationship_index(relationship: &BoltRelationship) -> &Self::Index {
        &relationship.id
    }
    fn relationship_start_index(relationship: &BoltRelationship) -> &Self::Index {
        &relationship.start_node_id
    }
    fn relationship_end_index(relationship: &BoltRelationship) -> &Self::Index {
        &relationship.end_node_id
    }
}

struct ElementIdPathIndexer;
impl PathIndexer for ElementIdPathIndexer {
    type Index = String;
    fn name() -> &'static str {
        "element id"
    }
    fn node_index(node: &BoltNode) -> &Self::Index {
        node.element_id
            .as_ref()
            .expect("cannot user ElementIdPathIndexer without element ids")
    }
    fn relationship_index(relationship: &BoltRelationship) -> &Self::Index {
        &relationship
            .element_id_ext
            .as_ref()
            .expect("cannot user ElementIdPathIndexer without element ids")
            .element_id
    }
    fn relationship_start_index(relationship: &BoltRelationship) -> &Self::Index {
        &relationship
            .element_id_ext
            .as_ref()
            .expect("cannot user ElementIdPathIndexer without element ids")
            .start_node_element_id
    }
    fn relationship_end_index(relationship: &BoltRelationship) -> &Self::Index {
        &relationship
            .element_id_ext
            .as_ref()
            .expect("cannot user ElementIdPathIndexer without element ids")
            .end_node_element_id
    }
}

fn next_field<R: ExtractableField>(
    fields: &mut impl Iterator<Item = (usize, JsonValue)>,
    name: &str,
    field_num: usize,
    sigil: &str,
    config: &ActorConfig,
) -> Result<(usize, R), ParseError> {
    let Some((field_num, field)) = fields.next() else {
        return Err(ParseError::new(format!(
            "Missing array entry {name} (field {}) after sigil \"{sigil}\"",
            field_num + 1
        )));
    };
    let field = match R::extract(field, config) {
        Ok(field) => field,
        Err(err) => return Err(err.into_parse_error(name, field_num, sigil)),
    };
    Ok((field_num, field))
}

fn check_last_field(
    fields: &mut impl Iterator<Item = (usize, JsonValue)>,
    field_num: usize,
    sigil: &str,
) -> Result<(), ParseError> {
    if fields.next().is_some() {
        return Err(ParseError::new(format!(
            "Too many fields after sigil \"{sigil}\", expected {}",
            field_num + 1
        )));
    };
    Ok(())
}

#[derive(Debug)]
enum ExtractionFailure {
    Expectation(String),
    Nested(ParseError),
}

impl ExtractionFailure {
    fn into_parse_error(self, name: &str, field_num: usize, sigil: &str) -> ParseError {
        match self {
            Self::Expectation(expectation) => ParseError::new(format!(
                "Expected {name} (field {field_num}) after sigil \"{sigil}\" {expectation}"
            )),
            Self::Nested(err) => err,
        }
    }
}

impl From<String> for ExtractionFailure {
    fn from(expectation: String) -> Self {
        Self::Expectation(expectation)
    }
}

impl From<ParseError> for ExtractionFailure {
    fn from(err: ParseError) -> Self {
        Self::Nested(err)
    }
}

trait ExtractableField: Sized {
    fn extract(field: JsonValue, config: &ActorConfig) -> Result<Self, ExtractionFailure>;
}

impl ExtractableField for i64 {
    fn extract(field: JsonValue, _: &ActorConfig) -> Result<Self, ExtractionFailure> {
        field
            .as_i64()
            .ok_or_else(|| format!("to be i64, but found {field:?}").into())
    }
}

impl ExtractableField for Vec<String> {
    fn extract(field: JsonValue, _: &ActorConfig) -> Result<Self, ExtractionFailure> {
        let JsonValue::Array(field) = field else {
            return Err(format!("to be an array, but found {field:?}").into());
        };
        field
            .into_iter()
            .enumerate()
            .map(|(i, l)| {
                let JsonValue::String(l) = l else {
                    return Err(format!(
                        "to be an array of strings, but found {l:?} (at {})",
                        i + 1,
                    )
                    .into());
                };
                Ok(l)
            })
            .collect()
    }
}

impl ExtractableField for IndexMap<String, PackStreamValue> {
    fn extract(field: JsonValue, config: &ActorConfig) -> Result<Self, ExtractionFailure> {
        let JsonValue::Object(field) = field else {
            return Err(format!("to be a map, but found {field:?}").into());
        };
        field
            .into_iter()
            .map(|(k, v)| transcode_field(v, config).map(|v| (k, v)))
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }
}

impl ExtractableField for String {
    fn extract(field: JsonValue, _: &ActorConfig) -> Result<Self, ExtractionFailure> {
        let JsonValue::String(field) = field else {
            return Err(format!("to be string, but found {field:?}").into());
        };
        Ok(field)
    }
}

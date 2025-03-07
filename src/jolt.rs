use std::cell::LazyCell;
use std::str::FromStr;

use chrono::{NaiveDate, NaiveTime};
use regex::{Captures, Regex};

use crate::util::opt_res_ret;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum JoltVersion {
    V1,
    V2,
}

impl JoltVersion {
    pub fn parse(s: &str) -> Result<Self, String> {
        let jolt_version =
            i32::from_str(s).map_err(|e| format!("Jolt version must be i32 (found {s:?}): {e}"))?;
        Ok(match jolt_version {
            1 => Self::V1,
            2 => Self::V2,
            _ => return Err(format!("Unknown jolt version: {s}")),
        })
    }
}

#[derive(Debug)]
pub struct JoltDate {
    pub date: NaiveDate,
}

impl JoltDate {
    pub fn parse(s: &str) -> Option<Result<Self, String>> {
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
            return Some(Err(format!("Unparsable date {s:?}")));
        };
        Some(Ok(Self { date }))
    }
}

#[derive(Debug)]
pub struct JoltTime<'a> {
    pub time: NaiveTime,
    pub utc_offset_seconds: Option<i32>,
    pub time_zone_id: Option<&'a str>,
}

impl<'a> JoltTime<'a> {
    pub fn parse(s: &'a str) -> Option<Result<Self, String>> {
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
            return Some(Err(format!(
                "Unparsable time: {s:?} (leap seconds are not supported)"
            )));
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
            return Some(Err(format!("Unparsable time: {s:?}")));
        };
        Some(Ok(Self {
            time,
            utc_offset_seconds,
            time_zone_id,
        }))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct JoltDuration {
    pub months: i64,
    pub days: i64,
    pub seconds: i64,
    pub nanos: i64,
}

impl JoltDuration {
    pub fn parse(s: &str) -> Option<Result<Self, String>> {
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
                return Some(Err(format!(
                    "Failed to parse duration years {}: {e}",
                    &captures[0]
                )))
            }
        };

        fn i64_capture(i: usize, name: &str, captures: &Captures) -> Option<Result<i64, String>> {
            Some(match captures.get(i) {
                None => Ok(0),
                Some(c) => i64::from_str(c.as_str())
                    .map_err(|e| format!("Failed to parse duration {name} {}: {e}", c.as_str())),
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
            return Some(Err(String::from(
                "Duration months (together with years) are overflowing",
            )));
        };
        let Some(seconds) = hours
            .checked_mul(60)
            .and_then(|hours_as_minutes| minutes.checked_add(hours_as_minutes))
            .and_then(|minutes| seconds.checked_add(minutes * 60))
        else {
            return Some(Err(String::from(
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
}

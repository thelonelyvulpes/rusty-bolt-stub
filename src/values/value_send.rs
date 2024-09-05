// Copyright Rouven Bauer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, VecDeque};

use itertools::Itertools;

use super::graph;
use super::spatial;
use super::time;

/// A value received from the database.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ValueSend {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<ValueSend>),
    Map(HashMap<String, ValueSend>),
    Node(graph::Node),
    Relationship(graph::Relationship),
    Path(graph::Path),
    Cartesian2D(spatial::Cartesian2D),
    Cartesian3D(spatial::Cartesian3D),
    WGS84_2D(spatial::WGS84_2D),
    WGS84_3D(spatial::WGS84_3D),
    Duration(time::Duration),
    LocalTime(time::LocalTime),
    Time(time::Time),
    Date(time::Date),
    LocalDateTime(time::LocalDateTime),
    DateTime(time::DateTime),
    DateTimeFixed(time::DateTimeFixed),
    /// A value that could not be received.
    /// This can have multiple reasons, for example:
    ///  * Unexpected struct data (likely a driver or server bug)
    ///  * Temporal types that cannot be represented on the client side like values out of range
    ///    and time zones not supported by the client.
    BrokenValue(BrokenValue),
}

impl ValueSend {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, ValueSend::Null)
    }
}

impl TryFrom<ValueSend> for bool {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Boolean(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, ValueSend::Boolean(_))
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ValueSend::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_bool(self) -> Result<bool, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for i64 {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Integer(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_int(&self) -> bool {
        matches!(self, ValueSend::Integer(_))
    }

    #[inline]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            ValueSend::Integer(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_int(self) -> Result<i64, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for f64 {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Float(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, ValueSend::Float(_))
    }

    #[inline]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            ValueSend::Float(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_float(self) -> Result<f64, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for Vec<u8> {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Bytes(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_bytes(&self) -> bool {
        matches!(self, ValueSend::Bytes(_))
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            ValueSend::Bytes(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_bytes(self) -> Result<Vec<u8>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for String {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::String(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, ValueSend::String(_))
    }

    #[inline]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            ValueSend::String(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_string(self) -> Result<String, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for Vec<ValueSend> {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::List(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, ValueSend::List(_))
    }

    #[inline]
    pub fn as_list(&self) -> Option<&[ValueSend]> {
        match self {
            ValueSend::List(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_list(self) -> Result<Vec<ValueSend>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for HashMap<String, ValueSend> {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Map(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, ValueSend::Map(_))
    }

    #[inline]
    pub fn as_map(&self) -> Option<&HashMap<String, ValueSend>> {
        match self {
            ValueSend::Map(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_map(self) -> Result<HashMap<String, ValueSend>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for graph::Node {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Node(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_node(&self) -> bool {
        matches!(self, ValueSend::Node(_))
    }

    #[inline]
    pub fn as_node(&self) -> Option<&graph::Node> {
        match self {
            ValueSend::Node(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_node(self) -> Result<graph::Node, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for graph::Relationship {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Relationship(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_relationship(&self) -> bool {
        matches!(self, ValueSend::Relationship(_))
    }

    #[inline]
    pub fn as_relationship(&self) -> Option<&graph::Relationship> {
        match self {
            ValueSend::Relationship(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_relationship(self) -> Result<graph::Relationship, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for graph::Path {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Path(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_path(&self) -> bool {
        matches!(self, ValueSend::Path(_))
    }

    #[inline]
    pub fn as_path(&self) -> Option<&graph::Path> {
        match self {
            ValueSend::Path(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_path(self) -> Result<graph::Path, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for spatial::Cartesian2D {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Cartesian2D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_cartesian_2d(&self) -> bool {
        matches!(self, ValueSend::Cartesian2D(_))
    }

    #[inline]
    pub fn as_cartesian_2d(&self) -> Option<&spatial::Cartesian2D> {
        match self {
            ValueSend::Cartesian2D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_cartesian_2d(self) -> Result<spatial::Cartesian2D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for spatial::Cartesian3D {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Cartesian3D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_cartesian_3d(&self) -> bool {
        matches!(self, ValueSend::Cartesian3D(_))
    }

    #[inline]
    pub fn as_cartesian_3d(&self) -> Option<&spatial::Cartesian3D> {
        match self {
            ValueSend::Cartesian3D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_cartesian_3d(self) -> Result<spatial::Cartesian3D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for spatial::WGS84_2D {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::WGS84_2D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_wgs84_2d(&self) -> bool {
        matches!(self, ValueSend::WGS84_2D(_))
    }

    #[inline]
    pub fn as_wgs84_2d(&self) -> Option<&spatial::WGS84_2D> {
        match self {
            ValueSend::WGS84_2D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_wgs84_2d(self) -> Result<spatial::WGS84_2D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for spatial::WGS84_3D {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::WGS84_3D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_wgs84_3d(&self) -> bool {
        matches!(self, ValueSend::WGS84_3D(_))
    }

    #[inline]
    pub fn as_wgs84_3d(&self) -> Option<&spatial::WGS84_3D> {
        match self {
            ValueSend::WGS84_3D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_wgs84_3d(self) -> Result<spatial::WGS84_3D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for time::Duration {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Duration(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_duration(&self) -> bool {
        matches!(self, ValueSend::Duration(_))
    }

    #[inline]
    pub fn as_duration(&self) -> Option<&time::Duration> {
        match self {
            ValueSend::Duration(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_duration(self) -> Result<time::Duration, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for time::LocalTime {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::LocalTime(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_local_time(&self) -> bool {
        matches!(self, ValueSend::LocalTime(_))
    }

    #[inline]
    pub fn as_local_time(&self) -> Option<&time::LocalTime> {
        match self {
            ValueSend::LocalTime(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_local_time(self) -> Result<time::LocalTime, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for time::Time {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Time(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_time(&self) -> bool {
        matches!(self, ValueSend::Time(_))
    }

    #[inline]
    pub fn as_time(&self) -> Option<&time::Time> {
        match self {
            ValueSend::Time(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_time(self) -> Result<time::Time, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for time::Date {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::Date(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_date(&self) -> bool {
        matches!(self, ValueSend::Date(_))
    }

    #[inline]
    pub fn as_date(&self) -> Option<&time::Date> {
        match self {
            ValueSend::Date(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_date(self) -> Result<time::Date, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for time::LocalDateTime {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::LocalDateTime(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_local_date_time(&self) -> bool {
        matches!(self, ValueSend::LocalDateTime(_))
    }

    #[inline]
    pub fn as_local_date_time(&self) -> Option<&time::LocalDateTime> {
        match self {
            ValueSend::LocalDateTime(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_local_date_time(self) -> Result<time::LocalDateTime, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for time::DateTime {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::DateTime(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_date_time(&self) -> bool {
        matches!(self, ValueSend::DateTime(_))
    }

    #[inline]
    pub fn as_date_time(&self) -> Option<&time::DateTime> {
        match self {
            ValueSend::DateTime(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_date_time(self) -> Result<time::DateTime, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueSend> for time::DateTimeFixed {
    type Error = ValueSend;

    #[inline]
    fn try_from(value: ValueSend) -> Result<Self, Self::Error> {
        match value {
            ValueSend::DateTimeFixed(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueSend {
    #[inline]
    pub fn is_date_time_fixed(&self) -> bool {
        matches!(self, ValueSend::DateTimeFixed(_))
    }

    #[inline]
    pub fn as_date_time_fixed(&self) -> Option<&time::DateTimeFixed> {
        match self {
            ValueSend::DateTimeFixed(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_date_time_fixed(self) -> Result<time::DateTimeFixed, Self> {
        self.try_into()
    }
}

#[derive(Debug, Clone)]
pub struct BrokenValue {
    pub(crate) inner: BrokenValueInner,
}

impl PartialEq for BrokenValue {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub(crate) enum BrokenValueInner {
    Reason(String),
    UnknownStruct {
        tag: u8,
        fields: VecDeque<ValueSend>,
    },
    InvalidStruct {
        reason: String,
    },
}

impl BrokenValue {
    pub fn reason(&self) -> &str {
        match &self.inner {
            BrokenValueInner::Reason(reason) => reason,
            BrokenValueInner::UnknownStruct { .. } => "received an unknown packstream struct",
            BrokenValueInner::InvalidStruct { reason, .. } => reason,
        }
    }
}

impl From<BrokenValueInner> for BrokenValue {
    fn from(inner: BrokenValueInner) -> Self {
        BrokenValue { inner }
    }
}

impl ValueSend {
    pub(crate) fn dbg_print(&self) -> String {
        match self {
            ValueSend::Null => "null".into(),
            ValueSend::Boolean(v) => v.to_string(),
            ValueSend::Integer(v) => v.to_string(),
            ValueSend::Float(v) => v.to_string(),
            ValueSend::Bytes(v) => format!("bytes{v:02X?}"),
            ValueSend::String(v) => format!("{v:?}"),
            ValueSend::List(v) => format!("[{}]", v.iter().map(|e| e.dbg_print()).format(", ")),
            ValueSend::Map(v) => format!(
                "{{{}}}",
                v.iter()
                    .map(|(k, e)| format!("{:?}: {}", k, e.dbg_print()))
                    .format(", ")
            ),
            ValueSend::Node(node) => node.to_string(),
            ValueSend::Relationship(relationship) => relationship.to_string(),
            ValueSend::Path(path) => path.to_string(),
            ValueSend::Cartesian2D(v) => format!("{v:?}"),
            ValueSend::Cartesian3D(v) => format!("{v:?}"),
            ValueSend::WGS84_2D(v) => format!("{v:?}"),
            ValueSend::WGS84_3D(v) => format!("{v:?}"),
            ValueSend::Duration(v) => format!("{v:?}"),
            ValueSend::LocalTime(v) => format!("{v:?}"),
            ValueSend::Time(v) => format!("{v:?}"),
            ValueSend::Date(v) => format!("{v:?}"),
            ValueSend::LocalDateTime(v) => format!("{v:?}"),
            ValueSend::DateTime(v) => format!("{v:?}"),
            ValueSend::DateTimeFixed(v) => format!("{v:?}"),
            ValueSend::BrokenValue(broken_value) => {
                format!("BrokenValue({})", broken_value.reason())
            }
        }
    }
}

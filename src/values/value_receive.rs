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

use crate::values::{spatial, time};
use std::collections::HashMap;
use crate::types::BoltVersion;

#[allow(unused)]
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ValueReceive {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<ValueReceive>),
    Map(HashMap<String, ValueReceive>),
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
}

impl ValueReceive {
    pub(crate) fn from_data(p0: &[u8], p1: &BoltVersion) -> anyhow::Result<Vec<ValueReceive>> {

        Ok(vec![])
    }
}

impl ValueReceive {
    pub(crate) fn eq_data(&self, other: &Self) -> bool {
        match self {
            ValueReceive::Null => matches!(other, ValueReceive::Null),
            ValueReceive::Boolean(v1) => matches!(other, ValueReceive::Boolean(v2) if v1 == v2),
            ValueReceive::Integer(v1) => matches!(other, ValueReceive::Integer(v2) if v1 == v2),
            ValueReceive::Float(v1) => match other {
                ValueReceive::Float(v2) => v1.to_bits() == v2.to_bits(),
                _ => false,
            },
            ValueReceive::Bytes(v1) => matches!(other, ValueReceive::Bytes(v2) if v1 == v2),
            ValueReceive::String(v1) => matches!(other, ValueReceive::String(v2) if v1 == v2),
            ValueReceive::List(v1) => match other {
                ValueReceive::List(v2) if v1.len() == v2.len() => {
                    v1.iter().zip(v2.iter()).all(|(v1, v2)| v1.eq_data(v2))
                }
                _ => false,
            },
            ValueReceive::Map(v1) => match other {
                ValueReceive::Map(v2) if v1.len() == v2.len() => v1
                    .iter()
                    .zip(v2.iter())
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && v1.eq_data(v2)),
                _ => false,
            },
            ValueReceive::Cartesian2D(v1) => {
                matches!(other, ValueReceive::Cartesian2D(v2) if v1.eq_data(v2))
            }
            ValueReceive::Cartesian3D(v1) => {
                matches!(other, ValueReceive::Cartesian3D(v2) if v1.eq_data(v2))
            }
            ValueReceive::WGS84_2D(v1) => {
                matches!(other, ValueReceive::WGS84_2D(v2) if v1.eq_data(v2))
            }
            ValueReceive::WGS84_3D(v1) => {
                matches!(other, ValueReceive::WGS84_3D(v2) if v1.eq_data(v2))
            }
            ValueReceive::Duration(v1) => matches!(other, ValueReceive::Duration(v2) if v1 == v2),
            ValueReceive::LocalTime(v1) => matches!(other, ValueReceive::LocalTime(v2) if v1 == v2),
            ValueReceive::Time(v1) => matches!(other, ValueReceive::Time(v2) if v1 == v2),
            ValueReceive::Date(v1) => matches!(other, ValueReceive::Date(v2) if v1 == v2),
            ValueReceive::LocalDateTime(v1) => {
                matches!(other, ValueReceive::LocalDateTime(v2) if v1 == v2)
            }
            ValueReceive::DateTime(v1) => matches!(other, ValueReceive::DateTime(v2) if v1 == v2),
            ValueReceive::DateTimeFixed(v1) => {
                matches!(other, ValueReceive::DateTimeFixed(v2) if v1 == v2)
            }
        }
    }
}

macro_rules! impl_value_from_into {
    ( $value:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for ValueReceive {
                fn from(value: $ty) -> Self {
                    $value(value.into())
                }
            }
        )*
    };
}

macro_rules! impl_value_from_owned {
    ( $value:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for ValueReceive {
                fn from(value: $ty) -> Self {
                    $value(value)
                }
            }
        )*
    };
}

impl_value_from_into!(ValueReceive::Boolean, bool);
impl_value_from_into!(ValueReceive::Integer, u8, u16, u32, i8, i16, i32, i64);
impl_value_from_into!(ValueReceive::Float, f32, f64);
impl_value_from_into!(ValueReceive::String, &str);

impl_value_from_owned!(ValueReceive::String, String);
// impl_value_from_owned!(Value::List, Vec<Value>);
// impl_value_from_owned!(Value::Map, HashMap<String, Value>);
impl_value_from_owned!(ValueReceive::Cartesian2D, spatial::Cartesian2D);
impl_value_from_owned!(ValueReceive::Cartesian3D, spatial::Cartesian3D);
impl_value_from_owned!(ValueReceive::WGS84_2D, spatial::WGS84_2D);
impl_value_from_owned!(ValueReceive::WGS84_3D, spatial::WGS84_3D);
impl_value_from_owned!(ValueReceive::Duration, time::Duration);
impl_value_from_owned!(ValueReceive::LocalTime, time::LocalTime);
impl_value_from_owned!(ValueReceive::Time, time::Time);
impl_value_from_owned!(ValueReceive::Date, time::Date);
impl_value_from_owned!(ValueReceive::LocalDateTime, time::LocalDateTime);
impl_value_from_owned!(ValueReceive::DateTime, time::DateTime);
impl_value_from_owned!(ValueReceive::DateTimeFixed, time::DateTimeFixed);

impl<T: Into<ValueReceive>> From<HashMap<String, T>> for ValueReceive {
    fn from(value: HashMap<String, T>) -> Self {
        ValueReceive::Map(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<ValueReceive>> From<Vec<T>> for ValueReceive {
    fn from(value: Vec<T>) -> Self {
        ValueReceive::List(value.into_iter().map(|v| v.into()).collect())
    }
}

impl<T: Into<ValueReceive>> From<Option<T>> for ValueReceive {
    fn from(value: Option<T>) -> Self {
        match value {
            None => ValueReceive::Null,
            Some(v) => v.into(),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, ValueReceive::Null)
    }
}

impl TryFrom<ValueReceive> for bool {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Boolean(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, ValueReceive::Boolean(_))
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ValueReceive::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_bool(self) -> Result<bool, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for i64 {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Integer(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_int(&self) -> bool {
        matches!(self, ValueReceive::Integer(_))
    }

    #[inline]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            ValueReceive::Integer(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_int(self) -> Result<i64, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for f64 {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Float(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, ValueReceive::Float(_))
    }

    #[inline]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            ValueReceive::Float(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_float(self) -> Result<f64, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for Vec<u8> {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Bytes(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_bytes(&self) -> bool {
        matches!(self, ValueReceive::Bytes(_))
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            ValueReceive::Bytes(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_bytes(self) -> Result<Vec<u8>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for String {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::String(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, ValueReceive::String(_))
    }

    #[inline]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            ValueReceive::String(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_string(self) -> Result<String, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for Vec<ValueReceive> {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::List(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, ValueReceive::List(_))
    }

    #[inline]
    pub fn as_list(&self) -> Option<&[ValueReceive]> {
        match self {
            ValueReceive::List(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_list(self) -> Result<Vec<ValueReceive>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for HashMap<String, ValueReceive> {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Map(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, ValueReceive::Map(_))
    }

    #[inline]
    pub fn as_map(&self) -> Option<&HashMap<String, ValueReceive>> {
        match self {
            ValueReceive::Map(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_map(self) -> Result<HashMap<String, ValueReceive>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for spatial::Cartesian2D {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Cartesian2D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_cartesian_2d(&self) -> bool {
        matches!(self, ValueReceive::Cartesian2D(_))
    }

    #[inline]
    pub fn as_cartesian_2d(&self) -> Option<&spatial::Cartesian2D> {
        match self {
            ValueReceive::Cartesian2D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_cartesian_2d(self) -> Result<spatial::Cartesian2D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for spatial::Cartesian3D {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Cartesian3D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_cartesian_3d(&self) -> bool {
        matches!(self, ValueReceive::Cartesian3D(_))
    }

    #[inline]
    pub fn as_cartesian_3d(&self) -> Option<&spatial::Cartesian3D> {
        match self {
            ValueReceive::Cartesian3D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_cartesian_3d(self) -> Result<spatial::Cartesian3D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for spatial::WGS84_2D {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::WGS84_2D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_wgs84_2d(&self) -> bool {
        matches!(self, ValueReceive::WGS84_2D(_))
    }

    #[inline]
    pub fn as_wgs84_2d(&self) -> Option<&spatial::WGS84_2D> {
        match self {
            ValueReceive::WGS84_2D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_wgs84_2d(self) -> Result<spatial::WGS84_2D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for spatial::WGS84_3D {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::WGS84_3D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_wgs84_3d(&self) -> bool {
        matches!(self, ValueReceive::WGS84_3D(_))
    }

    #[inline]
    pub fn as_wgs84_3d(&self) -> Option<&spatial::WGS84_3D> {
        match self {
            ValueReceive::WGS84_3D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_wgs84_3d(self) -> Result<spatial::WGS84_3D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for time::Duration {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Duration(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_duration(&self) -> bool {
        matches!(self, ValueReceive::Duration(_))
    }

    #[inline]
    pub fn as_duration(&self) -> Option<&time::Duration> {
        match self {
            ValueReceive::Duration(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_duration(self) -> Result<time::Duration, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for time::LocalTime {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::LocalTime(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_local_time(&self) -> bool {
        matches!(self, ValueReceive::LocalTime(_))
    }

    #[inline]
    pub fn as_local_time(&self) -> Option<&time::LocalTime> {
        match self {
            ValueReceive::LocalTime(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_local_time(self) -> Result<time::LocalTime, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for time::Time {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Time(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_time(&self) -> bool {
        matches!(self, ValueReceive::Time(_))
    }

    #[inline]
    pub fn as_time(&self) -> Option<&time::Time> {
        match self {
            ValueReceive::Time(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_time(self) -> Result<time::Time, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for time::Date {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Date(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_date(&self) -> bool {
        matches!(self, ValueReceive::Date(_))
    }

    #[inline]
    pub fn as_date(&self) -> Option<&time::Date> {
        match self {
            ValueReceive::Date(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_date(self) -> Result<time::Date, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for time::LocalDateTime {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::LocalDateTime(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_local_date_time(&self) -> bool {
        matches!(self, ValueReceive::LocalDateTime(_))
    }

    #[inline]
    pub fn as_local_date_time(&self) -> Option<&time::LocalDateTime> {
        match self {
            ValueReceive::LocalDateTime(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_local_date_time(self) -> Result<time::LocalDateTime, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for time::DateTime {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::DateTime(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_date_time(&self) -> bool {
        matches!(self, ValueReceive::DateTime(_))
    }

    #[inline]
    pub fn as_date_time(&self) -> Option<&time::DateTime> {
        match self {
            ValueReceive::DateTime(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_date_time(self) -> Result<time::DateTime, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for time::DateTimeFixed {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::DateTimeFixed(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_date_time_fixed(&self) -> bool {
        matches!(self, ValueReceive::DateTimeFixed(_))
    }

    #[inline]
    pub fn as_date_time_fixed(&self) -> Option<&time::DateTimeFixed> {
        match self {
            ValueReceive::DateTimeFixed(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_date_time_fixed(self) -> Result<time::DateTimeFixed, Self> {
        self.try_into()
    }
}

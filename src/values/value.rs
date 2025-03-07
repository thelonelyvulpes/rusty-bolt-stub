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

use std::convert::Into;
use std::io::Write;

use anyhow::{anyhow, Result};
use indexmap::IndexMap;
use nom::ToUsize;
use usize_cast::FromUsize;

// TODO: rename to BoltValue
#[allow(unused)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Value>),
    // TODO: rename to Dict to match PackStream terminology
    Map(IndexMap<String, Value>),
    Struct(Struct),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Struct {
    pub tag: u8,
    pub fields: Vec<Value>,
}

impl Value {
    pub(crate) fn from_data_consume_all(data: &[u8]) -> Result<Value> {
        let mut decoder = PackStreamDecoder::new(data, 0);
        let value = decoder.read()?;
        if decoder.index != data.len() {
            return Err(anyhow!(
                "Unconsumed data ({} byes) {:?} read: {:?}",
                data.len() - decoder.index,
                data,
                value
            ));
        }
        Ok(value)
    }

    pub(crate) fn to_data(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(128);
        let mut serializer = PackStreamSerializer::new(&mut data);
        serializer.write(self);
        data
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::Null => matches!(other, Value::Null),
            Value::Boolean(v1) => matches!(other, Value::Boolean(v2) if v1 == v2),
            Value::Integer(v1) => matches!(other, Value::Integer(v2) if v1 == v2),
            Value::Float(v1) => match other {
                Value::Float(v2) => v1.is_nan() && v2.is_nan() || v1.to_bits() == v2.to_bits(),
                _ => false,
            },
            Value::Bytes(v1) => matches!(other, Value::Bytes(v2) if v1 == v2),
            Value::String(v1) => matches!(other, Value::String(v2) if v1 == v2),
            Value::List(v1) => matches!(other, Value::List(v2) if v1 == v2),
            Value::Map(v1) => matches!(other, Value::Map(v2) if v1 == v2),
            Value::Struct(v1) => matches!(other, Value::Struct(v2) if v1 == v2),
        }
    }
}

impl Eq for Value {}

macro_rules! impl_value_from_into {
    ( $value:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for Value {
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
            impl From<$ty> for Value {
                fn from(value: $ty) -> Self {
                    $value(value)
                }
            }
        )*
    };
}

impl_value_from_into!(Value::Boolean, bool);
impl_value_from_into!(Value::Integer, u8, u16, u32, i8, i16, i32, i64);
impl_value_from_into!(Value::Float, f32, f64);
impl_value_from_into!(Value::String, &str);
impl_value_from_into!(Value::Bytes, &[u8]);

impl_value_from_owned!(Value::String, String);
impl_value_from_owned!(Value::Struct, Struct);
// impl_value_from_owned!(Value::List, Vec<Value>);
// impl_value_from_owned!(Value::Map, HashMap<String, Value>);
impl<T: Into<Value>> From<IndexMap<String, T>> for Value {
    fn from(value: IndexMap<String, T>) -> Self {
        Value::Map(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(value: Vec<T>) -> Self {
        Value::List(value.into_iter().map(|v| v.into()).collect())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        match value {
            None => Value::Null,
            Some(v) => v.into(),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl TryFrom<Value> for bool {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Boolean(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Boolean(_))
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_bool(self) -> Result<bool, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for i64 {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Integer(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_int(&self) -> bool {
        matches!(self, Value::Integer(_))
    }

    #[inline]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_int(self) -> Result<i64, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for f64 {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    #[inline]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_float(self) -> Result<f64, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bytes(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_bytes(&self) -> bool {
        matches!(self, Value::Bytes(_))
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Value::Bytes(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_bytes(self) -> Result<Vec<u8>, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for String {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    #[inline]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_string(self) -> Result<String, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for Vec<Value> {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::List(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, Value::List(_))
    }

    #[inline]
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_list(self) -> Result<Vec<Value>, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for IndexMap<String, Value> {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Map(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, Value::Map(_))
    }

    #[inline]
    pub fn as_map(&self) -> Option<&IndexMap<String, Value>> {
        match self {
            Value::Map(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_map(self) -> Result<IndexMap<String, Value>, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for (u8, Vec<Value>) {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Struct(Struct { tag, fields }) => Ok((tag, fields)),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_struct(&self) -> bool {
        matches!(self, Value::Struct(_))
    }

    #[inline]
    pub fn as_struct(&self) -> Option<(u8, &Vec<Value>)> {
        match self {
            Value::Struct(Struct { tag, fields }) => Some((*tag, fields)),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_struct(self) -> Result<(u8, Vec<Value>), Self> {
        self.try_into()
    }
}

const TINY_STRING: u8 = 0x80;
const TINY_LIST: u8 = 0x90;
const TINY_MAP: u8 = 0xA0;
const TINY_STRUCT: u8 = 0xB0;
const NULL: u8 = 0xC0;
const FALSE: u8 = 0xC2;
const TRUE: u8 = 0xC3;
const INT_8: u8 = 0xC8;
const INT_16: u8 = 0xC9;
const INT_32: u8 = 0xCA;
const INT_64: u8 = 0xCB;
const FLOAT_64: u8 = 0xC1;
const STRING_8: u8 = 0xD0;
const STRING_16: u8 = 0xD1;
const STRING_32: u8 = 0xD2;
const LIST_8: u8 = 0xD4;
const LIST_16: u8 = 0xD5;
const LIST_32: u8 = 0xD6;
const MAP_8: u8 = 0xD8;
const MAP_16: u8 = 0xD9;
const MAP_32: u8 = 0xDA;
const BYTES_8: u8 = 0xCC;
const BYTES_16: u8 = 0xCD;
const BYTES_32: u8 = 0xCE;

struct PackStreamDecoder<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> PackStreamDecoder<'a> {
    fn new(bytes: &'a [u8], idx: usize) -> Self {
        Self { bytes, index: idx }
    }

    fn read(&mut self) -> Result<Value> {
        let marker = self.read_byte()?;
        self.read_value(marker)
    }

    fn read_value(&mut self, marker: u8) -> Result<Value> {
        let high_nibble = marker & 0xF0;

        Ok(match marker {
            // tiny int
            _ if marker as i8 >= -16 => marker.into(),
            NULL => Value::Null,
            FLOAT_64 => self.read_f64()?.into(),
            FALSE => false.into(),
            TRUE => true.into(),
            INT_8 => self.read_i8()?.into(),
            INT_16 => self.read_i16()?.into(),
            INT_32 => self.read_i32()?.into(),
            INT_64 => self.read_i64()?.into(),
            BYTES_8 => {
                let len = self.read_u8()?;
                self.read_bytes(len)?
            }
            BYTES_16 => {
                let len = self.read_u16()?;
                self.read_bytes(len)?
            }
            BYTES_32 => {
                let len = self.read_u32()?;
                self.read_bytes(len)?
            }
            _ if high_nibble == TINY_STRING => self.read_string((marker & 0x0F).into())?,
            STRING_8 => {
                let len = self.read_u8()?;
                self.read_string(len)?
            }
            STRING_16 => {
                let len = self.read_u16()?;
                self.read_string(len)?
            }
            STRING_32 => {
                let len = self.read_u32()?;
                self.read_string(len)?
            }
            _ if high_nibble == TINY_LIST => self.read_list((marker & 0x0F).into())?,
            LIST_8 => {
                let len = self.read_u8()?;
                self.read_list(len)?
            }
            LIST_16 => {
                let len = self.read_u16()?;
                self.read_list(len)?
            }
            LIST_32 => {
                let len = self.read_u32()?;
                self.read_list(len)?
            }
            _ if high_nibble == TINY_MAP => self.read_map((marker & 0x0F).into())?,
            MAP_8 => {
                let len = self.read_u8()?;
                self.read_map(len)?
            }
            MAP_16 => {
                let len = self.read_u16()?;
                self.read_map(len)?
            }
            MAP_32 => {
                let len = self.read_u32()?;
                self.read_map(len)?
            }
            _ if high_nibble == TINY_STRUCT => self.read_struct((marker & 0x0F).into())?,
            _ => {
                // raise ValueError("Unknown PackStream marker %02X" % marker)
                return Err(anyhow!("Unknown PackStream marker {:02X}", marker));
            }
        })
    }

    fn read_list(&mut self, length: usize) -> Result<Value> {
        let mut items = Vec::with_capacity(length);
        for _ in 0..length {
            items.push(self.read()?);
        }
        Ok(items.into())
    }

    fn read_string(&mut self, length: usize) -> Result<Value> {
        self.read_raw_string(length).map(Into::into)
    }

    fn read_map(&mut self, length: usize) -> Result<Value> {
        let mut key_value_pairs = IndexMap::with_capacity(length);
        for _ in 0..length {
            let len = self.read_string_length()?;
            let key = self.read_raw_string(len)?;
            let value = self.read()?;
            key_value_pairs.insert(key, value);
        }
        Ok(key_value_pairs.into())
    }

    fn read_bytes(&mut self, length: usize) -> Result<Value> {
        let data = self.bytes.get(self.index..self.index + length);
        self.index += length;
        Ok(data.into())
    }

    fn read_struct(&mut self, length: usize) -> Result<Value> {
        let tag = self.read_byte()?;
        let mut fields = Vec::with_capacity(length);
        for _ in 0..length {
            fields.push(self.read()?)
        }
        let bolt_struct = Value::Struct(Struct { tag, fields });
        Ok(bolt_struct)
    }

    fn read_string_length(&mut self) -> Result<usize> {
        let marker = self.read_byte()?;
        let high_nibble = marker & 0xF0;
        match high_nibble {
            TINY_STRING => Ok((marker & 0x0F) as usize),
            STRING_8 => self.read_u8(),
            STRING_16 => self.read_u16(),
            STRING_32 => self.read_u32(),
            _ => Err(anyhow!("Invalid string length marker: {}", marker)),
        }
    }

    fn read_byte(&mut self) -> Result<u8> {
        let byte = *self
            .bytes
            .get(self.index)
            .ok_or_else(|| anyhow!("Nothing to unpack"))?;
        self.index += 1;
        Ok(byte)
    }

    fn read_n_bytes<const N: usize>(&mut self) -> Result<[u8; N]> {
        let to = self.index + N;
        match self.bytes.get(self.index..to) {
            Some(b) => {
                self.index = to;
                Ok(<[u8; N]>::try_from(b).expect("we know the slice has exactly N values"))
            }
            None => Err(anyhow!("no me gusta")),
        }
    }

    fn read_u8(&mut self) -> Result<usize> {
        self.read_byte().map(Into::into)
    }

    fn read_u16(&mut self) -> Result<usize> {
        let data = self.read_n_bytes()?;
        Ok(u16::from_be_bytes(data).to_usize())
    }

    fn read_u32(&mut self) -> Result<usize> {
        let data = self.read_n_bytes()?;
        Ok(u32::from_be_bytes(data).to_usize())
    }

    fn read_i8(&mut self) -> Result<i8> {
        self.read_byte().map(|b| i8::from_be_bytes([b]))
    }

    fn read_i16(&mut self) -> Result<i16> {
        self.read_n_bytes().map(i16::from_be_bytes)
    }

    fn read_i32(&mut self) -> Result<i32> {
        self.read_n_bytes().map(i32::from_be_bytes)
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.read_n_bytes().map(i64::from_be_bytes)
    }

    fn read_f64(&mut self) -> Result<f64> {
        self.read_n_bytes().map(f64::from_be_bytes)
    }

    fn read_raw_string(&mut self, length: usize) -> Result<String> {
        if length == 0 {
            return Ok("".into());
        }
        let data = self
            .bytes
            .get(self.index..self.index + length)
            .ok_or(anyhow!("expected some bytes"))?;
        let parsed = String::from_utf8(data.into())?;
        self.index += length;
        Ok(parsed)
    }
}

pub struct PackStreamSerializer<'a> {
    data: &'a mut Vec<u8>,
}

impl<'a> PackStreamSerializer<'a> {
    pub fn new(writer: &'a mut Vec<u8>) -> Self {
        Self { data: writer }
    }

    pub fn write(&mut self, value: &Value) {
        match value {
            Value::Null => self.write_null(),
            Value::Boolean(b) => self.write_bool(*b),
            Value::Integer(i) => self.write_int(*i),
            Value::Float(f) => self.write_float(*f),
            Value::Bytes(b) => self.write_bytes(b),
            Value::String(s) => self.write_string(s),
            Value::List(l) => {
                self.write_list_header(u64::from_usize(l.len()));
                for value in l {
                    self.write(value);
                }
            }
            Value::Map(m) => {
                self.write_dict_header(u64::from_usize(m.len()));
                for (k, v) in m {
                    self.write_string(k);
                    self.write(v);
                }
            }
            Value::Struct(Struct { tag, fields }) => {
                self.write_struct_header(
                    *tag,
                    fields
                        .len()
                        .try_into()
                        .expect("Produced struct with too many fields"),
                );
                for value in fields {
                    self.write(value);
                }
            }
        }
    }

    #[inline]
    fn write_all(&mut self, data: &[u8]) {
        self.data.write_all(data).unwrap();
    }

    fn write_null(&mut self) {
        self.write_all(&[0xC0]);
    }

    fn write_bool(&mut self, b: bool) {
        self.write_all(match b {
            false => &[0xC2],
            true => &[0xC3],
        });
    }

    fn write_int(&mut self, i: i64) {
        if (-16..=127).contains(&i) {
            self.write_all(&i8::to_be_bytes(i as i8));
        } else if (-128..=127).contains(&i) {
            self.write_all(&[0xC8]);
            self.write_all(&i8::to_be_bytes(i as i8));
        } else if (-32_768..=32_767).contains(&i) {
            self.write_all(&[0xC9]);
            self.write_all(&i16::to_be_bytes(i as i16));
        } else if (-2_147_483_648..=2_147_483_647).contains(&i) {
            self.write_all(&[0xCA]);
            self.write_all(&i32::to_be_bytes(i as i32));
        } else {
            self.write_all(&[0xCB]);
            self.write_all(&i64::to_be_bytes(i));
        }
    }

    fn write_float(&mut self, f: f64) {
        self.write_all(&[0xC1]);
        self.write_all(&f64::to_be_bytes(f));
    }

    fn write_bytes(&mut self, b: &[u8]) {
        let size = b.len();
        if size <= 255 {
            self.write_all(&[0xCC]);
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xCD]);
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xCE]);
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("bytes exceed max size of 2,147,483,647");
        }
        self.write_all(b);
    }

    fn write_string(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let size = bytes.len();
        if size <= 15 {
            self.write_all(&[0x80 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD0]);
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD1]);
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xD2]);
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("string exceeds max size of 2,147,483,647 bytes");
        }
        self.write_all(bytes);
    }

    fn write_list_header(&mut self, size: u64) {
        if size <= 15 {
            self.write_all(&[0x90 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD4]);
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD5]);
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xD6]);
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("list exceeds max size of 2,147,483,647");
        }
    }

    fn write_dict_header(&mut self, size: u64) {
        if size <= 15 {
            self.write_all(&[0xA0 + size as u8]);
        } else if size <= 255 {
            self.write_all(&[0xD8]);
            self.write_all(&u8::to_be_bytes(size as u8));
        } else if size <= 65_535 {
            self.write_all(&[0xD9]);
            self.write_all(&u16::to_be_bytes(size as u16));
        } else if size <= 2_147_483_647 {
            self.write_all(&[0xDA]);
            self.write_all(&u32::to_be_bytes(size as u32));
        } else {
            panic!("map exceeds max size of 2,147,483,647");
        }
    }

    fn write_struct_header(&mut self, tag: u8, size: u8) {
        self.write_all(&[0xB0 + size, tag]);
    }
}

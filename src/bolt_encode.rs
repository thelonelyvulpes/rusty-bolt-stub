use crate::values::value::{Struct, Value};

pub fn read_point_2d(srid: i64, x: f64, y: f64) -> Struct {
    let tag = 0x58;
    let srid = Value::Integer(srid);
    let x = Value::Float(x);
    let y = Value::Float(y);
    let fields = vec![srid, x, y];
    Struct { tag, fields }
}

pub fn read_point_3d(srid: i64, x: f64, y: f64, z: f64) -> Struct {
    let tag = 0x59;
    let srid = Value::Integer(srid);
    let x = Value::Float(x);
    let y = Value::Float(y);
    let z = Value::Float(z);
    let fields = vec![srid, x, y, z];
    Struct { tag, fields }
}

use anyhow::anyhow;

pub(crate) fn str_to_data(byte_str: &str) -> anyhow::Result<Vec<u8>> {
    let clean = byte_str.replace(' ', "");
    if clean.len() % 2 != 0 {
        return Err(anyhow!("not right"));
    }
    let mut res = Vec::with_capacity(byte_str.len() / 2);
    for i in (0..clean.len()).step_by(2) {
        match u8::from_str_radix(&clean[i..i + 2], 16) {
            Ok(val) => {
                res.push(val);
                continue;
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }
    Ok(res)
}

#[cfg(test)]
mod test {
    use crate::str_byte::str_to_data;

    #[test]
    pub fn test() {
        let bytes = "FF 00 22FF ffa1";
        let bytes = str_to_data(bytes).unwrap();
        assert_eq!(vec![255, 0, 34, 255, 255, 161], bytes);
    }
}

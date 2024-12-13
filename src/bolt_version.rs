#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum BoltVersion {
    V3,
    V4_0,
    V4_1,
    V4_2,
    V4_3,
    V4_4,
    V5_0,
    V5_1,
    V5_2,
    V5_3,
    V5_4,
    V5_5,
}

impl BoltVersion {
    pub fn match_valid_version(major: u8, minor: &Option<u8>) -> Option<Self> {
        Some(match (major, minor) {
            (3, None) => BoltVersion::V3,
            (4, Some(x)) => match x {
                0 => BoltVersion::V4_0,
                1 => BoltVersion::V4_1,
                2 => BoltVersion::V4_2,
                3 => BoltVersion::V4_3,
                4 => BoltVersion::V4_4,
                _ => return None,
            },
            (5, Some(x)) => match x {
                0 => BoltVersion::V5_0,
                1 => BoltVersion::V5_1,
                2 => BoltVersion::V5_2,
                3 => BoltVersion::V5_3,
                4 => BoltVersion::V5_4,
                5 => BoltVersion::V5_5,
                _ => return None,
            },
            _ => return None,
        })
    }

    pub fn major(&self) -> u8 {
        match self {
            BoltVersion::V3 => 3,
            BoltVersion::V4_0 => 4,
            BoltVersion::V4_1 => 4,
            BoltVersion::V4_2 => 4,
            BoltVersion::V4_3 => 4,
            BoltVersion::V4_4 => 4,
            BoltVersion::V5_0 => 5,
            BoltVersion::V5_1 => 5,
            BoltVersion::V5_2 => 5,
            BoltVersion::V5_3 => 5,
            BoltVersion::V5_4 => 5,
            BoltVersion::V5_5 => 5,
        }
    }

    pub fn minor(&self) -> u8 {
        match self {
            BoltVersion::V3 => 0,
            BoltVersion::V4_0 => 0,
            BoltVersion::V4_1 => 1,
            BoltVersion::V4_2 => 2,
            BoltVersion::V4_3 => 3,
            BoltVersion::V4_4 => 4,
            BoltVersion::V5_0 => 0,
            BoltVersion::V5_1 => 1,
            BoltVersion::V5_2 => 2,
            BoltVersion::V5_3 => 3,
            BoltVersion::V5_4 => 4,
            BoltVersion::V5_5 => 5,
        }
    }
}

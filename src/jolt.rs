use anyhow::Result;
use nom_span::Spanned;

type Input<'a> = Spanned<&'a str>;

pub fn blah(input: &str) -> Result<()> {
    let spanned: Input = Spanned::new(input, false);

    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use rstest::rstest;
//     use crate::jolt::blah;
//
//     #[test]
//     fn test() {
//         let case = "{\"{}\": \"*\"}";
//
//         let result = blah(case);
//
//         assert!()
//     }
//
// }

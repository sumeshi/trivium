use std::cmp::Ordering;

/// Exact finite base-ten key. Keeping significant digits and their decimal
/// position avoids f64 rounding and never allocates zeroes for an exponent.
pub(super) struct ExactNumber {
    negative: bool,
    digits: String,
    position: i128,
}

impl ExactNumber {
    pub(super) fn parse(text: &str) -> Option<Self> {
        let (negative, unsigned) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(index) => {
                let exponent = unsigned[index + 1..].parse::<i128>().ok()?;
                (&unsigned[..index], exponent)
            }
            None => (unsigned, 0),
        };
        let mut before_point = None;
        let mut digits = String::with_capacity(mantissa.len());
        for byte in mantissa.bytes() {
            match byte {
                b'0'..=b'9' => digits.push(char::from(byte)),
                b'.' if before_point.is_none() => before_point = Some(digits.len()),
                _ => return None,
            }
        }
        if digits.is_empty() {
            return None;
        }
        let integer_digits = before_point.unwrap_or(digits.len());
        let leading_zeroes = digits.bytes().take_while(|byte| *byte == b'0').count();
        if leading_zeroes == digits.len() {
            return Some(Self {
                negative: false,
                digits: "0".into(),
                position: 0,
            });
        }
        let position = exponent
            .checked_add(integer_digits as i128)?
            .checked_sub(leading_zeroes as i128)?;
        digits.drain(..leading_zeroes);
        let significant = digits.trim_end_matches('0').len();
        digits.truncate(significant);
        Some(Self {
            negative,
            digits,
            position,
        })
    }

    pub(super) fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_parts(other.negative, &other.digits, other.position)
    }

    /// The common integer key stays inline. A mixed integer/decimal comparison
    /// needs only a stack buffer, never a temporary allocation or a float.
    pub(super) fn cmp_integer(&self, integer: i128) -> Ordering {
        let mut buffer = [b'0'; 39];
        let mut start = buffer.len();
        let mut magnitude = integer.unsigned_abs();
        loop {
            start -= 1;
            buffer[start] = b'0' + (magnitude % 10) as u8;
            magnitude /= 10;
            if magnitude == 0 {
                break;
            }
        }
        let digits = std::str::from_utf8(&buffer[start..]).expect("decimal digits are ASCII");
        let position = if integer == 0 {
            0
        } else {
            digits.len() as i128
        };
        let digits = if integer == 0 {
            "0"
        } else {
            digits.trim_end_matches('0')
        };
        self.cmp_parts(integer < 0, digits, position)
    }

    fn cmp_parts(&self, negative: bool, digits: &str, position: i128) -> Ordering {
        match self.negative.cmp(&negative) {
            Ordering::Less => return Ordering::Greater,
            Ordering::Greater => return Ordering::Less,
            Ordering::Equal => {}
        }
        let magnitude = match (self.digits == "0", digits == "0") {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self
                .position
                .cmp(&position)
                .then_with(|| self.digits.as_str().cmp(digits)),
        };
        if self.negative {
            magnitude.reverse()
        } else {
            magnitude
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equivalent_spellings_and_signed_zero_share_keys() {
        for (a, b) in [
            ("1", "1.000"),
            ("1000", "1e3"),
            (".0012", "12e-4"),
            ("-0001.2500", "-125e-2"),
            ("-0.000e1000", "+0e-1000"),
        ] {
            assert_eq!(
                ExactNumber::parse(a)
                    .unwrap()
                    .cmp(&ExactNumber::parse(b).unwrap()),
                Ordering::Equal,
                "{a} and {b}"
            );
        }
        for invalid in ["", ".", "NaN", "inf", "1.2.3", "1e", "1e2e3", "--1", "1 2"] {
            assert!(ExactNumber::parse(invalid).is_none(), "{invalid}");
        }
    }

    #[test]
    fn mixed_integer_comparison_matches_exact_numeric_keys() {
        for integer in [i128::MIN, -10_000, -1, 0, 1, 10_000, i128::MAX] {
            let exact_integer = ExactNumber::parse(&integer.to_string()).unwrap();
            for same in [format!("{integer}.0"), format!("{integer}0e-1")] {
                assert_eq!(
                    ExactNumber::parse(&same).unwrap().cmp_integer(integer),
                    Ordering::Equal,
                    "{same} equals {integer}"
                );
            }
            for text in [
                "-170141183460469231731687303715884105729",
                "-10000.000000000000000001",
                "-1.0",
                "-0.0000000000000000001",
                "0e100",
                "0.0000000000000000001",
                "1.000",
                "1e4",
                "170141183460469231731687303715884105728",
            ] {
                let number = ExactNumber::parse(text).unwrap();
                assert_eq!(number.cmp_integer(integer), number.cmp(&exact_integer));
            }
        }
    }
}

//! Keys whose bytes compare as the values they encode compare in Python.
//!
//! Push values into a byte vector. Values pushed one after another compare in order, as the items
//! of a tuple do, and [`begin_tuple`], [`begin_list`], and [`end_sequence`] nest them.
//!
//! ```
//! use spillsort::key;
//!
//! let encode = |refname: &str, start: i64| {
//!     let mut out = Vec::new();
//!     key::push_str(&mut out, refname);
//!     key::push_int(&mut out, start);
//!     out
//! };
//! assert!(encode("chr1", 900) < encode("chr1", 1000));
//! assert!(encode("chr1", 1000) < encode("chr2", 5));
//!
//! let (mut int, mut float) = (Vec::new(), Vec::new());
//! key::push_int(&mut int, 3);
//! key::push_float(&mut float, 3.0);
//! assert_eq!(int, float);
//! ```
//!
//! Each value starts with a tag byte, then:
//!
//! - A number, int or float, holds the greatest float not above it, then how much the int exceeds
//!   that float. Ints and floats of any size therefore compare exactly as Python compares them, so
//!   `1 == 1.0` and `2**53 + 1 > 2.0**53`.
//! - Text and bytes escape `0x00` as `0x00 0xFF` and end with `0x00 0x00`, so a prefix sorts first.
//!   UTF-8 text compares by code point, as Python compares strings.
//! - A tuple or list holds its values and ends with `0x00`, so it sorts before any longer one that
//!   it starts.
//!
//! Python refuses to compare values of different kinds, such as None and an int. Here they sort by
//! kind: None, then numbers, text, bytes, tuples, and lists. NaN, which Python finds neither less
//! nor greater than any number, sorts after every number.

const END: u8 = 0x00;
const NONE: u8 = 0x10;
const NUMBER: u8 = 0x20;
const STR: u8 = 0x30;
const BYTES: u8 = 0x40;
const TUPLE: u8 = 0x50;
const LIST: u8 = 0x60;

/// Follows the floor of an int below every finite float, before its complemented magnitude.
const BELOW_FLOATS: u8 = 0x01;

/// 2^127 as a float, the first float beyond `i128`.
const I128_END: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;

/// Push None.
pub fn push_none(out: &mut Vec<u8>) {
    out.push(NONE);
}

/// Push an int, which may be a bool, as Python's `True == 1`.
pub fn push_int(out: &mut Vec<u8>, value: impl Into<i128>) {
    let value = value.into();
    let (floor, excess) = int_floor(value);
    push_number(out, floor, &excess.to_be_bytes());
}

/// Push an int of any size, given its sign and its magnitude as big-endian bytes.
#[allow(clippy::cast_precision_loss)]
pub fn push_big_int(out: &mut Vec<u8>, negative: bool, magnitude: &[u8]) {
    let magnitude = without_leading_zeros(magnitude);
    if magnitude.len() <= 16 {
        let mut word = [0u8; 16];
        word[16 - magnitude.len()..].copy_from_slice(magnitude);
        let unsigned = u128::from_be_bytes(word);
        let value = if negative {
            0i128.checked_sub_unsigned(unsigned)
        } else {
            i128::try_from(unsigned).ok()
        };
        if let Some(value) = value {
            push_int(out, value);
            return;
        }
    }
    let bits = magnitude.len() * 8 - magnitude[0].leading_zeros() as usize;
    let shift = bits - 53;
    let top = shift_right(magnitude, shift);
    let low = low_bits(magnitude, shift);
    if !negative {
        if bits > 1024 {
            // Its magnitude outgrows any excess over the greatest float, so it orders these ints.
            push_number(out, f64::MAX, magnitude);
        } else {
            push_number(out, top as f64 * power_of_two(shift), &low);
        }
        return;
    }
    let (ceiling, excess) = if low.iter().all(|&byte| byte == 0) {
        (top, Vec::new())
    } else {
        (top + 1, complement_below(&low, shift))
    };
    if bits > 1024 || (bits == 1024 && ceiling == 1 << 53) {
        push_below_floats(out, magnitude);
    } else {
        push_number(out, -(ceiling as f64) * power_of_two(shift), &excess);
    }
}

/// Push a float, with -0.0 as 0.0, since Python finds them equal, and NaN after every number.
pub fn push_float(out: &mut Vec<u8>, value: f64) {
    let value = if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    };
    push_number(out, value, &[]);
}

/// Push text, which compares by code point.
pub fn push_str(out: &mut Vec<u8>, value: &str) {
    push_escaped(out, STR, value.as_bytes());
}

/// Push text as UTF-8 that may also encode lone surrogates, as Python's `surrogatepass` does.
#[cfg(feature = "python")]
pub(crate) fn push_utf8(out: &mut Vec<u8>, value: &[u8]) {
    push_escaped(out, STR, value);
}

/// Push bytes, which compare byte by byte.
pub fn push_bytes(out: &mut Vec<u8>, value: &[u8]) {
    push_escaped(out, BYTES, value);
}

/// Begin a tuple, whose values are pushed next and which [`end_sequence`] ends.
pub fn begin_tuple(out: &mut Vec<u8>) {
    out.push(TUPLE);
}

/// Begin a list, whose values are pushed next and which [`end_sequence`] ends.
pub fn begin_list(out: &mut Vec<u8>) {
    out.push(LIST);
}

/// End the innermost tuple or list.
pub fn end_sequence(out: &mut Vec<u8>) {
    out.push(END);
}

/// Map a float's bits to an unsigned integer that orders as the floats do.
fn ordered(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits | 1 << 63
    }
}

fn push_number(out: &mut Vec<u8>, floor: f64, excess: &[u8]) {
    out.push(NUMBER);
    out.extend_from_slice(&ordered(floor).to_be_bytes());
    push_unsigned(out, excess);
}

/// Push an int below every finite float: after negative infinity, so that it sorts above it, and
/// with its magnitude complemented, so that a larger magnitude sorts first.
fn push_below_floats(out: &mut Vec<u8>, magnitude: &[u8]) {
    out.push(NUMBER);
    out.extend_from_slice(&ordered(f64::NEG_INFINITY).to_be_bytes());
    out.push(BELOW_FLOATS);
    let start = out.len();
    push_unsigned(out, magnitude);
    for byte in &mut out[start..] {
        *byte = !*byte;
    }
}

/// Push a non-negative integer, given big-endian, as its length and then its digits, so that its
/// bytes compare as the integer does.
fn push_unsigned(out: &mut Vec<u8>, big_endian: &[u8]) {
    let digits = without_leading_zeros(big_endian);
    match u8::try_from(digits.len()) {
        Ok(length) if length < u8::MAX => out.push(length),
        _ => {
            out.push(u8::MAX);
            out.extend_from_slice(&(digits.len() as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(digits);
}

fn push_escaped(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push(tag);
    for (index, part) in bytes.split(|&byte| byte == 0).enumerate() {
        if index > 0 {
            out.extend_from_slice(&[0x00, 0xFF]);
        }
        out.extend_from_slice(part);
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

/// The greatest float not above an int, and how much the int exceeds it.
#[allow(clippy::cast_precision_loss)]
fn int_floor(value: i128) -> (f64, u128) {
    let mut floor = value as f64;
    if floor >= I128_END || floor as i128 > value {
        floor = floor.next_down();
    }
    (floor, value.abs_diff(floor as i128))
}

/// 2 to a power from 0 to 1023.
fn power_of_two(exponent: usize) -> f64 {
    f64::from_bits((exponent as u64 + 1023) << 52)
}

fn without_leading_zeros(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|&byte| byte != 0)
        .unwrap_or(bytes.len());
    &bytes[start..]
}

/// The bits of a big-endian magnitude above `shift`, at most 64 of them.
fn shift_right(magnitude: &[u8], shift: usize) -> u64 {
    let kept = &magnitude[..magnitude.len() - shift / 8];
    let value = kept
        .iter()
        .fold(0u128, |value, &byte| value << 8 | u128::from(byte));
    (value >> (shift % 8)) as u64
}

/// The low `shift` bits of a big-endian magnitude, big-endian.
fn low_bits(magnitude: &[u8], shift: usize) -> Vec<u8> {
    let mut low = magnitude[magnitude.len() - shift.div_ceil(8)..].to_vec();
    if !shift.is_multiple_of(8) {
        low[0] &= (1 << (shift % 8)) - 1;
    }
    low
}

/// 2^`shift` minus a big-endian integer between 1 and 2^`shift` - 1.
fn complement_below(low: &[u8], shift: usize) -> Vec<u8> {
    let mut out: Vec<u8> = low.iter().map(|&byte| !byte).collect();
    for byte in out.iter_mut().rev() {
        let (sum, carry) = byte.overflowing_add(1);
        *byte = sum;
        if !carry {
            break;
        }
    }
    if !shift.is_multiple_of(8) {
        out[0] &= (1 << (shift % 8)) - 1;
    }
    out
}

#[cfg(test)]
#[allow(clippy::cast_precision_loss)]
mod tests {
    use std::cmp::Ordering::{self, Equal, Greater, Less};

    use proptest::prelude::*;

    use super::*;

    fn int(value: i128) -> Vec<u8> {
        let mut out = Vec::new();
        push_int(&mut out, value);
        out
    }

    fn float(value: f64) -> Vec<u8> {
        let mut out = Vec::new();
        push_float(&mut out, value);
        out
    }

    fn big(negative: bool, magnitude: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        push_big_int(&mut out, negative, magnitude);
        out
    }

    /// The exact value of an integral float, as a sign and big-endian magnitude.
    fn float_as_big(value: f64) -> (bool, Vec<u8>) {
        let bits = value.to_bits();
        let exponent = ((bits >> 52) & 0x7FF) as usize;
        let mantissa = (bits & ((1 << 52) - 1)) | 1 << 52;
        assert!(exponent >= 1075, "{value} is not a large integral float");
        let shift = exponent - 1075;
        let mut magnitude = vec![0u8];
        magnitude.extend_from_slice(&mantissa.to_be_bytes());
        magnitude.extend(std::iter::repeat_n(0u8, shift / 8));
        let mut carry = 0u8;
        for byte in magnitude.iter_mut().rev() {
            let shifted = u16::from(*byte) << (shift % 8) | u16::from(carry);
            *byte = shifted as u8;
            carry = (shifted >> 8) as u8;
        }
        (value < 0.0, magnitude)
    }

    fn compare_big(a: (bool, &[u8]), b: (bool, &[u8])) -> Ordering {
        let (a_digits, b_digits) = (without_leading_zeros(a.1), without_leading_zeros(b.1));
        let a_negative = a.0 && !a_digits.is_empty();
        let b_negative = b.0 && !b_digits.is_empty();
        let magnitudes = a_digits
            .len()
            .cmp(&b_digits.len())
            .then_with(|| a_digits.cmp(b_digits));
        match (a_negative, b_negative) {
            (false, false) => magnitudes,
            (true, true) => magnitudes.reverse(),
            (true, false) => Less,
            (false, true) => Greater,
        }
    }

    /// How Python orders an int that does not fit in `i128` against a float that is not NaN.
    fn compare_big_float(negative: bool, magnitude: &[u8], value: f64) -> Ordering {
        if value.is_infinite() {
            return if value > 0.0 { Less } else { Greater };
        }
        if value.abs() < 2f64.powi(64) {
            return if negative { Less } else { Greater };
        }
        let (float_negative, float_magnitude) = float_as_big(value);
        compare_big((negative, magnitude), (float_negative, &float_magnitude))
    }

    /// How Python orders an int against a float that is not NaN.
    fn compare_int_float(a: i128, b: f64) -> Ordering {
        if b >= I128_END {
            return Less;
        }
        if b < -I128_END {
            return Greater;
        }
        let floor = b.floor();
        match a.cmp(&(floor as i128)) {
            Equal if b > floor => Less,
            order => order,
        }
    }

    fn magnitude_strategy() -> impl Strategy<Value = Vec<u8>> {
        let ones = |bytes: usize| vec![0xFF; bytes];
        let max_float = float_as_big(f64::MAX).1;
        let edges = vec![
            ones(16),
            ones(128),
            ones(129),
            max_float.clone(),
            [vec![1], vec![0; 16]].concat(),
            [vec![1], vec![0; 128]].concat(),
        ];
        prop_oneof![
            proptest::collection::vec(any::<u8>(), 17..140),
            (proptest::sample::select(edges), any::<u8>(), any::<bool>()).prop_map(
                |(mut edge, delta, up)| {
                    let last = edge.len() - 1;
                    edge[last] = if up {
                        edge[last].saturating_add(delta)
                    } else {
                        edge[last].saturating_sub(delta)
                    };
                    edge
                }
            ),
        ]
    }

    fn integral_float_strategy() -> impl Strategy<Value = f64> {
        prop_oneof![
            (any::<u64>(), 64usize..1023, any::<bool>()).prop_map(
                |(mantissa, exponent, negative)| {
                    let value = (mantissa | 1 << 63) as f64 * power_of_two(exponent - 63);
                    if negative { -value } else { value }
                }
            ),
            Just(f64::MAX),
            Just(-f64::MAX),
            Just(f64::INFINITY),
            Just(f64::NEG_INFINITY),
            any::<f64>().prop_filter("not NaN", |value| !value.is_nan()),
        ]
    }

    fn tuple(key: &(i64, String, f64, Vec<u8>)) -> Vec<u8> {
        let mut out = Vec::new();
        begin_tuple(&mut out);
        push_int(&mut out, key.0);
        push_str(&mut out, &key.1);
        push_float(&mut out, key.2);
        push_bytes(&mut out, &key.3);
        end_sequence(&mut out);
        out
    }

    proptest! {
        #[test]
        fn tuples_order_as_in_python(
            a in (any::<i64>(), "\\PC{0,4}", -1e9f64..1e9, proptest::collection::vec(0u8..3, 0..4)),
            b in (any::<i64>(), "\\PC{0,4}", -1e9f64..1e9, proptest::collection::vec(0u8..3, 0..4)),
        ) {
            let python = a.0.cmp(&b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.partial_cmp(&b.2).unwrap())
                .then_with(|| a.3.cmp(&b.3));
            prop_assert_eq!(tuple(&a).cmp(&tuple(&b)), python);
        }

        #[test]
        fn ints_and_floats_compare_exactly(
            a in prop_oneof![any::<i128>(), any::<i64>().prop_map(i128::from), -(1i128 << 60)..(1i128 << 60)],
            b in prop_oneof![
                any::<f64>().prop_filter("not NaN", |value| !value.is_nan()),
                -1e40f64..1e40,
                (-(1i64 << 60)..(1i64 << 60)).prop_map(|value| value as f64),
            ],
        ) {
            prop_assert_eq!(int(a).cmp(&float(b)), compare_int_float(a, b));
        }

        #[test]
        fn ints_order_as_numbers(a in any::<i128>(), b in any::<i128>()) {
            prop_assert_eq!(int(a).cmp(&int(b)), a.cmp(&b));
        }

        #[test]
        fn big_ints_that_fit_encode_as_small_ints(value in any::<i128>()) {
            let magnitude = value.unsigned_abs().to_be_bytes();
            prop_assert_eq!(big(value < 0, &magnitude), int(value));
        }

        #[test]
        fn big_ints_order_as_numbers(
            a in (any::<bool>(), magnitude_strategy()),
            b in (any::<bool>(), magnitude_strategy()),
        ) {
            prop_assert_eq!(big(a.0, &a.1).cmp(&big(b.0, &b.1)), compare_big((a.0, &a.1), (b.0, &b.1)));
        }

        #[test]
        fn big_ints_and_floats_compare_exactly(
            a in (any::<bool>(), magnitude_strategy()),
            b in integral_float_strategy(),
        ) {
            prop_assume!(without_leading_zeros(&a.1).len() > 16);
            prop_assert_eq!(big(a.0, &a.1).cmp(&float(b)), compare_big_float(a.0, &a.1, b));
        }

        #[test]
        fn big_ints_and_ints_compare_exactly(a in (any::<bool>(), magnitude_strategy()), b in any::<i128>()) {
            let b_magnitude = b.unsigned_abs().to_be_bytes();
            prop_assert_eq!(big(a.0, &a.1).cmp(&int(b)), compare_big((a.0, &a.1), (b < 0, &b_magnitude)));
        }

        #[test]
        fn bytes_order_as_bytes(a in proptest::collection::vec(0u8..3, 0..6), b in proptest::collection::vec(0u8..3, 0..6)) {
            let (mut left, mut right) = (Vec::new(), Vec::new());
            push_bytes(&mut left, &a);
            push_bytes(&mut right, &b);
            prop_assert_eq!(left.cmp(&right), a.cmp(&b));
        }
    }

    #[test]
    fn equal_ints_and_floats_encode_equally() {
        for (a, b) in [
            (1, 1.0),
            (0, -0.0),
            (-7, -7.0),
            (1 << 60, 1_152_921_504_606_846_976.0),
            (i128::MIN, -I128_END),
        ] {
            assert_eq!(int(a), float(b));
        }
        assert!(int(i128::from(i64::MAX)) > float(9_223_372_036_854_774_784.0));
        assert!(int(i128::MAX) < float(I128_END));
        assert_eq!(big(false, &float_as_big(f64::MAX).1), float(f64::MAX));
        assert_eq!(big(true, &float_as_big(f64::MAX).1), float(-f64::MAX));
    }

    #[test]
    fn ints_beyond_floats_sit_between_the_largest_float_and_infinity() {
        let beyond = [vec![1], vec![0; 128]].concat();
        let (mut just_above, mut far_above) = (float_as_big(f64::MAX).1, vec![0xFF; 300]);
        let last = just_above.len() - 1;
        just_above[last] += 1;
        far_above[0] = 0x7F;
        assert!(float(f64::MAX) < big(false, &just_above));
        assert!(big(false, &just_above) < big(false, &beyond));
        assert!(big(false, &beyond) < big(false, &far_above));
        assert!(big(false, &far_above) < float(f64::INFINITY));
        assert!(float(f64::NEG_INFINITY) < big(true, &far_above));
        assert!(big(true, &far_above) < big(true, &beyond));
        assert!(big(true, &beyond) < big(true, &just_above));
        assert!(big(true, &just_above) < float(-f64::MAX));
    }

    #[test]
    fn nan_sorts_after_every_number_and_encodes_one_way() {
        assert!(float(f64::INFINITY) < float(f64::NAN));
        assert!(big(false, &[0xFF; 300]) < float(f64::NAN));
        assert_eq!(float(f64::NAN), float(-f64::NAN));
        let mut none = Vec::new();
        push_none(&mut none);
        assert!(none < float(f64::NEG_INFINITY));
    }

    #[test]
    fn a_shorter_sequence_and_a_prefix_sort_first() {
        let mut short = Vec::new();
        begin_tuple(&mut short);
        push_int(&mut short, 1);
        end_sequence(&mut short);
        let mut long = Vec::new();
        begin_tuple(&mut long);
        push_int(&mut long, 1);
        push_none(&mut long);
        end_sequence(&mut long);
        assert!(short < long);
        let (mut prefix, mut text) = (Vec::new(), Vec::new());
        push_str(&mut prefix, "a");
        push_str(&mut text, "a\0");
        assert!(prefix < text);
    }

    #[test]
    fn kinds_python_cannot_compare_sort_by_kind() {
        let mut keys = vec![Vec::new(); 6];
        push_none(&mut keys[0]);
        push_int(&mut keys[1], i64::MAX);
        push_str(&mut keys[2], "");
        push_bytes(&mut keys[3], b"");
        begin_tuple(&mut keys[4]);
        end_sequence(&mut keys[4]);
        begin_list(&mut keys[5]);
        end_sequence(&mut keys[5]);
        assert!(keys.is_sorted());
    }
}

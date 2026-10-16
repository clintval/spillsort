#![no_main]

use std::cmp::Ordering::{self, Equal, Greater, Less};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spillsort::key;

#[derive(Arbitrary, Debug)]
enum Value {
    None,
    Int(i128),
    UInt(u128),
    BigInt(bool, Vec<u8>),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Tuple(Vec<Value>),
    List(Vec<Value>),
}

fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::None => key::push_none(out),
        Value::Int(int) => key::push_int(out, *int),
        Value::UInt(uint) => key::push_uint(out, *uint),
        Value::BigInt(negative, magnitude) => key::push_big_int(out, *negative, magnitude),
        Value::Float(float) => key::push_float(out, *float),
        Value::Str(text) => key::push_str(out, text),
        Value::Bytes(bytes) => key::push_bytes(out, bytes),
        Value::Tuple(items) | Value::List(items) => {
            if matches!(value, Value::Tuple(_)) {
                key::begin_tuple(out);
            } else {
                key::begin_list(out);
            }
            for item in items {
                encode(item, out);
            }
            key::end_sequence(out);
        }
    }
}

/// An exact number: an int as its sign and big-endian magnitude, or a float that is not NaN.
enum Number {
    Int(bool, Vec<u8>),
    Float(f64),
}

fn number(value: &Value) -> Option<Number> {
    match value {
        Value::Int(int) => Some(Number::Int(
            *int < 0,
            int.unsigned_abs().to_be_bytes().to_vec(),
        )),
        Value::UInt(uint) => Some(Number::Int(false, uint.to_be_bytes().to_vec())),
        Value::BigInt(negative, magnitude) => Some(Number::Int(*negative, magnitude.clone())),
        Value::Float(float) => Some(Number::Float(*float)),
        _ => None,
    }
}

fn digits(magnitude: &[u8]) -> &[u8] {
    let start = magnitude
        .iter()
        .position(|&byte| byte != 0)
        .unwrap_or(magnitude.len());
    &magnitude[start..]
}

fn compare_ints(a: (bool, &[u8]), b: (bool, &[u8])) -> Ordering {
    let (a_digits, b_digits) = (digits(a.1), digits(b.1));
    let (a_negative, b_negative) = (a.0 && !a_digits.is_empty(), b.0 && !b_digits.is_empty());
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

/// The exact value of a finite, integral float.
fn integral(value: f64) -> (bool, Vec<u8>) {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7FF) as i64;
    if exponent < 1075 {
        return (value < 0.0, (value.abs() as u64).to_be_bytes().to_vec());
    }
    let shift = (exponent - 1075) as usize;
    let mantissa = (bits & ((1 << 52) - 1)) | 1 << 52;
    let mut magnitude = vec![0u8];
    magnitude.extend_from_slice(&mantissa.to_be_bytes());
    magnitude.extend(std::iter::repeat_n(0u8, shift / 8));
    let mut carry = 0u16;
    for byte in magnitude.iter_mut().rev() {
        let shifted = u16::from(*byte) << (shift % 8) | carry;
        *byte = shifted as u8;
        carry = shifted >> 8;
    }
    (value < 0.0, magnitude)
}

fn compare_numbers(a: &Number, b: &Number) -> Option<Ordering> {
    match (a, b) {
        (Number::Float(a), Number::Float(b)) => a.partial_cmp(b),
        (Number::Int(negative, magnitude), Number::Float(float)) => {
            compare_int_float(*negative, magnitude, *float)
        }
        (Number::Float(float), Number::Int(negative, magnitude)) => {
            compare_int_float(*negative, magnitude, *float).map(Ordering::reverse)
        }
        (Number::Int(a_negative, a), Number::Int(b_negative, b)) => {
            Some(compare_ints((*a_negative, a), (*b_negative, b)))
        }
    }
}

fn compare_int_float(negative: bool, magnitude: &[u8], float: f64) -> Option<Ordering> {
    if float.is_nan() {
        return None;
    }
    if float.is_infinite() {
        return Some(if float > 0.0 { Less } else { Greater });
    }
    let floor = float.floor();
    let (floor_negative, floor_magnitude) = integral(floor);
    Some(
        match compare_ints((negative, magnitude), (floor_negative, &floor_magnitude)) {
            Equal if float > floor => Less,
            order => order,
        },
    )
}

/// How Python orders two values, or None where Python refuses to compare them.
fn python_order(a: &Value, b: &Value) -> Option<Ordering> {
    if let (Some(a), Some(b)) = (number(a), number(b)) {
        return compare_numbers(&a, &b);
    }
    match (a, b) {
        (Value::None, Value::None) => Some(Equal),
        (Value::Str(a), Value::Str(b)) => Some(a.cmp(b)),
        (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
        (Value::Tuple(a), Value::Tuple(b)) | (Value::List(a), Value::List(b)) => {
            for (a, b) in a.iter().zip(b) {
                match python_order(a, b) {
                    Some(Equal) => {}
                    order => return order,
                }
            }
            Some(a.len().cmp(&b.len()))
        }
        _ => None,
    }
}

fuzz_target!(|values: (Value, Value)| {
    let (a, b) = values;
    let (mut a_key, mut b_key) = (Vec::new(), Vec::new());
    encode(&a, &mut a_key);
    encode(&b, &mut b_key);
    if let Some(order) = python_order(&a, &b) {
        assert_eq!(a_key.cmp(&b_key), order, "{a:?} against {b:?}");
    }
});

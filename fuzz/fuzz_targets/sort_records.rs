#![no_main]

use libfuzzer_sys::fuzz_target;
use spillsort::{Config, Sorter};

type Records = Vec<(Vec<u8>, Vec<u8>)>;

fuzz_target!(|input: (u16, u8, Records)| {
    let (max_bytes, max_open, records) = input;
    let config = Config::default()
        .max_bytes(usize::from(max_bytes) + 2)
        .max_open(usize::from(max_open % 8) + 2);
    let mut sorter = Sorter::new(config).expect("valid settings");
    for (key, value) in &records {
        sorter.push(key, value).expect("a record is pushed");
    }
    let mut expected = records;
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    let merged = sorter.finish().expect("the records are merged");
    assert_eq!(merged.len(), expected.len() as u64);
    let sorted: Vec<_> = merged
        .map(|record| record.expect("a record is read"))
        .collect();
    assert_eq!(sorted, expected);
});

#![allow(dead_code)]

use std::sync::mpsc;
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn spsc(count: usize) -> usize {
    let (px, cx) = spsc::channel();

    thread::spawn(move || {
        for i in 0..count {
            px.send(i).unwrap();
        }
    });

    thread::spawn(move || {
        let mut sum = 0usize;

        while let Ok(i) = cx.recv() {
            sum += i;
        }

        sum
    })
    .join()
    .unwrap()
}

fn mpsc(count: usize) -> usize {
    let (sx, rx) = mpsc::channel();

    thread::spawn(move || {
        for i in 0..count {
            sx.send(i).unwrap();
        }
    });

    thread::spawn(move || {
        let mut sum = 0usize;

        while let Ok(i) = rx.recv() {
            sum += i;
        }

        sum
    })
    .join()
    .unwrap()
}

fn spsc_vs_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc vs mpsc");

    for ref i in (8..=12).map(|n| 1 << n) {
        group.bench_with_input(BenchmarkId::new("spsc", i), i, |b, &i| b.iter(|| spsc(i)));
        group.bench_with_input(BenchmarkId::new("mpsc", i), i, |b, &i| b.iter(|| mpsc(i)));
    }

    group.finish();
}

criterion_group!(benches, spsc_vs_mpsc,);
criterion_main!(benches);

extern crate timely;

use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::{Inspect, Map, ToStream};

fn main() {
    // calculate sum of even(0,2,4,6,8) and sum of odd(1,3,5,7,9)
    // Generic intra-timestamp aggregation
    timely::example(|scope| {
        (0..10)
            .to_stream(scope)
            .map(|x| (x % 2, x))
            .aggregate(
                |_key, val, agg| {
                    *agg += val;
                },
                |key, agg: i32| (key, agg),
                |key| *key as u64,
            )
            .inspect(|x| assert!(*x == (0, 20) || *x == (1, 25)));
    });
}

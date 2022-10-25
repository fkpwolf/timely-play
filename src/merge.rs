use timely::dataflow::operators::aggregation::StateMachine;
use timely::dataflow::operators::{Exchange, Input, Inspect, Map, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

fn main() {
    // inter-timestamp aggregation
    // currently only support 2 processes. Hang when use 4 processes
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|x| *x)
                .map(|x| (x % 3, x))
                .state_machine(
                    // hard code type "&mut u64"
                    |_key, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((*_key, *agg)))
                    },
                    |key| *key as u64,
                )
                .inspect(move |x| println!("worker {}:\thello {}|{}", index, x.0, x.1))
                .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            if index == 0 {
                input.send(round);
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step(); // to make sure data has been consumed
                println!("step {}!", round);
            }
        }
    })
    .unwrap();
}

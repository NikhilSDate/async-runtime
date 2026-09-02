use runtime::config::Config;
use runtime::executor::Runtime;

#[test]
#[should_panic]
fn block_on_panics_if_its_future_panics_before_sending_a_result() {
    let runtime = Runtime::new(Config::default());
    runtime.block_on(async {
        panic!("deliberate panic before block_on's channel send");
    });
}

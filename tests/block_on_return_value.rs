use runtime::config::Config;
use runtime::executor::Runtime;
use runtime::spawn;

#[test]
fn block_on_returns_value_from_simple_future_and_nested_spawn() {
    let runtime = Runtime::new(Config::default());

    let result = runtime.block_on(async {
        let simple = 40 + 2;

        let handle = spawn(async { 100 });
        let child = handle.await;

        simple + child
    });

    assert_eq!(result, 142);
}

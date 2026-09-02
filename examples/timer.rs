use std::time::Duration;

use runtime::config::Config;
use runtime::executor::Runtime;
use runtime::spawn;
use runtime::timer::Sleep;

fn main() {
    let runtime = Runtime::new(Config::default());
    runtime.block_on(async {
        println!("tick 1");
        let handle = spawn(async {
            Sleep::new(Duration::from_millis(1000));
            println!("child task");
        });
        Sleep::new(Duration::from_millis(500)).await;
        println!("tick 2");
        Sleep::new(Duration::from_millis(500)).await;
        println!("tick 3");
        handle.await;
    });
}

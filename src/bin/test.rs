use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

async fn hello() {
    println!("hello world");
}
fn kity() {
    println!("hello kity");
}
#[tokio::main]
async fn main() {
    hello().await;
    kity();
}

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    // Create a channel with a buffer size of 2
    let (tx, mut rx) = mpsc::channel(2);

    // Spawn 4 senders (simulating them sending messages)
    for i in 0..4 {
        let tx = tx.clone();
        tokio::spawn(async move {
            // Simulate work before sending a message
            sleep(Duration::from_millis(100)).await;
            tx.send(i).await.unwrap(); // Send the message
            println!("Sender {} sent message", i);
        });
    }
    drop(tx);
    // Create a new task to handle receiving messages
    let receive_task = tokio::spawn(async move {
        let mut received = vec![];
        let mut counter = 0;

        // Receive messages in a separate task
        while let Some(msg) = rx.recv().await {
            println!("Received message: {}", msg);
            received.push(msg);
            counter += 1;

  
        }

        // Optionally return the received messages to the main thread
        received
    });

    // Wait for the receive task to finish and get the result
    let received_messages = receive_task.await.unwrap();

    // Print all the received messages
    println!("All received messages: {:?}", received_messages);
}

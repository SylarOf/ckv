use rand::{seq::IteratorRandom, Rng};
use rand::seq::SliceRandom;
use std::time::{SystemTime, UNIX_EPOCH};

fn rand_str(length: usize) -> String {
    // Define the characters to choose from (including special characters and emojis)
    let chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|©®😁😭🉑️🐂㎡我爱吃鱼";
    
    // Convert the string into a slice of chars for random selection
    let mut rng = rand::thread_rng();
    
    // Generate the random string by selecting random characters
    let result: String = (0..length)
        .map(|_| {
            let random_char = chars.chars().choose(&mut rng).unwrap(); // Choose a random character
            random_char
        })
        .collect();
    
    result
}

fn main() {
    loop {
     let random_string = rand_str(10);
    println!("Random String: {}", random_string);

       
    }
}

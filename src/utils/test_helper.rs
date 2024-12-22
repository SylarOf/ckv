use rand::{seq::IteratorRandom, Rng};
use std::fmt;

pub fn generate_incredible_strings(num: usize) -> Vec<String> {
    // Define the alphabet
    let alphabet = "abcdefghijklmnopqrstuvwxyz";

    // Starting string is "abc"
    let mut current_chars = vec!['a', 'b', 'c'];

    let mut result_vec = Vec::new(); // Vec to hold the generated strings

    for _ in 0..num {
        // Generate the current string
        let mut result = String::new();
        for &ch in &current_chars {
            result.push(ch);
        }
        result_vec.push(result); // Add the string to the result vector

        // Increment the last character, handling overflow
        let mut carry = true;
        for i in (0..current_chars.len()).rev() {
            if carry {
                if current_chars[i] == 'z' {
                    current_chars[i] = 'a'; // Reset to 'a' if 'z' is reached
                } else {
                    current_chars[i] = (current_chars[i] as u8 + 1) as char; // Increment character
                    carry = false; // Stop carry over
                }
            }
        }
    }

    result_vec // Return the vector of generated strings
}

pub fn display(s: &Vec<u8>) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(s.clone())
}

pub fn work_dir_clear(dir: &str) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        std::fs::remove_file(path)?
    }
    Ok(())
}
pub fn rand_str(length: usize) -> String {
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
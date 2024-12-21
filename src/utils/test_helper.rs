use rand::Rng;
use std::fmt;

pub fn rand_str(length: usize) -> String {
    let str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|©®😁😭🉑️🐂㎡硬核课堂";
    let chars: Vec<char> = str.chars().collect(); // Convert to a Vec<char> for direct UTF-8 handling

    let mut rng = rand::thread_rng(); // No need to manually seed

    let mut result = String::with_capacity(length);
    for _ in 0..length {
        let random_index = rng.gen_range(0..chars.len());
        result.push(chars[random_index]);
    }

    result
}
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

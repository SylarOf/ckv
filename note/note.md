    
    // borrow and mut borrow can not live together
    pub fn seek(&mut self, key : &Slice)->Option<Slice>{
        // use borrow
        let found_idx = self.entry_offsets.binary_search_by(|i|{
            // use mut borrow
            self.set_idx(*i as i32);
            self.key.cmp(key)
        });
        if let Ok(i) = found_idx{
            self.set_idx(i as i32);
            Some(self.key.clone())
        } 
        else{
            None
        }

    } 


    // use Option to take if you want to move ownership of a member
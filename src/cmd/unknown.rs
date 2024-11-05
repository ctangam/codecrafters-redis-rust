pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub fn new(command_name: impl ToString) -> Self {
        Self {
            command_name: command_name.to_string(),
        }
    }
}
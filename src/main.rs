use anyhow::{Ok, Result};
use mesin::{context::ExecutionContext, get_dialect};

fn main() -> Result<()> {
    let dialect = get_dialect("postgres");
    let context = ExecutionContext::new(dialect);
    let result = context.execute("SELECT email, name FROM users").unwrap();
    



    Ok(())
}

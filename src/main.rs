use mesin::{context::ExecutionContext, get_dialect};

fn main() {
    let dialect = get_dialect("postgres");
    let context = ExecutionContext::new(dialect);
    context.run("SELECT name, salary FROM users where name = ilzam");
    context.run("SELECT * FROM transactions");
}

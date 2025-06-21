use anyhow::{bail, Result};
use sqlparser::ast::{ObjectName, ObjectNamePart};

pub fn extract_table_name(name: &ObjectName) -> Result<String> {
    if let Some(ObjectNamePart::Identifier(ident)) = name.0.first() {
        Ok(ident.value.clone())
    } else {
        bail!("Invalid table name")
    }
}

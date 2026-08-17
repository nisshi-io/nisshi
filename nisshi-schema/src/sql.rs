// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::Result;
use arrow::datatypes::DataType;
use datafusion::sql::sqlparser::{
    ast::{DataType as SqlDataType, Expr},
    dialect::GenericDialect,
    parser::Parser,
};
use tracing::debug;

pub(crate) fn typeof_sql_expr(expr: &str) -> Result<DataType> {
    let dialect = GenericDialect {};
    match Parser::new(&dialect)
        .try_with_sql(expr)?
        .parse_expr()
        .inspect(|ast| debug!(?ast))?
    {
        Expr::Cast { data_type, .. } => delta_sql_type(data_type),

        otherwise => todo!("{otherwise:?}"),
    }
}

fn delta_sql_type(data_type: SqlDataType) -> Result<DataType> {
    match data_type {
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int32),

        otherwise => todo!("{otherwise:?}"),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::Error;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn simple_cast() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(
            typeof_sql_expr("cast(meta.timestamp as date)")?,
            DataType::Date32
        );

        Ok(())
    }
}

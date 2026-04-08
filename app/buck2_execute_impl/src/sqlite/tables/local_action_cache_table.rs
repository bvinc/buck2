/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Arc;

use buck2_error::BuckErrorContext;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rusqlite::Connection;

const TABLE_NAME: &str = "local_action_cache";

pub struct LocalActionCacheSqliteTable {
    connection: Arc<Mutex<Connection>>,
}

impl LocalActionCacheSqliteTable {
    /// Get a clone of the underlying connection for sharing.
    pub fn connection(&self) -> &Arc<Mutex<Connection>> {
        &self.connection
    }
}

impl LocalActionCacheSqliteTable {
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }

    pub(crate) fn create_table(&self) -> buck2_error::Result<()> {
        let sql = format!(
            "CREATE TABLE {TABLE_NAME} (
                action_digest   TEXT NOT NULL PRIMARY KEY,
                output_data     TEXT NOT NULL
            )",
        );
        tracing::trace!(sql = %sql, "creating table");
        self.connection
            .lock()
            .execute(&sql, [])
            .with_buck_error_context(|| format!("creating sqlite table {TABLE_NAME}"))?;
        Ok(())
    }

    pub fn lookup(&self, action_digest: &str) -> buck2_error::Result<Option<String>> {
        static SQL: Lazy<String> = Lazy::new(|| {
            format!("SELECT output_data FROM {TABLE_NAME} WHERE action_digest = ?1")
        });
        let connection = self.connection.lock();
        let mut stmt = connection.prepare(&SQL)?;
        let mut rows = stmt.query(rusqlite::params![action_digest])?;
        match rows.next()? {
            Some(row) => {
                let data: String = row.get(0)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    pub fn insert(
        &self,
        action_digest: &str,
        output_data: &str,
    ) -> buck2_error::Result<()> {
        static SQL: Lazy<String> = Lazy::new(|| {
            format!(
                "INSERT OR REPLACE INTO {TABLE_NAME} (action_digest, output_data) VALUES (?1, ?2)"
            )
        });
        tracing::trace!(sql = %*SQL, action_digest = %action_digest, "inserting into action cache");
        self.connection
            .lock()
            .execute(&SQL, rusqlite::params![action_digest, output_data])
            .with_buck_error_context(|| {
                format!("inserting `{action_digest}` into sqlite table {TABLE_NAME}")
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_action_cache_table() {
        let conn = Connection::open_in_memory().unwrap();
        let table = LocalActionCacheSqliteTable::new(Arc::new(Mutex::new(conn)));
        table.create_table().unwrap();

        // Lookup on empty table
        assert!(table.lookup("digest1").unwrap().is_none());

        // Insert and lookup
        table.insert("digest1", "output_data_1").unwrap();
        assert_eq!(
            table.lookup("digest1").unwrap(),
            Some("output_data_1".to_owned())
        );

        // Replace existing entry
        table.insert("digest1", "output_data_2").unwrap();
        assert_eq!(
            table.lookup("digest1").unwrap(),
            Some("output_data_2".to_owned())
        );

        // Multiple entries
        table.insert("digest2", "output_data_3").unwrap();
        assert_eq!(
            table.lookup("digest1").unwrap(),
            Some("output_data_2".to_owned())
        );
        assert_eq!(
            table.lookup("digest2").unwrap(),
            Some("output_data_3".to_owned())
        );
    }
}

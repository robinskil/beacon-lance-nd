//! Beacon plugin with the table type `lance_nd`: it reads a lance-nd table as flat rows.

#[cfg(test)]
mod fixture;
mod flatten;
mod schema;
mod stream;
mod table;

beacon_plugin_sdk::export_plugin!(tables = table::tables);

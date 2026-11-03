//! Translates DataFusion filters into Lance SQL filters: on attribute columns, and on the
//! stats of variables.

use beacon_plugin_sdk::datafusion::arrow::datatypes::Schema;
use beacon_plugin_sdk::datafusion::common::tree_node::{Transformed, TreeNode};
use beacon_plugin_sdk::datafusion::common::{Column as DfColumn, ScalarValue};
use beacon_plugin_sdk::datafusion::logical_expr::expr::{InList, ScalarFunction};
use beacon_plugin_sdk::datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use lance_nd::prune::prune_filter;
use nd_arrow_array::extension::{ATTRS_FIELD, GLOBAL_ATTRIBUTES_FIELD};

use crate::schema::Column;

/// The Lance SQL text of `expr`, or `None` when `expr` uses a variable or an unknown node.
pub fn to_lance(expr: &Expr, columns: &[Column]) -> Option<String> {
    match expr {
        Expr::Column(column) => attribute(&column.name, columns),
        Expr::Literal(value, _) => literal(value),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (left, right) = (to_lance(left, columns)?, to_lance(right, columns)?);
            match op {
                Operator::And | Operator::Or => Some(format!("({left}) {op} ({right})")),
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => Some(format!("{left} {op} {right}")),
                _ => None,
            }
        }
        Expr::Not(inner) => Some(format!("NOT ({})", to_lance(inner, columns)?)),
        Expr::IsNull(inner) => Some(format!("{} IS NULL", attribute_of(inner, columns)?)),
        Expr::IsNotNull(inner) => Some(format!("{} IS NOT NULL", attribute_of(inner, columns)?)),
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let values = list
                .iter()
                .map(|value| match value {
                    Expr::Literal(value, _) => literal(value),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let not = if *negated { "NOT " } else { "" };
            let column = attribute_of(expr, columns)?;
            Some(format!("{column} {not}IN ({})", values.join(", ")))
        }
        _ => None,
    }
}

/// The Lance path of `expr` when it is a global attribute column.
fn attribute_of(expr: &Expr, columns: &[Column]) -> Option<String> {
    match expr {
        Expr::Column(column) => attribute(&column.name, columns),
        _ => None,
    }
}

/// The Lance path of the attribute column `name`, or `None` for a variable or an unknown column.
fn attribute(name: &str, columns: &[Column]) -> Option<String> {
    match columns.iter().find(|c| c.name() == name)? {
        Column::Variable(_) => None,
        Column::Attribute(key) => plain(key).then(|| format!("{GLOBAL_ATTRIBUTES_FIELD}.{key}")),
        Column::VariableAttribute { variable, key } => {
            (plain(variable) && plain(key)).then(|| format!("{variable}.{ATTRS_FIELD}.{key}"))
        }
    }
}

/// `true` when Lance reads `name` as a field without quotes.
fn plain(name: &str) -> bool {
    let identifier = name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    // Lance reads these names as literals, not as fields.
    let keyword = ["true", "false", "null"].contains(&name.to_ascii_lowercase().as_str());
    identifier && !keyword
}

/// A Lance SQL filter on the stats of `table` that keeps each row that can match `expr`.
///
/// `None` when `expr` uses a column that is not a variable, or has no stats form.
pub fn to_stats(expr: &Expr, columns: &[Column], table: &Schema) -> Option<String> {
    let expr = on_variables(expr, columns)?;
    stats_sql(&prune_filter(&expr, table).ok()??)
}

/// `true` when `expr` uses only variable columns, so the stats can prune it.
pub fn is_variable_filter(expr: &Expr, columns: &[Column]) -> bool {
    on_variables(expr, columns).is_some()
}

/// `expr` with unqualified columns, or `None` when it uses no column or a column that is not a
/// variable.
fn on_variables(expr: &Expr, columns: &[Column]) -> Option<Expr> {
    let mut variables = 0;
    let mut others = 0;
    let expr = expr
        .clone()
        .transform(|e| match e {
            Expr::Column(column) => {
                if columns.contains(&Column::Variable(column.name.clone())) {
                    variables += 1;
                } else {
                    others += 1;
                }
                let column = DfColumn::new_unqualified(column.name);
                Ok(Transformed::yes(Expr::Column(column)))
            }
            e => Ok(Transformed::no(e)),
        })
        .ok()?
        .data;
    (variables > 0 && others == 0).then_some(expr)
}

/// The Lance SQL text of a stats filter from `prune_filter`.
///
/// The stats filter has no `NOT`, so a dropped side of an `AND` only keeps more rows.
fn stats_sql(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => plain(&column.name).then(|| column.name.clone()),
        Expr::ScalarFunction(ScalarFunction { func, args }) if func.name() == "get_field" => {
            match args.as_slice() {
                [base, Expr::Literal(ScalarValue::Utf8(Some(name)), _)] if plain(name) => {
                    Some(format!("{}.{name}", stats_sql(base)?))
                }
                _ => None,
            }
        }
        Expr::Literal(value, _) => literal(value),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => match (stats_sql(left), stats_sql(right)) {
                (Some(left), Some(right)) => Some(format!("({left}) AND ({right})")),
                (Some(side), None) | (None, Some(side)) => Some(side),
                (None, None) => None,
            },
            Operator::Or => Some(format!("({}) OR ({})", stats_sql(left)?, stats_sql(right)?)),
            Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                Some(format!("{} {op} {}", stats_sql(left)?, stats_sql(right)?))
            }
            _ => None,
        },
        Expr::IsNull(inner) => Some(format!("{} IS NULL", stats_sql(inner)?)),
        Expr::IsNotNull(inner) => Some(format!("{} IS NOT NULL", stats_sql(inner)?)),
        _ => None,
    }
}

/// The Lance SQL text of a literal, or `None` for a null or an unsupported type.
fn literal(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => Some(format!("'{}'", text.replace('\'', "''"))),
        ScalarValue::Boolean(Some(value)) => Some(value.to_string().to_uppercase()),
        ScalarValue::Float32(Some(value)) if value.is_finite() => Some(value.to_string()),
        ScalarValue::Float64(Some(value)) if value.is_finite() => Some(value.to_string()),
        ScalarValue::Int8(Some(_))
        | ScalarValue::Int16(Some(_))
        | ScalarValue::Int32(Some(_))
        | ScalarValue::Int64(Some(_))
        | ScalarValue::UInt8(Some(_))
        | ScalarValue::UInt16(Some(_))
        | ScalarValue::UInt32(Some(_))
        | ScalarValue::UInt64(Some(_)) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Fields};
    use beacon_plugin_sdk::datafusion::functions::string::expr_fn::upper;
    use beacon_plugin_sdk::datafusion::prelude::{col, in_list, lit};

    use super::*;

    /// A column expression with `name` as it is, without a table qualifier.
    fn c(name: &str) -> Expr {
        Expr::Column(DfColumn::from_name(name))
    }

    fn columns() -> Vec<Column> {
        vec![
            Column::Variable("temperature".into()),
            Column::Attribute("id".into()),
            Column::VariableAttribute {
                variable: "temperature".into(),
                key: "units".into(),
            },
        ]
    }

    fn lance(expr: Expr) -> Option<String> {
        to_lance(&expr, &columns())
    }

    #[test]
    fn equality_maps_to_the_struct_field() {
        let text = lance(c(".id").eq(lit("a")));
        assert_eq!(text.as_deref(), Some("__global_attrs__.id = 'a'"));
    }

    #[test]
    fn comparison_and_in_list() {
        let expr = c(".id")
            .gt_eq(lit("b"))
            .and(in_list(c(".id"), vec![lit("b"), lit("c")], false));
        assert_eq!(
            lance(expr).as_deref(),
            Some("(__global_attrs__.id >= 'b') AND (__global_attrs__.id IN ('b', 'c'))")
        );
    }

    #[test]
    fn variable_filter_is_not_pushed() {
        assert_eq!(lance(col("temperature").gt(lit(5.0))), None);
    }

    #[test]
    fn quote_in_literal_is_escaped() {
        let text = lance(c(".id").eq(lit("o'brien")));
        assert_eq!(text.as_deref(), Some("__global_attrs__.id = 'o''brien'"));
    }

    #[test]
    fn other_expressions_are_not_pushed() {
        assert_eq!(lance(upper(c(".id")).eq(lit("A"))), None);
    }

    #[test]
    fn null_checks_and_not() {
        let text = lance(!c(".id").is_null());
        assert_eq!(text.as_deref(), Some("NOT (__global_attrs__.id IS NULL)"));
    }

    #[test]
    fn literal_keyword_names_are_not_pushed() {
        let columns = vec![
            Column::Attribute("null".into()),
            Column::Attribute("True".into()),
            Column::VariableAttribute {
                variable: "temperature".into(),
                key: "false".into(),
            },
        ];
        for name in [".null", ".True", "temperature.false"] {
            let expr = c(name).eq(lit("k"));
            assert_eq!(to_lance(&expr, &columns), None, "{name}");
        }
    }

    #[test]
    fn variable_attribute_maps_to_attrs() {
        let text = lance(c("temperature.units").eq(lit("K")));
        assert_eq!(text.as_deref(), Some("temperature.attrs.units = 'K'"));
    }

    #[test]
    fn bare_global_attribute_name_is_not_pushed() {
        assert_eq!(lance(c("id").eq(lit("a"))), None);
    }

    /// A table schema with stats for `temperature` (Float32) and none for `label`.
    fn stats_table() -> Schema {
        let bounds = Fields::from(vec![
            Field::new("min", DataType::Float32, true),
            Field::new("max", DataType::Float32, true),
        ]);
        let stats = Fields::from(vec![Field::new(
            "temperature",
            DataType::Struct(bounds),
            true,
        )]);
        Schema::new(vec![
            Field::new("temperature", DataType::Float32, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("__stats__", DataType::Struct(stats), true),
        ])
    }

    fn stats(expr: Expr) -> Option<String> {
        let columns = vec![
            Column::Variable("temperature".into()),
            Column::Variable("label".into()),
            Column::Attribute("id".into()),
        ];
        to_stats(&expr, &columns, &stats_table())
    }

    #[test]
    fn variable_filter_maps_to_the_stats() {
        let text = stats(col("temperature").gt(lit(50.0f32)));
        assert_eq!(
            text.as_deref(),
            Some(
                "(__stats__.temperature IS NOT NULL) AND \
                 ((__stats__.temperature.min IS NULL) OR (__stats__.temperature.max > 50))"
            )
        );
    }

    #[test]
    fn qualified_variable_maps_to_the_stats() {
        let expr = Expr::Column(DfColumn::new(Some("t"), "temperature")).lt(lit(5.0f32));
        let text = stats(expr).unwrap();
        assert!(text.contains("__stats__.temperature.min < 5"), "{text}");
    }

    #[test]
    fn filter_with_an_attribute_has_no_stats_form() {
        let expr = c(".id").eq(lit("a")).or(col("temperature").gt(lit(5.0f32)));
        assert_eq!(stats(expr), None);
    }

    #[test]
    fn variable_without_stats_has_no_stats_form() {
        assert_eq!(stats(col("label").eq(lit("k"))), None);
    }

    #[test]
    fn table_without_stats_has_no_stats_form() {
        let table = Schema::new(vec![Field::new("temperature", DataType::Float32, true)]);
        let columns = vec![Column::Variable("temperature".into())];
        let expr = col("temperature").gt(lit(5.0f32));
        assert_eq!(to_stats(&expr, &columns, &table), None);
    }

    #[test]
    fn variable_filters_are_pushed() {
        let columns = columns();
        assert!(is_variable_filter(
            &col("temperature").gt(lit(5.0)),
            &columns
        ));
        assert!(!is_variable_filter(&c(".id").eq(lit("a")), &columns));
        assert!(!is_variable_filter(&lit(true), &columns));
    }
}

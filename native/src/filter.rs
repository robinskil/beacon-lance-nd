//! Translates DataFusion filters on global attribute columns into Lance SQL filters.

use beacon_plugin_sdk::datafusion::common::ScalarValue;
use beacon_plugin_sdk::datafusion::logical_expr::{BinaryExpr, Expr, Operator, expr::InList};
use nd_arrow_array::extension::GLOBAL_ATTRIBUTES_FIELD;

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

/// The Lance path of the global attribute `name`. A name that needs quotes stays in DataFusion.
fn attribute(name: &str, columns: &[Column]) -> Option<String> {
    let plain = name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    // Lance reads these names as literals, not as fields.
    let keyword = ["true", "false", "null"].contains(&name.to_ascii_lowercase().as_str());
    let known = columns.contains(&Column::Attribute(name.to_string()));
    (plain && !keyword && known).then(|| format!("{GLOBAL_ATTRIBUTES_FIELD}.{name}"))
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
    use beacon_plugin_sdk::datafusion::functions::string::expr_fn::upper;
    use beacon_plugin_sdk::datafusion::prelude::{col, in_list, lit};

    use super::*;

    fn columns() -> Vec<Column> {
        vec![
            Column::Variable("temperature".into()),
            Column::Attribute("id".into()),
        ]
    }

    fn lance(expr: Expr) -> Option<String> {
        to_lance(&expr, &columns())
    }

    #[test]
    fn equality_maps_to_the_struct_field() {
        let text = lance(col("id").eq(lit("a")));
        assert_eq!(text.as_deref(), Some("__global_attrs__.id = 'a'"));
    }

    #[test]
    fn comparison_and_in_list() {
        let expr =
            col("id")
                .gt_eq(lit("b"))
                .and(in_list(col("id"), vec![lit("b"), lit("c")], false));
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
        let text = lance(col("id").eq(lit("o'brien")));
        assert_eq!(text.as_deref(), Some("__global_attrs__.id = 'o''brien'"));
    }

    #[test]
    fn other_expressions_are_not_pushed() {
        assert_eq!(lance(upper(col("id")).eq(lit("A"))), None);
    }

    #[test]
    fn null_checks_and_not() {
        let text = lance(!col("id").is_null());
        assert_eq!(text.as_deref(), Some("NOT (__global_attrs__.id IS NULL)"));
    }

    #[test]
    fn literal_keyword_names_are_not_pushed() {
        use beacon_plugin_sdk::datafusion::common::Column as DfColumn;
        let columns = vec![
            Column::Attribute("null".into()),
            Column::Attribute("True".into()),
        ];
        for name in ["null", "True"] {
            let expr = Expr::Column(DfColumn::from_name(name)).eq(lit("k"));
            assert_eq!(to_lance(&expr, &columns), None, "{name}");
        }
    }
}

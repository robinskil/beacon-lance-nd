//! Translates DataFusion filters on the table columns into a filter on the Lance columns.

use beacon_plugin_sdk::datafusion::common::Column as DfColumn;
use beacon_plugin_sdk::datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use beacon_plugin_sdk::datafusion::functions::core::expr_fn::get_field;
use beacon_plugin_sdk::datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use nd_arrow_array::extension::{ATTRS_FIELD, GLOBAL_ATTRIBUTES_FIELD, VALUES_FIELD};

use crate::schema::Column;

/// The filter of `expr` on the Lance columns, or `None` when Lance cannot use `expr`.
///
/// A variable becomes `<variable>.values`, which lance-nd prunes with the stats. An attribute
/// becomes the path of its struct field, which filters a row exactly.
pub fn to_lance(expr: &Expr, columns: &[Column]) -> Option<Expr> {
    if !supported(expr) {
        return None;
    }
    let mut known = true;
    let expr = expr
        .clone()
        .transform(|e| match e {
            Expr::Column(column) => match columns
                .iter()
                .find(|c| c.name() == column.name)
                .and_then(lance_path)
            {
                Some(path) => Ok(Transformed::yes(path)),
                None => {
                    known = false;
                    Ok(Transformed::no(Expr::Column(column)))
                }
            },
            e => Ok(Transformed::no(e)),
        })
        .ok()?
        .data;
    known.then_some(expr)
}

/// The Lance column path of a table column, or `None` for a column that Lance does not store.
fn lance_path(column: &Column) -> Option<Expr> {
    let root = |name: &str| Expr::Column(DfColumn::new_unqualified(name));
    match column {
        Column::Variable(name) => Some(get_field(root(name), VALUES_FIELD)),
        Column::Attribute(key) => Some(get_field(root(GLOBAL_ATTRIBUTES_FIELD), key.as_str())),
        Column::VariableAttribute { variable, key } => Some(get_field(
            get_field(root(variable), ATTRS_FIELD),
            key.as_str(),
        )),
        Column::Shard => None,
    }
}

/// `true` when each node of `expr` is one that Lance can plan.
fn supported(expr: &Expr) -> bool {
    let mut supported = true;
    expr.apply(|e| {
        supported = match e {
            Expr::BinaryExpr(BinaryExpr { op, .. }) => matches!(
                op,
                Operator::And
                    | Operator::Or
                    | Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ),
            Expr::Column(_)
            | Expr::Literal(..)
            | Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::InList(_)
            | Expr::Between(_)
            | Expr::Like(_)
            | Expr::Cast(_)
            | Expr::TryCast(_) => true,
            _ => false,
        };
        Ok(if supported {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })
    .expect("the visit does not fail");
    supported
}

#[cfg(test)]
mod tests {
    use beacon_plugin_sdk::datafusion::functions::string::expr_fn::upper;
    use beacon_plugin_sdk::datafusion::prelude::{in_list, lit};

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

    fn lance(expr: Expr) -> Option<Expr> {
        to_lance(&expr, &columns())
    }

    fn path(root: &str, names: &[&str]) -> Expr {
        names.iter().fold(
            Expr::Column(DfColumn::new_unqualified(root)),
            |base, name| get_field(base, *name),
        )
    }

    #[test]
    fn global_attribute_maps_to_its_field() {
        let expected = path("__global_attrs__", &["id"]).eq(lit("a"));
        assert_eq!(lance(c(".id").eq(lit("a"))), Some(expected));
    }

    #[test]
    fn variable_attribute_maps_to_attrs() {
        let expected = path("temperature", &["attrs", "units"]).eq(lit("K"));
        assert_eq!(lance(c("temperature.units").eq(lit("K"))), Some(expected));
    }

    #[test]
    fn variable_maps_to_its_values() {
        let expected = path("temperature", &["values"]).gt(lit(5.0));
        assert_eq!(lance(c("temperature").gt(lit(5.0))), Some(expected));
    }

    #[test]
    fn qualified_column_is_mapped() {
        let expr = Expr::Column(DfColumn::new(Some("t"), ".id")).eq(lit("a"));
        let expected = path("__global_attrs__", &["id"]).eq(lit("a"));
        assert_eq!(lance(expr), Some(expected));
    }

    #[test]
    fn mixed_or_is_pushed() {
        let expr = c(".id").eq(lit("a")).or(c("temperature").gt(lit(5.0)));
        let expected =
            path("__global_attrs__", &["id"])
                .eq(lit("a"))
                .or(path("temperature", &["values"]).gt(lit(5.0)));
        assert_eq!(lance(expr), Some(expected));
    }

    #[test]
    fn in_list_and_not_are_pushed() {
        let expr = !in_list(c(".id"), vec![lit("b"), lit("c")], false);
        let expected = !in_list(
            path("__global_attrs__", &["id"]),
            vec![lit("b"), lit("c")],
            false,
        );
        assert_eq!(lance(expr), Some(expected));
    }

    #[test]
    fn keyword_names_are_pushed() {
        let columns = vec![Column::Attribute("null".into())];
        let expected = path("__global_attrs__", &["null"]).eq(lit("k"));
        assert_eq!(to_lance(&c(".null").eq(lit("k")), &columns), Some(expected));
    }

    #[test]
    fn functions_are_not_pushed() {
        assert_eq!(lance(upper(c(".id")).eq(lit("A"))), None);
    }

    #[test]
    fn unknown_column_is_not_pushed() {
        assert_eq!(lance(c("id").eq(lit("a"))), None);
    }

    #[test]
    fn arithmetic_is_not_pushed() {
        let expr = (c("temperature") + lit(1.0)).gt(lit(5.0));
        assert_eq!(lance(expr), None);
    }

    #[test]
    fn shard_filter_is_not_for_lance() {
        let mut columns = columns();
        columns.push(Column::Shard);
        let expr = c("__shard").eq(lit("a.lance"));
        assert_eq!(to_lance(&expr, &columns), None);
        let mixed = c("__shard").eq(lit("a.lance")).or(c(".id").eq(lit("a")));
        assert_eq!(to_lance(&mixed, &columns), None);
    }
}

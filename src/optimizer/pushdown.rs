use super::*;

#[derive(Clone)]
pub(crate) struct PushedFilter {
    pub(crate) table: String,
    pub(crate) expression: Expr,
    pub(crate) derived: bool,
}

pub(crate) fn split_conjuncts<'a>(expression: &'a Expr, output: &mut Vec<&'a Expr>) {
    if let Expr::Binary(operator, left, right) = expression
        && operator == "and"
    {
        split_conjuncts(left, output);
        split_conjuncts(right, output);
    } else {
        output.push(expression);
    }
}

pub(super) fn owned_conjuncts(expression: Expr, output: &mut Vec<Expr>) {
    match expression {
        Expr::Binary(operator, left, right) if operator == "and" => {
            owned_conjuncts(*left, output);
            owned_conjuncts(*right, output);
        }
        expression => output.push(expression),
    }
}

fn expression_relations(
    expression: &Expr,
    bindings: &Bindings,
    output: &mut HashSet<String>,
) -> bool {
    match expression {
        Expr::Column(column) | Expr::DictEq(column, _, _) => {
            if let Ok((table, _)) = resolve_column(column, bindings) {
                output.insert(table);
                true
            } else {
                false
            }
        }
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            expression_relations(value, bindings, output)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            expression_relations(left, bindings, output)
                && expression_relations(right, bindings, output)
        }
        Expr::Call(_, values) => values
            .iter()
            .all(|value| expression_relations(value, bindings, output)),
        Expr::Case(branches, fallback) => {
            branches.iter().all(|(condition, value)| {
                expression_relations(condition, bindings, output)
                    && expression_relations(value, bindings, output)
            }) && expression_relations(fallback, bindings, output)
        }
        Expr::Cast(value, _) => expression_relations(value, bindings, output),
        Expr::InList(value, values, _) => {
            expression_relations(value, bindings, output)
                && values
                    .iter()
                    .all(|item| expression_relations(item, bindings, output))
        }
        Expr::Between(value, low, high, _) => {
            expression_relations(value, bindings, output)
                && expression_relations(low, bindings, output)
                && expression_relations(high, bindings, output)
        }
        Expr::Window { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_, _, _) => false,
        _ => true,
    }
}

pub(super) fn column_literal_filter(expression: &Expr) -> Option<(&str, &str, &Expr, bool)> {
    let Expr::Binary(operator, left, right) = expression else {
        return None;
    };
    let literal = |value: &Expr| literal_value(value).is_some();
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(column), value) if literal(value) => Some((column, operator, value, true)),
        (value, Expr::Column(column)) if literal(value) => Some((column, operator, value, false)),
        _ => None,
    }
}

fn equivalent_filter(expression: &Expr, from: &str, to: &str) -> Option<Expr> {
    let (column, operator, literal, column_on_left) = column_literal_filter(expression)?;
    if column != from {
        return None;
    }
    Some(if column_on_left {
        Expr::Binary(
            operator.into(),
            Box::new(Expr::Column(to.into())),
            Box::new(literal.clone()),
        )
    } else {
        Expr::Binary(
            operator.into(),
            Box::new(literal.clone()),
            Box::new(Expr::Column(to.into())),
        )
    })
}

pub(crate) fn pushed_filters(query: &Query) -> Vec<PushedFilter> {
    if !query.optimizer_enabled {
        return Vec::new();
    }
    if query
        .joins
        .iter()
        .any(|join| !matches!(join.kind, JoinKind::Inner | JoinKind::Cross))
    {
        return Vec::new();
    }
    let Ok(bindings) = query_bindings(query) else {
        return Vec::new();
    };
    let mut output = Vec::new();
    if let Some(filter) = &query.filter {
        let mut conjuncts = Vec::new();
        split_conjuncts(filter, &mut conjuncts);
        for expression in conjuncts {
            let mut relations = HashSet::new();
            if expression_relations(expression, &bindings, &mut relations) && relations.len() == 1 {
                output.push(PushedFilter {
                    table: relations.into_iter().next().expect("single relation"),
                    expression: expression.clone(),
                    derived: false,
                });
            }
        }
    }
    let seeds = output.clone();
    for join in &query.joins {
        if join.kind != JoinKind::Inner {
            continue;
        }
        let Some(Expr::Binary(operator, left, right)) = join.on.as_ref() else {
            continue;
        };
        if operator != "=" {
            continue;
        }
        let (Expr::Column(left), Expr::Column(right)) = (left.as_ref(), right.as_ref()) else {
            continue;
        };
        for seed in &seeds {
            for (from, to) in [
                (left.as_str(), right.as_str()),
                (right.as_str(), left.as_str()),
            ] {
                let Some(expression) = equivalent_filter(&seed.expression, from, to) else {
                    continue;
                };
                let Ok((table, _)) = resolve_column(to, &bindings) else {
                    continue;
                };
                let key = format!("{table}:{expression:?}");
                if output
                    .iter()
                    .any(|filter| format!("{}:{:?}", filter.table, filter.expression) == key)
                {
                    continue;
                }
                output.push(PushedFilter {
                    table,
                    expression,
                    derived: true,
                });
            }
        }
    }
    output
}

pub(crate) fn residual_filter(query: &Query) -> Option<Expr> {
    let pushed: HashSet<_> = pushed_filters(query)
        .into_iter()
        .filter(|filter| !filter.derived)
        .map(|filter| format!("{:?}", filter.expression))
        .collect();
    if pushed.is_empty() {
        return query.filter.clone();
    }
    let mut conjuncts = Vec::new();
    owned_conjuncts(query.filter.clone()?, &mut conjuncts);
    let mut residuals = conjuncts
        .into_iter()
        .filter(|expression| !pushed.contains(&format!("{expression:?}")));
    let mut residual = residuals.next()?;
    for expression in residuals {
        residual = Expr::Binary("and".into(), Box::new(residual), Box::new(expression));
    }
    Some(residual)
}

pub(crate) fn filter_always_false(filter: Option<&Expr>) -> bool {
    matches!(filter, Some(Expr::Bool(false) | Expr::Null))
}

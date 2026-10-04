use crate::execution::scalar::{apply_binary, cast_value, decimal_text, eval_values, like_matches};
use crate::sql::*;
use crate::storage::{Table, query_bindings, resolve_column};
use crate::types::Scalar;
use std::collections::{BTreeMap, HashMap, HashSet};

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

fn owned_conjuncts(expression: Expr, output: &mut Vec<Expr>) {
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
    bindings: &HashMap<String, String>,
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

fn column_literal_filter(expression: &Expr) -> Option<(&str, &str, &Expr, bool)> {
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

fn comparison_constraint(expression: &Expr) -> Option<(String, String, i64)> {
    let Expr::Binary(operator, left, right) = expression else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(column), Expr::Int(value)) => {
            Some((column.clone(), operator.clone(), *value))
        }
        (Expr::Int(value), Expr::Column(column)) => {
            let flipped = match operator.as_str() {
                "<" => ">",
                "<=" => ">=",
                ">" => "<",
                ">=" => "<=",
                value => value,
            };
            Some((column.clone(), flipped.into(), *value))
        }
        _ => None,
    }
}

fn contradictory_filter(expression: &Expr) -> bool {
    #[derive(Default)]
    struct Bounds {
        equal: Option<i64>,
        lower: Option<(i64, bool)>,
        upper: Option<(i64, bool)>,
    }
    let mut conjuncts = Vec::new();
    split_conjuncts(expression, &mut conjuncts);
    if conjuncts
        .iter()
        .any(|value| matches!(value, Expr::Bool(false) | Expr::Null))
    {
        return true;
    }
    let mut columns = HashMap::<String, Bounds>::new();
    for conjunct in conjuncts {
        let Some((column, operator, value)) = comparison_constraint(conjunct) else {
            continue;
        };
        let bounds = columns.entry(column).or_default();
        match operator.as_str() {
            "=" => {
                if bounds.equal.is_some_and(|existing| existing != value) {
                    return true;
                }
                bounds.equal = Some(value);
            }
            ">" | ">=" => {
                let candidate = (value, operator == ">=");
                if bounds.lower.is_none_or(|current| {
                    candidate.0 > current.0
                        || (candidate.0 == current.0 && !candidate.1 && current.1)
                }) {
                    bounds.lower = Some(candidate);
                }
            }
            "<" | "<=" => {
                let candidate = (value, operator == "<=");
                if bounds.upper.is_none_or(|current| {
                    candidate.0 < current.0
                        || (candidate.0 == current.0 && !candidate.1 && current.1)
                }) {
                    bounds.upper = Some(candidate);
                }
            }
            _ => {}
        }
    }
    columns.into_values().any(|bounds| {
        if let Some(equal) = bounds.equal
            && (bounds
                .lower
                .is_some_and(|(value, inclusive)| equal < value || (equal == value && !inclusive))
                || bounds.upper.is_some_and(|(value, inclusive)| {
                    equal > value || (equal == value && !inclusive)
                }))
        {
            return true;
        }
        bounds.lower.zip(bounds.upper).is_some_and(
            |((lower, lower_inclusive), (upper, upper_inclusive))| {
                lower > upper || (lower == upper && !(lower_inclusive && upper_inclusive))
            },
        )
    })
}

fn predicate_rank(expression: &Expr) -> usize {
    match expression {
        Expr::DictEq(_, _, _) => 0,
        Expr::Binary(operator, left, right)
            if ["=", "!=", "<", "<=", ">", ">="].contains(&operator.as_str())
                && (literal_value(left).is_some() || literal_value(right).is_some()) =>
        {
            let non_literal = if literal_value(left).is_some() {
                right.as_ref()
            } else {
                left.as_ref()
            };
            if matches!(non_literal, Expr::Column(_)) {
                1
            } else {
                4
            }
        }
        Expr::IsNull(_, _) | Expr::Between(_, _, _, _) | Expr::InList(_, _, _) => 2,
        Expr::Like(_, _, _) | Expr::Func(_, _) | Expr::Call(_, _) => 4,
        _ => 3,
    }
}

fn reorder_conjuncts(filter: &mut Expr) -> bool {
    let original = format!("{filter:?}");
    let mut conjuncts = Vec::new();
    owned_conjuncts(filter.clone(), &mut conjuncts);
    if conjuncts.len() < 2 {
        return false;
    }
    conjuncts.sort_by_key(predicate_rank);
    let mut values = conjuncts.into_iter();
    let mut rebuilt = values.next().expect("non-empty conjuncts");
    for value in values {
        rebuilt = Expr::Binary("and".into(), Box::new(rebuilt), Box::new(value));
    }
    let changed = original != format!("{rebuilt:?}");
    *filter = rebuilt;
    changed
}

fn estimated_filtered_rows(table: &str, filters: &[PushedFilter], event_rows: usize) -> usize {
    let base = match table {
        "events" => event_rows,
        "users" => 250_000,
        "campaigns" => 5_000,
        _ => 1_000,
    };
    let mut estimate = base;
    for filter in filters.iter().filter(|filter| filter.table == table) {
        if let Some((column, operator, Expr::Int(value), column_on_left)) =
            column_literal_filter(&filter.expression)
        {
            let operator = if column_on_left {
                operator
            } else {
                match operator {
                    "<" => ">",
                    "<=" => ">=",
                    ">" => "<",
                    ">=" => "<=",
                    value => value,
                }
            };
            if column
                .rsplit('.')
                .next()
                .is_some_and(|name| name.ends_with("_id"))
            {
                estimate = match operator {
                    "=" => 1,
                    "<" => estimate.min((*value).max(0) as usize),
                    "<=" => estimate.min(value.saturating_add(1).max(0) as usize),
                    ">" | ">=" => estimate.div_ceil(2),
                    _ => estimate.div_ceil(10),
                };
                continue;
            }
        }
        estimate = estimate.div_ceil(10);
    }
    estimate.max(1)
}

pub(crate) fn prepare_expr(e: &mut Expr, t: &Table) {
    match e {
        Expr::Binary(op, a, b) if (op == "=" || op == "!=") => {
            prepare_expr(a, t);
            prepare_expr(b, t);
            let replacement = match (&**a, &**b) {
                (Expr::Column(c), Expr::String(s)) => t
                    .dict_id(c, s)
                    .map(|id| Expr::DictEq(c.clone(), id, op == "!=")),
                (Expr::String(s), Expr::Column(c)) => t
                    .dict_id(c, s)
                    .map(|id| Expr::DictEq(c.clone(), id, op == "!=")),
                _ => None,
            };
            if let Some(x) = replacement {
                *e = x
            }
        }
        Expr::Binary(_, a, b) => {
            prepare_expr(a, t);
            prepare_expr(b, t)
        }
        Expr::Unary(_, a) | Expr::Func(_, a) | Expr::IsNull(a, _) => prepare_expr(a, t),
        Expr::Call(_, args) => args.iter_mut().for_each(|arg| prepare_expr(arg, t)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                prepare_expr(condition, t);
                prepare_expr(value, t);
            }
            prepare_expr(fallback, t);
        }
        Expr::Cast(value, _) => prepare_expr(value, t),
        Expr::InList(value, values, _) => {
            prepare_expr(value, t);
            values.iter_mut().for_each(|item| prepare_expr(item, t));
        }
        Expr::Between(value, low, high, _) => {
            prepare_expr(value, t);
            prepare_expr(low, t);
            prepare_expr(high, t);
        }
        Expr::Like(value, pattern, _) => {
            prepare_expr(value, t);
            prepare_expr(pattern, t);
        }
        Expr::Window { args, .. } => {
            args.iter_mut()
                .for_each(|argument| prepare_expr(argument, t));
        }
        Expr::InSubquery(value, _, _) => prepare_expr(value, t),
        _ => {}
    }
}

pub(crate) fn literal_value(expression: &Expr) -> Option<Scalar> {
    match expression {
        Expr::Null => Some(Scalar::Null),
        Expr::Int(value) => Some(Scalar::Int(*value)),
        Expr::Float(value) => Some(Scalar::Float(*value)),
        Expr::Bool(value) => Some(Scalar::Bool(*value)),
        Expr::String(value) => Some(Scalar::Str(value.clone())),
        _ => None,
    }
}

pub(crate) fn literal_expression(value: Scalar) -> Expr {
    match value {
        Scalar::Null => Expr::Null,
        Scalar::Int(value) => Expr::Int(value),
        Scalar::Decimal(value) => Expr::Cast(
            Box::new(Expr::String(decimal_text(value))),
            "decimal(18,2)".into(),
        ),
        Scalar::Float(value) => Expr::Float(value),
        Scalar::Bool(value) => Expr::Bool(value),
        Scalar::Str(value) => Expr::String(value),
    }
}

/// Fold immutable scalar expressions and apply identities that are valid under
/// SQL three-valued logic. Returns the number of rewritten AST nodes.
pub(crate) fn optimize_expression(expression: &mut Expr) -> usize {
    let mut rewrites = 0;
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            rewrites += optimize_expression(value);
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            rewrites += optimize_expression(left);
            rewrites += optimize_expression(right);
        }
        Expr::Call(_, arguments) => {
            rewrites += arguments.iter_mut().map(optimize_expression).sum::<usize>();
        }
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                rewrites += optimize_expression(condition);
                rewrites += optimize_expression(value);
            }
            rewrites += optimize_expression(fallback);
        }
        Expr::Cast(value, _) => rewrites += optimize_expression(value),
        Expr::InList(value, candidates, _) => {
            rewrites += optimize_expression(value);
            rewrites += candidates
                .iter_mut()
                .map(optimize_expression)
                .sum::<usize>();
        }
        Expr::Between(value, low, high, _) => {
            rewrites += optimize_expression(value);
            rewrites += optimize_expression(low);
            rewrites += optimize_expression(high);
        }
        Expr::Window { args, .. } => {
            rewrites += args.iter_mut().map(optimize_expression).sum::<usize>();
        }
        Expr::InSubquery(value, _, _) => rewrites += optimize_expression(value),
        _ => {}
    }

    let replacement = match expression {
        Expr::Unary(op, value) => literal_value(value).map(|value| {
            if op == "not" {
                literal_expression(
                    value
                        .sql_bool()
                        .map_or(Scalar::Null, |boolean| Scalar::Bool(!boolean)),
                )
            } else {
                literal_expression(match value {
                    Scalar::Int(value) => value.checked_neg().map_or(Scalar::Null, Scalar::Int),
                    Scalar::Decimal(value) => {
                        value.checked_neg().map_or(Scalar::Null, Scalar::Decimal)
                    }
                    Scalar::Float(value) => Scalar::Float(-value),
                    _ => Scalar::Null,
                })
            }
        }),
        Expr::Binary(op, left, right) => {
            let left_value = literal_value(left);
            let right_value = literal_value(right);
            if let (Some(left), Some(right)) = (left_value.clone(), right_value.clone()) {
                Some(literal_expression(apply_binary(op, left, right)))
            } else {
                match (op.as_str(), left_value.as_ref(), right_value.as_ref()) {
                    ("and", Some(Scalar::Bool(true)), _) => Some((**right).clone()),
                    ("and", _, Some(Scalar::Bool(true))) => Some((**left).clone()),
                    ("and", Some(Scalar::Bool(false)), _)
                    | ("and", _, Some(Scalar::Bool(false))) => Some(Expr::Bool(false)),
                    ("or", Some(Scalar::Bool(false)), _) => Some((**right).clone()),
                    ("or", _, Some(Scalar::Bool(false))) => Some((**left).clone()),
                    ("or", Some(Scalar::Bool(true)), _) | ("or", _, Some(Scalar::Bool(true))) => {
                        Some(Expr::Bool(true))
                    }
                    _ => None,
                }
            }
        }
        Expr::Func(name, value) if !matches!(name.as_str(), "date" | "timestamp" | "interval") => {
            literal_value(value).map(|value| literal_expression(eval_values(name, vec![value])))
        }
        Expr::Call(name, arguments)
            if !matches!(
                name.as_str(),
                "date" | "timestamp" | "interval" | "date_trunc" | "extract"
            ) && arguments
                .iter()
                .all(|argument| literal_value(argument).is_some()) =>
        {
            Some(literal_expression(eval_values(
                name,
                arguments.iter().filter_map(literal_value).collect(),
            )))
        }
        Expr::Cast(value, data_type) => {
            literal_value(value).map(|value| literal_expression(cast_value(data_type, value)))
        }
        Expr::IsNull(value, negated) => {
            literal_value(value).map(|value| Expr::Bool(matches!(value, Scalar::Null) ^ *negated))
        }
        Expr::Between(value, low, high, negated) => {
            match (
                literal_value(value),
                literal_value(low),
                literal_value(high),
            ) {
                (Some(value), Some(low), Some(high)) => {
                    let result = apply_binary(
                        "and",
                        apply_binary(">=", value.clone(), low),
                        apply_binary("<=", value, high),
                    );
                    Some(literal_expression(if *negated {
                        result
                            .sql_bool()
                            .map_or(Scalar::Null, |boolean| Scalar::Bool(!boolean))
                    } else {
                        result
                    }))
                }
                _ => None,
            }
        }
        Expr::Like(value, pattern, negated) => match (literal_value(value), literal_value(pattern))
        {
            (Some(Scalar::Str(value)), Some(Scalar::Str(pattern))) => {
                Some(Expr::Bool(like_matches(&value, &pattern) ^ *negated))
            }
            (Some(Scalar::Null), _) | (_, Some(Scalar::Null)) => Some(Expr::Null),
            _ => None,
        },
        Expr::Case(branches, fallback) => {
            let mut replacement = None;
            let mut all_constant_false_or_null = true;
            for (condition, value) in branches.iter() {
                match literal_value(condition) {
                    Some(Scalar::Bool(true)) => {
                        replacement = Some(value.clone());
                        break;
                    }
                    Some(Scalar::Bool(false) | Scalar::Null) => {}
                    _ => {
                        all_constant_false_or_null = false;
                        break;
                    }
                }
            }
            if replacement.is_some() {
                replacement
            } else if all_constant_false_or_null {
                Some((**fallback).clone())
            } else {
                None
            }
        }
        _ => None,
    };
    if let Some(replacement) = replacement {
        *expression = replacement;
        rewrites += 1;
    }
    rewrites
}

pub(crate) fn optimize_query(query: &mut Query, event_rows: usize) -> usize {
    let mut rewrites = 0;
    for item in &mut query.select {
        rewrites += optimize_expression(&mut item.expr);
    }
    if let Some(filter) = &mut query.filter {
        rewrites += optimize_expression(filter);
        if contradictory_filter(filter) {
            *filter = Expr::Bool(false);
            rewrites += 1;
        } else if reorder_conjuncts(filter) {
            rewrites += 1;
        }
    }
    for join in &mut query.joins {
        if let Some(on) = &mut join.on {
            rewrites += optimize_expression(on);
        }
    }
    if let Some(having) = &mut query.having {
        rewrites += optimize_expression(having);
    }
    let safe_star_reorder = query_bindings(query).is_ok_and(|bindings| {
        query.joins.iter().all(|join| {
            if join.kind != JoinKind::Inner {
                return false;
            }
            let Some(Expr::Binary(operator, left, right)) = join.on.as_ref() else {
                return false;
            };
            if operator != "=" {
                return false;
            }
            let (Expr::Column(left), Expr::Column(right)) = (left.as_ref(), right.as_ref()) else {
                return false;
            };
            let (Ok((left_table, _)), Ok((right_table, _))) = (
                resolve_column(left, &bindings),
                resolve_column(right, &bindings),
            ) else {
                return false;
            };
            (left_table == query.from.name && right_table == join.table.name)
                || (right_table == query.from.name && left_table == join.table.name)
        })
    });
    if query.optimizer_enabled && query.joins.len() > 1 && safe_star_reorder {
        let filters = pushed_filters(query);
        let original: Vec<_> = query
            .joins
            .iter()
            .map(|join| join.table.name.clone())
            .collect();
        query
            .joins
            .sort_by_key(|join| estimated_filtered_rows(&join.table.name, &filters, event_rows));
        if original
            != query
                .joins
                .iter()
                .map(|join| join.table.name.clone())
                .collect::<Vec<_>>()
        {
            rewrites += 1;
        }
    }
    rewrites
}

pub(crate) fn prepare(mut q: Query, t: &Table) -> Query {
    q.optimizer_enabled = std::env::var_os("DREMEL_DISABLE_OPTIMIZER").is_none();
    let mut rewrites = if q.optimizer_enabled {
        optimize_query(&mut q, t.len())
    } else {
        0
    };
    q.plan().expect("query already passed planning");
    if let Some(f) = &mut q.filter {
        prepare_expr(f, t)
    }
    for join in &mut q.joins {
        if let Some(on) = &mut join.on {
            prepare_expr(on, t);
        }
    }
    if let Some(having) = &mut q.having {
        prepare_expr(having, t);
    }
    let mut scan_filters = Vec::new();
    if q.optimizer_enabled {
        let mut scan_operators = Vec::new();
        if filter_always_false(q.filter.as_ref()) {
            scan_operators.push("EmptyScanExec(reason=contradiction)".into());
        }
        let mut by_table = BTreeMap::<String, (usize, usize)>::new();
        scan_filters = pushed_filters(&q);
        rewrites += scan_filters.len();
        for filter in &scan_filters {
            let counts = by_table.entry(filter.table.clone()).or_default();
            counts.0 += 1;
            counts.1 += usize::from(filter.derived);
        }
        for (table, (predicates, derived)) in by_table {
            scan_operators.push(format!(
                "ScanFilterExec(table={table};predicates={predicates};derived={derived})"
            ));
        }
        q.physical.splice(1..1, scan_operators);
    }
    let mut estimate = match q.from.name.as_str() {
        "users" => 250_000,
        "campaigns" => 5_000,
        _ => t.len(),
    };
    if filter_always_false(q.filter.as_ref()) {
        estimate = 0;
    } else if q.optimizer_enabled {
        if scan_filters.is_empty() {
            if q.filter.is_some() {
                estimate = estimate.div_ceil(10);
            }
        } else {
            estimate = estimated_filtered_rows(&q.from.name, &scan_filters, t.len());
        }
    } else if q.filter.is_some() {
        estimate = estimate.div_ceil(10);
    }
    if q.limit.is_some() {
        estimate = estimate.min(q.limit.unwrap_or(estimate));
    }
    q.physical.push(format!("EstimateExec(rows={estimate})"));
    let statistics = match q.from.name.as_str() {
        "events" => format!(
            "StatsExec(table=events;rows={};campaign_nulls={};country_distinct={};device_distinct={};event_type_distinct={};event_id_min={};event_id_max={})",
            t.len(),
            t.campaign_null_count(),
            t.country_dict.values.len(),
            t.device_dict.values.len(),
            t.event_dict.values.len(),
            t.event_id_bounds().0,
            t.event_id_bounds().1,
        ),
        "users" => "StatsExec(table=users;rows=250000;nulls=0;distinct=user_id:250000;min=user_id:1;max=user_id:250000)".into(),
        "campaigns" => "StatsExec(table=campaigns;rows=5000;nulls=0;distinct=campaign_id:5000;min=campaign_id:1;max=campaign_id:5000)".into(),
        table => format!("StatsExec(table={table};rows={estimate})"),
    };
    q.physical.push(statistics);
    q.physical.push(if q.optimizer_enabled {
        format!("OptimizerExec(rewrites={rewrites};rules=constant_folding+3vl+projection_pruning+predicate_pushdown+transitive_predicates+range_contradiction+filter_ordering+selectivity_join_order+join_selection+runtime_filter+topk)")
    } else {
        "OptimizerExec(disabled=true)".into()
    });
    q
}

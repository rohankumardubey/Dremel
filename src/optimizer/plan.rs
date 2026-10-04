use super::*;

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

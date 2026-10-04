use super::*;

impl Query {
    pub(crate) fn plan(&mut self) -> Result<(), String> {
        let aggregate = self.select.iter().any(|x| contains_agg(&x.expr))
            || self.having.as_ref().is_some_and(contains_agg)
            || !self.group_by.is_empty();
        if aggregate
            && self.select.iter().any(|x| {
                !is_agg(&x.expr) && !matches!(&x.expr,Expr::Column(c) if self.group_by.contains(c))
            })
        {
            return Err("non-aggregate select expression must be grouped".into());
        }
        let mut cols = BTreeSet::new();
        for s in &self.select {
            collect(&s.expr, &mut cols)
        }
        if let Some(f) = &self.filter {
            collect(f, &mut cols)
        }
        for join in &self.joins {
            if let Some(on) = &join.on {
                collect(on, &mut cols);
            }
        }
        if let Some(having) = &self.having {
            collect(having, &mut cols);
        }
        for g in &self.group_by {
            cols.insert(g.clone());
        }
        self.columns = cols.into_iter().collect();
        self.logical = vec![format!("Scan(columns=[{}])", self.columns.join(","))];
        self.physical = vec![format!("ScanExec(columns=[{}])", self.columns.join(","))];
        let table_rows = |table: &str| match table {
            "events" => 1_000_000usize,
            "users" => 250_000,
            "campaigns" => 5_000,
            _ => 1_000,
        };
        let mut estimated_rows = table_rows(&self.from.name);
        let materialized_joins = !self.ctes.is_empty();
        for join in &self.joins {
            self.logical
                .push(format!("{:?}Join({})", join.kind, join.table.name));
            let equi = matches!(
                join.on.as_ref(),
                Some(Expr::Binary(operator, left, right))
                    if operator == "="
                        && matches!(left.as_ref(), Expr::Column(_))
                        && matches!(right.as_ref(), Expr::Column(_))
            );
            let right_rows = table_rows(&join.table.name);
            estimated_rows = if equi {
                estimated_rows.max(right_rows)
            } else {
                estimated_rows.saturating_mul(right_rows)
            };
            self.physical.push(if self.optimizer_enabled && equi && !materialized_joins {
                format!(
                    "HashJoinExec(type={:?};table={};build=right;runtime_filter=true;estimated_rows={estimated_rows})",
                    join.kind, join.table.name,
                )
            } else {
                format!(
                    "NestedLoopJoinExec(type={:?};table={};estimated_rows={estimated_rows})",
                    join.kind, join.table.name,
                )
            });
        }
        if self.filter.is_some() {
            self.logical.push("Filter".into())
        }
        if aggregate {
            self.logical.push("Aggregate".into())
        } else {
            self.logical.push("Project".into())
        }
        if self.select.iter().any(|item| contains_window(&item.expr)) {
            self.logical.push("Window".into())
        }
        if self.select.iter().any(|item| contains_subquery(&item.expr))
            || self.filter.as_ref().is_some_and(contains_subquery)
        {
            self.logical.push("Subquery".into());
        }
        if self.having.is_some() {
            self.logical.push("Having".into())
        }
        if self.distinct {
            self.logical.push("Distinct".into())
        }
        if !self.order_by.is_empty() {
            self.logical
                .push(if self.optimizer_enabled && self.limit.is_some() {
                    "TopK".into()
                } else {
                    "Sort".into()
                })
        }
        if self.limit.is_some() {
            self.logical.push("Limit".into())
        }
        if self.filter.is_some() {
            self.physical.push("FilterExec(pushdown=true)".into())
        }
        if aggregate {
            self.physical
                .extend(["PartialAggregateExec".into(), "FinalAggregateExec".into()])
        } else {
            self.physical.push("ProjectExec".into())
        }
        if self.select.iter().any(|item| contains_window(&item.expr)) {
            self.physical.push("WindowExec(partition_sort=true)".into())
        }
        if self.select.iter().any(|item| contains_subquery(&item.expr))
            || self.filter.as_ref().is_some_and(contains_subquery)
        {
            self.physical.push("SubqueryExec(correlated=true)".into());
        }
        if self.having.is_some() {
            self.physical.push("HavingExec".into())
        }
        if self.distinct {
            self.physical.push("HashDistinctExec".into())
        }
        if !self.order_by.is_empty() {
            self.physical.push(
                if let (true, Some(limit)) = (self.optimizer_enabled, self.limit) {
                    format!("TopKExec(k={})", limit.saturating_add(self.offset))
                } else {
                    "SortExec".into()
                },
            )
        }
        if self.limit.is_some() {
            self.physical.push("LimitExec".into())
        }
        if self.union.is_some() {
            self.logical.push(if self.union_all {
                "UnionAll".into()
            } else {
                "UnionDistinct".into()
            });
            self.physical.push(if self.union_all {
                "UnionAllExec".into()
            } else {
                "UnionDistinctExec".into()
            });
        }
        if !self.ctes.is_empty() {
            self.logical
                .insert(0, format!("With(ctes={})", self.ctes.len()));
            self.physical
                .insert(0, format!("CteMaterializeExec(count={})", self.ctes.len()));
        }
        Ok(())
    }
    pub(crate) fn explain(&self, batch: usize) -> String {
        let mut x = self.physical.clone();
        x[0] = format!("{} batch_size={batch}", x[0]);
        x.into_iter()
            .rev()
            .enumerate()
            .map(|(i, s)| format!("{}{}", "  ".repeat(i), s))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

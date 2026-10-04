use super::*;

pub(super) fn estimated_filtered_rows(
    table: &str,
    filters: &[PushedFilter],
    event_rows: usize,
) -> usize {
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

use crate::relational::execute_rel;
use crate::sql::*;
use crate::storage::*;
use crate::types::*;
use std::cmp::Ordering;

pub(crate) fn cmp(a: &Scalar, b: &Scalar) -> Option<Ordering> {
    match (a, b) {
        (Scalar::Int(x), Scalar::Int(y)) => Some(x.cmp(y)),
        (Scalar::Decimal(x), Scalar::Decimal(y)) => Some(x.cmp(y)),
        (Scalar::Int(x), Scalar::Decimal(y)) => x.checked_mul(100).map(|x| x.cmp(y)),
        (Scalar::Decimal(x), Scalar::Int(y)) => y.checked_mul(100).map(|y| x.cmp(&y)),
        (Scalar::Float(x), Scalar::Float(y)) => x.partial_cmp(y),
        (Scalar::Int(x), Scalar::Float(y)) => (*x as f64).partial_cmp(y),
        (Scalar::Float(x), Scalar::Int(y)) => x.partial_cmp(&(*y as f64)),
        (Scalar::Decimal(x), Scalar::Float(y)) => (*x as f64 / 100.0).partial_cmp(y),
        (Scalar::Float(x), Scalar::Decimal(y)) => x.partial_cmp(&(*y as f64 / 100.0)),
        (Scalar::Str(x), Scalar::Str(y)) => Some(x.cmp(y)),
        (Scalar::Bool(x), Scalar::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

pub(crate) fn like_matches(value: &str, pattern: &str) -> bool {
    let value = value.as_bytes();
    let pattern = pattern.as_bytes();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;
    for &token in pattern {
        let mut current = vec![false; value.len() + 1];
        if token == b'%' {
            current[0] = previous[0];
            for index in 1..=value.len() {
                current[index] = previous[index] || current[index - 1];
            }
        } else {
            for index in 1..=value.len() {
                current[index] =
                    previous[index - 1] && (token == b'_' || token == value[index - 1]);
            }
        }
        previous = current;
    }
    previous[value.len()]
}

pub(crate) fn eval_call(name: &str, args: &[Expr], t: &Table, row: usize) -> Scalar {
    let values: Vec<_> = args.iter().map(|arg| eval(arg, t, row)).collect();
    eval_values(name, values)
}

pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let (roundtrip_year, roundtrip_month, roundtrip_day) = civil_from_days(days);
    (roundtrip_year == year && roundtrip_month == month && roundtrip_day == day).then_some(days)
}

pub(crate) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

pub(crate) fn parse_date(value: &str) -> Option<i64> {
    if value.len() != 10 || &value[4..5] != "-" || &value[7..8] != "-" {
        return None;
    }
    days_from_civil(
        value[0..4].parse().ok()?,
        value[5..7].parse().ok()?,
        value[8..10].parse().ok()?,
    )
}

pub(crate) fn format_date(days: i64) -> String {
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

pub(crate) fn parse_timestamp(value: &str) -> Option<i64> {
    let days = parse_date(value.get(0..10)?)?;
    if value.len() == 10 {
        return days.checked_mul(86_400);
    }
    let separator = value.as_bytes().get(10).copied()?;
    if separator != b'T' && separator != b' ' {
        return None;
    }
    let hour: i64 = value.get(11..13)?.parse().ok()?;
    let minute: i64 = value.get(14..16)?.parse().ok()?;
    let second: i64 = value.get(17..19)?.parse().ok()?;
    if value.get(13..14)? != ":"
        || value.get(16..17)? != ":"
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let suffix = value.get(19..).unwrap_or_default();
    if !suffix.is_empty()
        && suffix != "Z"
        && !(suffix.starts_with('.')
            && suffix
                .trim_start_matches('.')
                .trim_end_matches('Z')
                .chars()
                .all(|character| character.is_ascii_digit()))
    {
        return None;
    }
    days.checked_mul(86_400)?
        .checked_add(hour * 3600 + minute * 60 + second)
}

pub(crate) fn interval_seconds(value: &str) -> Option<i64> {
    let mut parts = value.split_whitespace();
    let amount: i64 = parts.next()?.parse().ok()?;
    let unit = parts.next()?.trim_end_matches('s');
    if parts.next().is_some() {
        return None;
    }
    amount.checked_mul(match unit {
        "microsecond" => 0,
        "second" => 1,
        "minute" => 60,
        "hour" => 3600,
        "day" => 86_400,
        "week" => 604_800,
        _ => return None,
    })
}

pub(crate) fn decimal_text(units: i64) -> String {
    format!(
        "{}{}.{:02}",
        if units < 0 { "-" } else { "" },
        units.unsigned_abs() / 100,
        units.unsigned_abs() % 100
    )
}

pub(crate) fn temporal_components(value: &Scalar) -> Option<(i64, i64, i64, i64, i64, i64)> {
    let seconds = match value {
        Scalar::Int(seconds) => *seconds,
        Scalar::Str(date) => parse_timestamp(date)?,
        _ => return None,
    };
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    Some((
        year,
        month,
        day,
        seconds_of_day / 3600,
        seconds_of_day / 60 % 60,
        seconds_of_day % 60,
    ))
}

pub(crate) fn eval_values(name: &str, values: Vec<Scalar>) -> Scalar {
    match name {
        "coalesce" => values
            .into_iter()
            .find(|value| !matches!(value, Scalar::Null))
            .unwrap_or(Scalar::Null),
        "nullif" if values.len() == 2 => {
            if cmp(&values[0], &values[1]) == Some(Ordering::Equal) {
                Scalar::Null
            } else {
                values[0].clone()
            }
        }
        "lower" if values.len() == 1 => match &values[0] {
            Scalar::Str(value) => Scalar::Str(value.to_ascii_lowercase()),
            _ => Scalar::Null,
        },
        "upper" if values.len() == 1 => match &values[0] {
            Scalar::Str(value) => Scalar::Str(value.to_ascii_uppercase()),
            _ => Scalar::Null,
        },
        "length" if values.len() == 1 => match &values[0] {
            Scalar::Str(value) => Scalar::Int(value.chars().count() as i64),
            _ => Scalar::Null,
        },
        "abs" if values.len() == 1 => match values[0] {
            Scalar::Int(value) => value.checked_abs().map_or(Scalar::Null, Scalar::Int),
            Scalar::Decimal(value) => value.checked_abs().map_or(Scalar::Null, Scalar::Decimal),
            Scalar::Float(value) => Scalar::Float(value.abs()),
            _ => Scalar::Null,
        },
        "concat" => {
            if values.iter().any(|value| matches!(value, Scalar::Null)) {
                Scalar::Null
            } else {
                Scalar::Str(
                    values
                        .iter()
                        .map(|value| match value {
                            Scalar::Str(text) => text.clone(),
                            Scalar::Int(number) => number.to_string(),
                            Scalar::Decimal(number) => decimal_text(*number),
                            Scalar::Float(number) => number.to_string(),
                            Scalar::Bool(boolean) => boolean.to_string(),
                            Scalar::Null => unreachable!(),
                        })
                        .collect(),
                )
            }
        }
        "substring" if values.len() == 2 || values.len() == 3 => {
            let (Scalar::Str(text), Scalar::Int(start)) = (&values[0], &values[1]) else {
                return Scalar::Null;
            };
            let start = (*start - 1).max(0) as usize;
            let length = if let Some(Scalar::Int(length)) = values.get(2) {
                (*length).max(0) as usize
            } else {
                usize::MAX
            };
            Scalar::Str(text.chars().skip(start).take(length).collect())
        }
        "date" if values.len() == 1 => match &values[0] {
            Scalar::Str(value) => {
                parse_date(value).map_or(Scalar::Null, |days| Scalar::Str(format_date(days)))
            }
            Scalar::Int(seconds) => Scalar::Str(format_date(seconds.div_euclid(86_400))),
            _ => Scalar::Null,
        },
        "timestamp" if values.len() == 1 => match &values[0] {
            Scalar::Str(value) => parse_timestamp(value).map_or(Scalar::Null, Scalar::Int),
            Scalar::Int(value) => Scalar::Int(*value),
            _ => Scalar::Null,
        },
        "interval" if values.len() == 1 => match &values[0] {
            Scalar::Str(value) => interval_seconds(value).map_or(Scalar::Null, Scalar::Int),
            _ => Scalar::Null,
        },
        "extract" if values.len() == 2 => {
            let Scalar::Str(field) = &values[0] else {
                return Scalar::Null;
            };
            let Some((year, month, day, hour, minute, second)) = temporal_components(&values[1])
            else {
                return Scalar::Null;
            };
            Scalar::Int(match field.as_str() {
                "year" => year,
                "month" => month,
                "day" => day,
                "hour" => hour,
                "minute" => minute,
                "second" => second,
                _ => return Scalar::Null,
            })
        }
        "date_trunc" if values.len() == 2 => {
            let Scalar::Str(unit) = &values[0] else {
                return Scalar::Null;
            };
            let Some((year, month, day, hour, minute, second)) = temporal_components(&values[1])
            else {
                return Scalar::Null;
            };
            let (month, day, hour, minute, second) = match unit.as_str() {
                "year" => (1, 1, 0, 0, 0),
                "month" => (month, 1, 0, 0, 0),
                "day" => (month, day, 0, 0, 0),
                "hour" => (month, day, hour, 0, 0),
                "minute" => (month, day, hour, minute, 0),
                "second" => (month, day, hour, minute, second),
                _ => return Scalar::Null,
            };
            let truncated = days_from_civil(year, month, day)
                .and_then(|days| days.checked_mul(86_400))
                .and_then(|seconds| seconds.checked_add(hour * 3600 + minute * 60 + second));
            match (&values[1], truncated) {
                (Scalar::Str(_), Some(seconds))
                    if unit == "year" || unit == "month" || unit == "day" =>
                {
                    Scalar::Str(format_date(seconds.div_euclid(86_400)))
                }
                (_, Some(seconds)) => Scalar::Int(seconds),
                _ => Scalar::Null,
            }
        }
        _ => Scalar::Null,
    }
}

pub(crate) fn cast_value(data_type: &str, value: Scalar) -> Scalar {
    match (data_type, value) {
        (_, Scalar::Null) => Scalar::Null,
        ("bigint" | "int64" | "integer", Scalar::Int(value)) => Scalar::Int(value),
        ("bigint" | "int64" | "integer", Scalar::Decimal(value)) => Scalar::Int(value / 100),
        ("bigint" | "int64" | "integer", Scalar::Float(value))
            if value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64 =>
        {
            Scalar::Int(value.trunc() as i64)
        }
        ("bigint" | "int64" | "integer", Scalar::Str(value)) => {
            value.parse().map_or(Scalar::Null, Scalar::Int)
        }
        ("double" | "float" | "real", Scalar::Int(value)) => Scalar::Float(value as f64),
        ("double" | "float" | "real", Scalar::Decimal(value)) => {
            Scalar::Float(value as f64 / 100.0)
        }
        ("double" | "float" | "real", Scalar::Float(value)) => Scalar::Float(value),
        ("double" | "float" | "real", Scalar::Str(value)) => {
            value.parse().map_or(Scalar::Null, Scalar::Float)
        }
        ("varchar" | "string" | "text", Scalar::Str(value)) => Scalar::Str(value),
        ("varchar" | "string" | "text", Scalar::Int(value)) => Scalar::Str(value.to_string()),
        ("varchar" | "string" | "text", Scalar::Decimal(value)) => Scalar::Str(decimal_text(value)),
        ("varchar" | "string" | "text", Scalar::Float(value)) => Scalar::Str(value.to_string()),
        ("varchar" | "string" | "text", Scalar::Bool(value)) => Scalar::Str(value.to_string()),
        ("boolean" | "bool", Scalar::Bool(value)) => Scalar::Bool(value),
        ("boolean" | "bool", Scalar::Str(value)) if value == "true" => Scalar::Bool(true),
        ("boolean" | "bool", Scalar::Str(value)) if value == "false" => Scalar::Bool(false),
        ("date", Scalar::Str(value)) => {
            parse_date(&value).map_or(Scalar::Null, |days| Scalar::Str(format_date(days)))
        }
        ("date", Scalar::Int(seconds)) => Scalar::Str(format_date(seconds.div_euclid(86_400))),
        ("timestamp", Scalar::Str(value)) => {
            parse_timestamp(&value).map_or(Scalar::Null, Scalar::Int)
        }
        ("timestamp", Scalar::Int(value)) => Scalar::Int(value),
        (data_type, Scalar::Decimal(value)) if data_type.starts_with("decimal") => {
            Scalar::Decimal(value)
        }
        (data_type, Scalar::Int(value)) if data_type.starts_with("decimal") => {
            value.checked_mul(100).map_or(Scalar::Null, Scalar::Decimal)
        }
        (data_type, Scalar::Float(value)) if data_type.starts_with("decimal") => {
            let scaled = value * 100.0;
            if scaled.is_finite() && scaled >= i64::MIN as f64 && scaled <= i64::MAX as f64 {
                Scalar::Decimal(scaled.round() as i64)
            } else {
                Scalar::Null
            }
        }
        (data_type, Scalar::Str(value)) if data_type.starts_with("decimal") => {
            parse_decimal_cents(&value).map_or_else(
                |_| {
                    value.parse::<f64>().map_or(Scalar::Null, |value| {
                        Scalar::Decimal((value * 100.0).round() as i64)
                    })
                },
                Scalar::Decimal,
            )
        }
        _ => Scalar::Null,
    }
}

pub(crate) fn apply_binary(op: &str, x: Scalar, y: Scalar) -> Scalar {
    if op == "and" {
        return match (x.sql_bool(), y.sql_bool()) {
            (Some(false), _) | (_, Some(false)) => Scalar::Bool(false),
            (Some(true), Some(true)) => Scalar::Bool(true),
            _ => Scalar::Null,
        };
    }
    if op == "or" {
        return match (x.sql_bool(), y.sql_bool()) {
            (Some(true), _) | (_, Some(true)) => Scalar::Bool(true),
            (Some(false), Some(false)) => Scalar::Bool(false),
            _ => Scalar::Null,
        };
    }
    if matches!(x, Scalar::Null) || matches!(y, Scalar::Null) {
        return Scalar::Null;
    }
    if (op == "+" || op == "-")
        && let (Scalar::Str(date), Scalar::Int(interval)) = (&x, &y)
        && let Some(days) = parse_date(date)
        && interval % 86_400 == 0
    {
        let delta = if op == "+" { *interval } else { -*interval };
        return days
            .checked_add(delta / 86_400)
            .map_or(Scalar::Null, |days| Scalar::Str(format_date(days)));
    }
    match op {
        "=" => Scalar::Bool(cmp(&x, &y) == Some(Ordering::Equal)),
        "!=" => Scalar::Bool(cmp(&x, &y) != Some(Ordering::Equal)),
        "<" => Scalar::Bool(cmp(&x, &y) == Some(Ordering::Less)),
        "<=" => Scalar::Bool(matches!(
            cmp(&x, &y),
            Some(Ordering::Less | Ordering::Equal)
        )),
        ">" => Scalar::Bool(cmp(&x, &y) == Some(Ordering::Greater)),
        ">=" => Scalar::Bool(matches!(
            cmp(&x, &y),
            Some(Ordering::Greater | Ordering::Equal)
        )),
        "+" | "-" | "*" | "/" => {
            if op != "/" {
                let decimal_operands = match (&x, &y) {
                    (Scalar::Decimal(left), Scalar::Decimal(right)) => Some((*left, *right)),
                    (Scalar::Decimal(left), Scalar::Int(right)) => {
                        right.checked_mul(100).map(|right| (*left, right))
                    }
                    (Scalar::Int(left), Scalar::Decimal(right)) => {
                        left.checked_mul(100).map(|left| (left, *right))
                    }
                    _ => None,
                };
                if let Some((left, right)) = decimal_operands {
                    let units = match op {
                        "+" => left.checked_add(right).map(i128::from),
                        "-" => left.checked_sub(right).map(i128::from),
                        _ => Some(i128::from(left) * i128::from(right) / 100),
                    };
                    return units
                        .and_then(|units| i64::try_from(units).ok())
                        .map_or(Scalar::Null, Scalar::Decimal);
                }
            }
            let Some(nx) = x.number() else {
                return Scalar::Null;
            };
            let Some(ny) = y.number() else {
                return Scalar::Null;
            };
            if op == "/" && ny == 0.0 {
                return Scalar::Null;
            }
            if op != "/"
                && let (Scalar::Int(ix), Scalar::Int(iy)) = (&x, &y)
            {
                return match op {
                    "+" => ix.checked_add(*iy),
                    "-" => ix.checked_sub(*iy),
                    _ => ix.checked_mul(*iy),
                }
                .map_or(Scalar::Null, Scalar::Int);
            }
            Scalar::Float(match op {
                "+" => nx + ny,
                "-" => nx - ny,
                "*" => nx * ny,
                _ => nx / ny,
            })
        }
        _ => Scalar::Null,
    }
}

pub(crate) fn eval(e: &Expr, t: &Table, i: usize) -> Scalar {
    match e {
        Expr::Null => Scalar::Null,
        Expr::Column(c) => t.scalar(c, i),
        Expr::Int(v) => Scalar::Int(*v),
        Expr::Float(v) => Scalar::Float(*v),
        Expr::Bool(v) => Scalar::Bool(*v),
        Expr::String(v) => Scalar::Str(v.clone()),
        Expr::Star => Scalar::Int(1),
        Expr::DictEq(c, id, neg) => {
            let v = match c.as_str() {
                "country" => t.country[i],
                "device" => t.device[i],
                "event_type" => t.event_type[i],
                _ => u32::MAX,
            };
            Scalar::Bool((v == *id) ^ *neg)
        }
        Expr::IsNull(a, neg) => Scalar::Bool(matches!(eval(a, t, i), Scalar::Null) ^ *neg),
        Expr::Unary(op, a) => {
            let v = eval(a, t, i);
            if op == "not" {
                v.sql_bool()
                    .map_or(Scalar::Null, |value| Scalar::Bool(!value))
            } else {
                match v {
                    Scalar::Int(x) => x.checked_neg().map_or(Scalar::Null, Scalar::Int),
                    Scalar::Decimal(x) => x.checked_neg().map_or(Scalar::Null, Scalar::Decimal),
                    Scalar::Float(x) => Scalar::Float(-x),
                    _ => Scalar::Null,
                }
            }
        }
        Expr::Func(name, arg) => eval_call(name, std::slice::from_ref(arg), t, i),
        Expr::Call(name, args) => eval_call(name, args, t, i),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if eval(condition, t, i).sql_bool() == Some(true) {
                    return eval(value, t, i);
                }
            }
            eval(fallback, t, i)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, eval(value, t, i)),
        Expr::InList(value, candidates, negated) => {
            let value = eval(value, t, i);
            if matches!(value, Scalar::Null) {
                return Scalar::Null;
            }
            let mut saw_null = false;
            for candidate in candidates {
                let candidate = eval(candidate, t, i);
                if matches!(candidate, Scalar::Null) {
                    saw_null = true;
                } else if cmp(&value, &candidate) == Some(Ordering::Equal) {
                    return Scalar::Bool(!negated);
                }
            }
            if saw_null {
                Scalar::Null
            } else {
                Scalar::Bool(*negated)
            }
        }
        Expr::Between(value, low, high, negated) => {
            let value = eval(value, t, i);
            let low = eval(low, t, i);
            let high = eval(high, t, i);
            if matches!(value, Scalar::Null)
                || matches!(low, Scalar::Null)
                || matches!(high, Scalar::Null)
            {
                Scalar::Null
            } else {
                let inside = matches!(cmp(&value, &low), Some(Ordering::Equal | Ordering::Greater))
                    && matches!(cmp(&value, &high), Some(Ordering::Equal | Ordering::Less));
                Scalar::Bool(inside ^ *negated)
            }
        }
        Expr::Like(value, pattern, negated) => match (eval(value, t, i), eval(pattern, t, i)) {
            (Scalar::Str(value), Scalar::Str(pattern)) => {
                Scalar::Bool(like_matches(&value, &pattern) ^ *negated)
            }
            _ => Scalar::Null,
        },
        Expr::Window { .. } => Scalar::Null,
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_, _, _) => Scalar::Null,
        Expr::Binary(op, a, b) => {
            let left = eval(a, t, i);
            if (op == "and" && left.sql_bool() == Some(false))
                || (op == "or" && left.sql_bool() == Some(true))
            {
                left
            } else {
                apply_binary(op, left, eval(b, t, i))
            }
        }
    }
}

pub(crate) fn scalar_expression(value: Scalar) -> Expr {
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

pub(crate) fn substitute_outer_expr(
    expression: &mut Expr,
    local: &std::collections::HashSet<String>,
    catalog: &Catalog,
    row: RelRow,
    outer_bindings: &std::collections::HashMap<String, String>,
) {
    if let Expr::Column(column) = expression
        && let Some((qualifier, _)) = column.split_once('.')
        && !local.contains(qualifier)
        && let Ok((table, name)) = resolve_column(column, outer_bindings)
    {
        *expression = scalar_expression(relation_scalar(catalog, row, &table, &name));
        return;
    }
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            substitute_outer_expr(value, local, catalog, row, outer_bindings)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            substitute_outer_expr(left, local, catalog, row, outer_bindings);
            substitute_outer_expr(right, local, catalog, row, outer_bindings);
        }
        Expr::Call(_, args) => args
            .iter_mut()
            .for_each(|arg| substitute_outer_expr(arg, local, catalog, row, outer_bindings)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                substitute_outer_expr(condition, local, catalog, row, outer_bindings);
                substitute_outer_expr(value, local, catalog, row, outer_bindings);
            }
            substitute_outer_expr(fallback, local, catalog, row, outer_bindings);
        }
        Expr::Cast(value, _) => substitute_outer_expr(value, local, catalog, row, outer_bindings),
        Expr::InList(value, values, _) => {
            substitute_outer_expr(value, local, catalog, row, outer_bindings);
            values
                .iter_mut()
                .for_each(|item| substitute_outer_expr(item, local, catalog, row, outer_bindings));
        }
        Expr::Between(value, low, high, _) => {
            substitute_outer_expr(value, local, catalog, row, outer_bindings);
            substitute_outer_expr(low, local, catalog, row, outer_bindings);
            substitute_outer_expr(high, local, catalog, row, outer_bindings);
        }
        Expr::Window { args, .. } => args
            .iter_mut()
            .for_each(|arg| substitute_outer_expr(arg, local, catalog, row, outer_bindings)),
        Expr::InSubquery(value, _, _) => {
            substitute_outer_expr(value, local, catalog, row, outer_bindings)
        }
        _ => {}
    }
}

pub(crate) fn execute_subquery(
    query: &Query,
    catalog: &Catalog,
    row: RelRow,
    outer_bindings: &std::collections::HashMap<String, String>,
) -> Vec<Vec<Scalar>> {
    let mut query = query.clone();
    let mut local = std::collections::HashSet::new();
    for table in std::iter::once(&query.from).chain(query.joins.iter().map(|join| &join.table)) {
        local.insert(table.name.clone());
        local.insert(table.alias.clone());
    }
    for item in &mut query.select {
        substitute_outer_expr(&mut item.expr, &local, catalog, row, outer_bindings);
    }
    if let Some(filter) = &mut query.filter {
        substitute_outer_expr(filter, &local, catalog, row, outer_bindings);
    }
    for join in &mut query.joins {
        if let Some(on) = &mut join.on {
            substitute_outer_expr(on, &local, catalog, row, outer_bindings);
        }
    }
    if let Some(having) = &mut query.having {
        substitute_outer_expr(having, &local, catalog, row, outer_bindings);
    }
    execute_rel(&query, catalog).unwrap_or_default()
}

pub(crate) fn simple_campaign_lookup(
    query: &Query,
    catalog: &Catalog,
    row: RelRow,
    outer_bindings: &std::collections::HashMap<String, String>,
) -> Option<Option<usize>> {
    if query.from.name != "campaigns"
        || !query.joins.is_empty()
        || !query.group_by.is_empty()
        || query.having.is_some()
        || query.union.is_some()
        || !query.ctes.is_empty()
    {
        return None;
    }
    let Expr::Binary(operator, left, right) = query.filter.as_ref()? else {
        return None;
    };
    if operator != "=" {
        return None;
    }
    let is_local_id = |expression: &Expr| {
        let Expr::Column(column) = expression else {
            return false;
        };
        let Some((qualifier, name)) = column.split_once('.') else {
            return false;
        };
        name == "campaign_id" && (qualifier == query.from.name || qualifier == query.from.alias)
    };
    let outer = if is_local_id(left) {
        right.as_ref()
    } else if is_local_id(right) {
        left.as_ref()
    } else {
        return None;
    };
    let Expr::Column(column) = outer else {
        return None;
    };
    let (table, column) = resolve_column(column, outer_bindings).ok()?;
    match relation_scalar(catalog, row, &table, &column) {
        Scalar::Int(value) => Some(
            catalog
                .campaigns
                .index
                .get(&value)
                .and_then(|indices| indices.first().copied()),
        ),
        Scalar::Null => Some(None),
        _ => None,
    }
}

pub(crate) fn simple_campaign_max_budget(
    query: &Query,
    catalog: &Catalog,
    row: RelRow,
    outer_bindings: &std::collections::HashMap<String, String>,
) -> Option<Scalar> {
    if query.from.name != "campaigns" || query.select.len() != 1 || !query.joins.is_empty() {
        return None;
    }
    let Expr::Func(name, argument) = &query.select[0].expr else {
        return None;
    };
    if name != "max"
        || !matches!(argument.as_ref(), Expr::Column(column) if column.rsplit('.').next() == Some("budget"))
    {
        return None;
    }
    if query.filter.is_none() {
        return Some(
            (0..catalog.campaigns.row_count())
                .filter_map(|index| match catalog.campaigns.scalar("budget", index) {
                    Scalar::Decimal(value) => Some(value),
                    _ => None,
                })
                .max()
                .map_or(Scalar::Null, Scalar::Decimal),
        );
    }
    simple_campaign_lookup(query, catalog, row, outer_bindings).map(|index| {
        index.map_or(Scalar::Null, |index| {
            catalog.campaigns.scalar("budget", index)
        })
    })
}

pub(crate) fn simple_campaign_id_membership(
    query: &Query,
    value: &Scalar,
    catalog: &Catalog,
) -> Option<bool> {
    if query.from.name != "campaigns"
        || query.select.len() != 1
        || query.filter.is_some()
        || !query.joins.is_empty()
        || !matches!(&query.select[0].expr, Expr::Column(column) if column.rsplit('.').next() == Some("campaign_id"))
    {
        return None;
    }
    match value {
        Scalar::Int(value) => Some(catalog.campaigns.index.contains_key(value)),
        _ => None,
    }
}

pub(crate) fn eval_rel(
    expression: &Expr,
    catalog: &Catalog,
    row: RelRow,
    bindings: &std::collections::HashMap<String, String>,
) -> Scalar {
    match expression {
        Expr::Null => Scalar::Null,
        Expr::Column(column) => resolve_column(column, bindings)
            .map_or(Scalar::Null, |(table, name)| {
                relation_scalar(catalog, row, &table, &name)
            }),
        Expr::Int(value) => Scalar::Int(*value),
        Expr::Float(value) => Scalar::Float(*value),
        Expr::Bool(value) => Scalar::Bool(*value),
        Expr::String(value) => Scalar::Str(value.clone()),
        Expr::Star => Scalar::Int(1),
        Expr::DictEq(column, id, negated) => {
            let Some(event) = row.event else {
                return Scalar::Null;
            };
            let value = match column.rsplit('.').next().unwrap_or(column) {
                "country" => catalog.events.country[event],
                "device" => catalog.events.device[event],
                "event_type" => catalog.events.event_type[event],
                _ => return Scalar::Null,
            };
            Scalar::Bool((value == *id) ^ *negated)
        }
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(eval_rel(value, catalog, row, bindings), Scalar::Null) ^ *negated)
        }
        Expr::Unary(operator, value) => {
            let value = eval_rel(value, catalog, row, bindings);
            if operator == "not" {
                value
                    .sql_bool()
                    .map_or(Scalar::Null, |value| Scalar::Bool(!value))
            } else {
                match value {
                    Scalar::Int(value) => value.checked_neg().map_or(Scalar::Null, Scalar::Int),
                    Scalar::Decimal(value) => {
                        value.checked_neg().map_or(Scalar::Null, Scalar::Decimal)
                    }
                    Scalar::Float(value) => Scalar::Float(-value),
                    _ => Scalar::Null,
                }
            }
        }
        Expr::Binary(operator, left, right) => {
            let left = eval_rel(left, catalog, row, bindings);
            if (operator == "and" && left.sql_bool() == Some(false))
                || (operator == "or" && left.sql_bool() == Some(true))
            {
                return left;
            }
            apply_binary(operator, left, eval_rel(right, catalog, row, bindings))
        }
        Expr::Func(name, argument) => {
            eval_values(name, vec![eval_rel(argument, catalog, row, bindings)])
        }
        Expr::Call(name, arguments) => eval_values(
            name,
            arguments
                .iter()
                .map(|argument| eval_rel(argument, catalog, row, bindings))
                .collect(),
        ),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if eval_rel(condition, catalog, row, bindings).sql_bool() == Some(true) {
                    return eval_rel(value, catalog, row, bindings);
                }
            }
            eval_rel(fallback, catalog, row, bindings)
        }
        Expr::Cast(value, data_type) => {
            cast_value(data_type, eval_rel(value, catalog, row, bindings))
        }
        Expr::InList(value, candidates, negated) => {
            let value = eval_rel(value, catalog, row, bindings);
            if matches!(value, Scalar::Null) {
                return Scalar::Null;
            }
            let mut saw_null = false;
            for candidate in candidates {
                let candidate = eval_rel(candidate, catalog, row, bindings);
                if matches!(candidate, Scalar::Null) {
                    saw_null = true;
                } else if cmp(&value, &candidate) == Some(Ordering::Equal) {
                    return Scalar::Bool(!negated);
                }
            }
            if saw_null {
                Scalar::Null
            } else {
                Scalar::Bool(*negated)
            }
        }
        Expr::Between(value, low, high, negated) => {
            let value = eval_rel(value, catalog, row, bindings);
            let low = eval_rel(low, catalog, row, bindings);
            let high = eval_rel(high, catalog, row, bindings);
            if matches!(value, Scalar::Null)
                || matches!(low, Scalar::Null)
                || matches!(high, Scalar::Null)
            {
                Scalar::Null
            } else {
                let inside = matches!(cmp(&value, &low), Some(Ordering::Equal | Ordering::Greater))
                    && matches!(cmp(&value, &high), Some(Ordering::Equal | Ordering::Less));
                Scalar::Bool(inside ^ *negated)
            }
        }
        Expr::Like(value, pattern, negated) => match (
            eval_rel(value, catalog, row, bindings),
            eval_rel(pattern, catalog, row, bindings),
        ) {
            (Scalar::Str(value), Scalar::Str(pattern)) => {
                Scalar::Bool(like_matches(&value, &pattern) ^ *negated)
            }
            _ => Scalar::Null,
        },
        Expr::ScalarSubquery(query) => simple_campaign_max_budget(query, catalog, row, bindings)
            .unwrap_or_else(|| {
                execute_subquery(query, catalog, row, bindings)
                    .first()
                    .and_then(|result| result.first())
                    .cloned()
                    .unwrap_or(Scalar::Null)
            }),
        Expr::Exists(query) => simple_campaign_lookup(query, catalog, row, bindings).map_or_else(
            || Scalar::Bool(!execute_subquery(query, catalog, row, bindings).is_empty()),
            |index| Scalar::Bool(index.is_some()),
        ),
        Expr::InSubquery(value, query, negated) => {
            let value = eval_rel(value, catalog, row, bindings);
            if matches!(value, Scalar::Null) {
                return Scalar::Null;
            }
            if let Some(found) = simple_campaign_id_membership(query, &value, catalog) {
                return Scalar::Bool(found ^ *negated);
            }
            let mut saw_null = false;
            for candidate in execute_subquery(query, catalog, row, bindings)
                .into_iter()
                .filter_map(|row| row.into_iter().next())
            {
                if matches!(candidate, Scalar::Null) {
                    saw_null = true;
                } else if cmp(&value, &candidate) == Some(Ordering::Equal) {
                    return Scalar::Bool(!negated);
                }
            }
            if saw_null {
                Scalar::Null
            } else {
                Scalar::Bool(*negated)
            }
        }
        Expr::Window { .. } => Scalar::Null,
    }
}

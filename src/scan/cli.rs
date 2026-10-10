use super::run_parquet_scan;
use std::collections::HashMap;

pub fn run_scan_cli(args: &[String]) -> Result<(), String> {
    if args == ["--help"] {
        println!(
            "dremel scan --data PATH.parquet [--columns profile.city,items.price] [--row-groups 0,1|none] [--batch-size N] [--output PATH.arrow]"
        );
        return Ok(());
    }
    let mut values = HashMap::new();
    let mut index = 0;
    while index < args.len() {
        let key = args[index].as_str();
        if ![
            "--data",
            "--columns",
            "--row-groups",
            "--batch-size",
            "--output",
        ]
        .contains(&key)
        {
            return Err(format!("unsupported scan option {key}"));
        }
        let value = args
            .get(index + 1)
            .filter(|value| !value.starts_with("--"))
            .ok_or_else(|| format!("missing value for {key}"))?;
        if values.insert(key, value.as_str()).is_some() {
            return Err(format!("duplicate scan option {key}"));
        }
        index += 2;
    }
    let data = values
        .get("--data")
        .ok_or("scan requires --data PATH.parquet")?;
    let columns = values
        .get("--columns")
        .map(|value| value.split(',').map(str::to_owned).collect())
        .unwrap_or_default();
    let groups = values
        .get("--row-groups")
        .map(|value| {
            if *value == "none" {
                return Ok(Vec::new());
            }
            value
                .split(',')
                .map(|value| {
                    value
                        .parse::<usize>()
                        .map_err(|_| "invalid --row-groups index")
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let batch_size = values
        .get("--batch-size")
        .unwrap_or(&"4096")
        .parse()
        .map_err(|_| "invalid --batch-size")?;
    run_parquet_scan(
        data,
        columns,
        groups,
        batch_size,
        values.get("--output").copied(),
    )
}

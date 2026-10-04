use crate::execution::aggregate::*;
use crate::execution::*;
use crate::nested::*;
use crate::optimizer::*;
use crate::relational::*;
use crate::sql::*;
use crate::storage::*;
use crate::types::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
#[test]
fn typed_json_is_canonical_and_escaped() {
    assert_eq!(
        Scalar::Float(0.0005).json(),
        "{\"t\":\"f\",\"v\":5.0000000000000001e-4}"
    );
    assert_eq!(
        Scalar::Str("line\n\t\"\\".into()).json(),
        "{\"t\":\"s\",\"v\":\"line\\n\\t\\\"\\\\\"}"
    );
}
#[test]
fn lexer_and_parser() {
    let q=Parser::new("SELECT event_id, duration_ms*2 AS x FROM events WHERE NOT success = false AND campaign_id IS NOT NULL LIMIT 3;").unwrap().parse().unwrap();
    assert_eq!(q.select.len(), 2);
    assert_eq!(q.limit, Some(3));
    assert!(q.logical.contains(&"Filter".into()));
}
#[test]
fn precedence() {
    let q = Parser::new(
        "SELECT COUNT(*) FROM events WHERE duration_ms < 10 OR bytes > 5 AND success = true",
    )
    .unwrap()
    .parse()
    .unwrap();
    let Some(Expr::Binary(op, _, b)) = q.filter else {
        panic!()
    };
    assert_eq!(op, "or");
    assert!(matches!(*b,Expr::Binary(ref x,_,_)if x=="and"));
}
#[test]
fn group_table_resizes() {
    let s = vec![AggState::Count(0)];
    let mut t = GroupTable::new().unwrap();
    for i in 0..1000 {
        t.get_or_insert(GroupKey { v: [i, 0, 0], n: 1 }, &s)
            .unwrap();
    }
    assert_eq!(t.len, 1000);
}
#[test]
fn query_memory_reservations_release_and_retain_peak() {
    let memory = QueryMemory::new(1);
    assert!(memory.try_account(256 * 1024));
    assert_eq!(memory.accounted_bytes(), 256 * 1024);
    memory.release(128 * 1024);
    assert_eq!(memory.accounted_bytes(), 128 * 1024);
    assert_eq!(memory.peak_accounted_bytes(), 256 * 1024);
    assert!(!memory.try_account(1024 * 1024));
}
#[test]
fn dictionary_and_nullable() {
    let mut d = Dictionary::default();
    assert_eq!(d.insert("IN"), 0);
    assert_eq!(d.insert("IN"), 0);
    assert_eq!(d.insert("US"), 1);
}
fn fixture() -> Arc<Table> {
    let path = std::env::temp_dir().join(format!(
        "dremel-test-{}-{}.csv",
        std::process::id(),
        FIXTURE_ID.fetch_add(1, AtomicOrdering::Relaxed)
    ));
    std::fs::write(&path, concat!(
        "event_id,user_id,timestamp,country,device,event_type,duration_ms,bytes,score,success,campaign_id\n",
        "1,10,100,IN,mobile,view,10,100,1.5,true,7\n",
        "2,10,101,US,desktop,click,20,200,2.5,false,\n",
        "3,11,102,IN,mobile,click,30,300,3.5,true,9\n",
        "4,12,103,US,mobile,view,40,400,4.5,false,11\n",
    )).unwrap();
    let table = Arc::new(Table::load(path.to_str().unwrap()).unwrap());
    std::fs::remove_file(path).unwrap();
    table
}
fn run(sql: &str, table: Arc<Table>, threads: usize) -> Vec<Vec<Scalar>> {
    let query = prepare(Parser::new(sql).unwrap().parse().unwrap(), &table);
    execute(&query, table, &Pool::new(threads), 2).expect("query executes")
}
#[test]
fn execution_operators_and_parallel_equivalence() {
    let table = fixture();
    assert_eq!(
        run("SELECT COUNT(*) FROM events", table.clone(), 1),
        vec![vec![Scalar::Int(4)]]
    );
    assert_eq!(
        run("SELECT COUNT(campaign_id) FROM events", table.clone(), 2),
        vec![vec![Scalar::Int(3)]]
    );
    assert_eq!(
        run(
            "SELECT SUM(bytes), AVG(duration_ms), MIN(score), MAX(score) FROM events",
            table.clone(),
            2
        ),
        vec![vec![
            Scalar::Int(1000),
            Scalar::Float(25.0),
            Scalar::Float(1.5),
            Scalar::Float(4.5)
        ]]
    );
    let projection = run(
        "SELECT event_id, bytes + duration_ms AS metric FROM events WHERE country = 'IN' ORDER BY event_id DESC LIMIT 1",
        table.clone(),
        1,
    );
    assert_eq!(projection, vec![vec![Scalar::Int(3), Scalar::Int(330)]]);
    assert_eq!(
        run(
            "SELECT campaign_id > 1 AS present FROM events LIMIT 2",
            table.clone(),
            1
        ),
        vec![vec![Scalar::Bool(true)], vec![Scalar::Null]],
    );
    assert_eq!(
        run(
            "SELECT NOT (campaign_id > 1) AS low FROM events LIMIT 2",
            table.clone(),
            1
        ),
        vec![vec![Scalar::Bool(false)], vec![Scalar::Null]],
    );
    let grouped_sql = "SELECT country, device, COUNT(*) AS cnt FROM events GROUP BY country, device ORDER BY country ASC";
    let single = run(grouped_sql, table.clone(), 1);
    let multi = run(grouped_sql, table, 3);
    assert_eq!(single, multi);
    assert_eq!(single.len(), 3);
}
#[test]
fn production_scalar_expressions() {
    let table = fixture();
    assert_eq!(
        run(
            "SELECT events.event_id, CASE WHEN score BETWEEN 2.0 AND 4.0 THEN upper(country) ELSE 'other' END AS bucket FROM events ORDER BY event_id ASC",
            table.clone(),
            1,
        ),
        vec![
            vec![Scalar::Int(1), Scalar::Str("other".into())],
            vec![Scalar::Int(2), Scalar::Str("US".into())],
            vec![Scalar::Int(3), Scalar::Str("IN".into())],
            vec![Scalar::Int(4), Scalar::Str("other".into())],
        ]
    );
    assert_eq!(
        run(
            "SELECT event_id FROM events WHERE country IN ('IN', 'GB') AND event_type LIKE 'cl_ck' ORDER BY event_id ASC",
            table.clone(),
            1,
        ),
        vec![vec![Scalar::Int(3)]]
    );
    assert_eq!(
        run(
            "SELECT event_id, campaign_id NOT IN (7, NULL) AS allowed FROM events ORDER BY event_id ASC",
            table.clone(),
            1,
        ),
        vec![
            vec![Scalar::Int(1), Scalar::Bool(false)],
            vec![Scalar::Int(2), Scalar::Null],
            vec![Scalar::Int(3), Scalar::Null],
            vec![Scalar::Int(4), Scalar::Null],
        ]
    );
    assert_eq!(
        run(
            "SELECT concat(lower(country), '-', cast(event_id AS varchar)) AS label FROM events WHERE event_id = 1",
            table,
            1,
        ),
        vec![vec![Scalar::Str("in-1".into())]]
    );
}
#[test]
fn relational_hash_join_and_having() {
    let events = fixture();
    let mut users = UsersTable {
        user_id: vec![10, 11, 12],
        segment: vec!["pro".into(), "free".into(), "free".into()],
        signup_date: vec!["2024-01-01".into(); 3],
        lifetime_value: vec![1000, 2000, 3000],
        region: vec!["apac".into(); 3],
        active: vec![true, false, true],
        index: std::collections::HashMap::new(),
    };
    for (row, &id) in users.user_id.iter().enumerate() {
        users.index.entry(id).or_default().push(row);
    }
    let catalog = Catalog {
        events: events.clone(),
        users,
        campaigns: CampaignsTable::default(),
    };
    let joined = prepare(
        Parser::new("SELECT e.event_id, u.segment FROM events e INNER JOIN users u ON e.user_id = u.user_id WHERE u.active = true ORDER BY event_id ASC")
            .unwrap()
            .parse()
            .unwrap(),
        &events,
    );
    assert_eq!(
        execute_rel(&joined, &catalog).unwrap(),
        vec![
            vec![Scalar::Int(1), Scalar::Str("pro".into())],
            vec![Scalar::Int(2), Scalar::Str("pro".into())],
            vec![Scalar::Int(4), Scalar::Str("free".into())],
        ]
    );
    let grouped = prepare(
        Parser::new("SELECT u.segment, COUNT(*) AS cnt FROM events e JOIN users u ON e.user_id = u.user_id GROUP BY u.segment HAVING COUNT(*) >= 1 ORDER BY u.segment ASC")
            .unwrap()
            .parse()
            .unwrap(),
        &events,
    );
    assert_eq!(
        execute_rel(&grouped, &catalog).unwrap(),
        vec![
            vec![Scalar::Str("free".into()), Scalar::Int(2)],
            vec![Scalar::Str("pro".into()), Scalar::Int(2)],
        ]
    );
}
#[test]
fn window_partition_order_and_frames() {
    let events = fixture();
    let catalog = Catalog {
        events: events.clone(),
        users: UsersTable::default(),
        campaigns: CampaignsTable::default(),
    };
    let ranked = prepare(
        Parser::new("SELECT event_id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC) AS rn, SUM(bytes) OVER (PARTITION BY country ORDER BY event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bytes FROM events ORDER BY event_id ASC")
            .unwrap()
            .parse()
            .unwrap(),
        &events,
    );
    assert_eq!(
        execute_rel(&ranked, &catalog).unwrap(),
        vec![
            vec![Scalar::Int(1), Scalar::Int(2), Scalar::Int(100)],
            vec![Scalar::Int(2), Scalar::Int(2), Scalar::Int(200)],
            vec![Scalar::Int(3), Scalar::Int(1), Scalar::Int(400)],
            vec![Scalar::Int(4), Scalar::Int(1), Scalar::Int(600)],
        ]
    );
    let lagged = prepare(
        Parser::new("SELECT event_id, LAG(bytes, 1, 0) OVER (PARTITION BY user_id ORDER BY timestamp ASC) AS previous_bytes FROM events ORDER BY event_id ASC")
            .unwrap()
            .parse()
            .unwrap(),
        &events,
    );
    assert_eq!(
        execute_rel(&lagged, &catalog).unwrap(),
        vec![
            vec![Scalar::Int(1), Scalar::Int(0)],
            vec![Scalar::Int(2), Scalar::Int(100)],
            vec![Scalar::Int(3), Scalar::Int(0)],
            vec![Scalar::Int(4), Scalar::Int(0)],
        ]
    );
}
#[test]
fn materialized_cte() {
    let events = fixture();
    let catalog = Catalog {
        events: events.clone(),
        users: UsersTable::default(),
        campaigns: CampaignsTable::default(),
    };
    let query = prepare(
        Parser::new("WITH totals AS (SELECT country, SUM(bytes) AS total FROM events GROUP BY country) SELECT country, total FROM totals WHERE total > 200 ORDER BY country ASC")
            .unwrap()
            .parse()
            .unwrap(),
        &events,
    );
    assert_eq!(
        execute_rel(&query, &catalog).unwrap(),
        vec![
            vec![Scalar::Str("IN".into()), Scalar::Int(400)],
            vec![Scalar::Str("US".into()), Scalar::Int(600)],
        ]
    );
}
#[test]
fn scalar_exists_in_and_correlated_subqueries() {
    let events = fixture();
    let catalog = Catalog {
        events: events.clone(),
        users: UsersTable::default(),
        campaigns: CampaignsTable {
            campaign_id: vec![7, 9],
            campaign_name: vec!["seven".into(), "nine".into()],
            budget: vec![7000, 9000],
            start_date: vec!["2024-01-01".into(); 2],
            end_date: vec!["2024-02-01".into(); 2],
            channel: vec!["search".into(); 2],
            index: std::collections::HashMap::new(),
        },
    };
    let scalar = prepare(
        Parser::new("SELECT event_id, (SELECT MAX(budget) FROM campaigns) AS max_budget FROM events WHERE event_id <= 2 ORDER BY event_id ASC")
            .unwrap()
            .parse()
            .unwrap(),
        &events,
    );
    assert_eq!(
        execute_rel(&scalar, &catalog).unwrap(),
        vec![
            vec![Scalar::Int(1), Scalar::Decimal(9000)],
            vec![Scalar::Int(2), Scalar::Decimal(9000)],
        ]
    );
    let exists = prepare(
        Parser::new("SELECT event_id FROM events e WHERE event_id <= 4 AND EXISTS (SELECT campaign_id FROM campaigns c WHERE c.campaign_id = e.campaign_id) ORDER BY event_id ASC")
            .unwrap()
            .parse()
            .unwrap(),
        &events,
    );
    assert_eq!(
        execute_rel(&exists, &catalog).unwrap(),
        vec![vec![Scalar::Int(1)], vec![Scalar::Int(3)]]
    );
    let in_query = prepare(
        Parser::new("SELECT event_id FROM events WHERE campaign_id IN (SELECT campaign_id FROM campaigns) ORDER BY event_id ASC")
            .unwrap()
            .parse()
            .unwrap(),
        &events,
    );
    assert_eq!(
        execute_rel(&in_query, &catalog).unwrap(),
        vec![vec![Scalar::Int(1)], vec![Scalar::Int(3)]]
    );
}
#[test]
fn binder_rejects_invalid_names_and_types() {
    let bind_error = |sql: &str| {
        let query = Parser::new(sql).unwrap().parse().unwrap();
        bind_query(&query).unwrap_err()
    };
    assert!(
        bind_error("SELECT event_id FROM events WHERE event_id")
            .contains("WHERE requires a BOOLEAN")
    );
    assert!(bind_error("SELECT 'x' + 1 FROM events").contains("incompatible operand types"));
    assert!(
        bind_error("SELECT user_id FROM events e JOIN users u ON e.user_id = u.user_id")
            .contains("ambiguous column user_id")
    );
    assert!(
        bind_error("SELECT CAST(event_id AS uuid) FROM events").contains("unsupported CAST type")
    );
}
#[test]
fn optimizer_reorders_only_safe_star_joins() {
    let mut star = Parser::new(
        "SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id JOIN campaigns c ON e.campaign_id = c.campaign_id",
    )
    .unwrap()
    .parse()
    .unwrap();
    star.optimizer_enabled = true;
    assert_eq!(optimize_query(&mut star, 1_000_000), 1);
    assert_eq!(star.joins[0].table.name, "campaigns");
    assert_eq!(star.joins[1].table.name, "users");

    let mut dependent = Parser::new(
        "SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id JOIN campaigns c ON c.campaign_id = u.user_id",
    )
    .unwrap()
    .parse()
    .unwrap();
    dependent.optimizer_enabled = true;
    assert_eq!(optimize_query(&mut dependent, 1_000_000), 0);
    assert_eq!(dependent.joins[0].table.name, "users");
    assert_eq!(dependent.joins[1].table.name, "campaigns");
}
#[test]
fn all_queries_parse() {
    for i in 1..=64 {
        let p = format!(
            "{}/benchmarks/workloads/queries/Q{i:03}.sql",
            env!("CARGO_MANIFEST_DIR")
        );
        let sql = std::fs::read_to_string(p).unwrap();
        Parser::new(&sql).unwrap().parse().unwrap();
    }
}
#[test]
fn nested_roundtrips() {
    let docs = vec![
        Document {
            doc_id: 1,
            names: vec![],
        },
        Document {
            doc_id: 2,
            names: vec![Name {
                url: None,
                languages: vec![],
            }],
        },
        Document {
            doc_id: 3,
            names: vec![
                Name {
                    url: Some("u".into()),
                    languages: vec![
                        Language {
                            code: "en".into(),
                            country: None,
                        },
                        Language {
                            code: "fr".into(),
                            country: Some("FR".into()),
                        },
                    ],
                },
                Name {
                    url: None,
                    languages: vec![Language {
                        code: "de".into(),
                        country: Some("DE".into()),
                    }],
                },
            ],
        },
    ];
    for d in docs {
        let s = shred(&d);
        assert_eq!(d, assemble(&s));
        assert!(!s.urls.is_empty());
    }
}

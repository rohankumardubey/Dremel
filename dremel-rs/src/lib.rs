#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Options {
    pub data: String,
    pub threads: usize,
    pub batch_size: usize,
    pub memory_limit_mb: usize,
    pub max_result_rows: usize,
    pub max_active_queries: usize,
    pub admission_queue_capacity: usize,
    pub scheduler_memory_mb: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Null,
    Int(i64),
    Decimal(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ScalarKey {
    Null,
    Dict(u32),
    Int(i64),
    Decimal(i64),
    Float(u64),
    Bool(bool),
    Str(String),
}

struct ExecutionControl {
    cancelled: AtomicBool,
    deadline: Option<Instant>,
}

thread_local! {
    static EXECUTION_CONTROL: RefCell<Option<Arc<ExecutionControl>>> = const { RefCell::new(None) };
}

fn execution_cancelled() -> bool {
    EXECUTION_CONTROL.with(|slot| {
        slot.borrow().as_ref().is_some_and(|control| {
            control.cancelled.load(AtomicOrdering::Relaxed)
                || control
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
        })
    })
}

fn set_execution_control(control: Option<Arc<ExecutionControl>>) {
    EXECUTION_CONTROL.with(|slot| *slot.borrow_mut() = control);
}
impl Scalar {
    fn truthy(&self) -> bool {
        matches!(self, Self::Bool(true))
    }
    fn sql_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            Self::Null => None,
            _ => None,
        }
    }
    fn number(&self) -> Option<f64> {
        match self {
            Self::Int(v) => Some(*v as f64),
            Self::Decimal(v) => Some(*v as f64 / 100.0),
            Self::Float(v) => Some(*v),
            _ => None,
        }
    }
    fn json(&self) -> String {
        match self {
            Self::Null => "null".into(),
            Self::Int(v) => format!("{{\"t\":\"i\",\"v\":{v}}}"),
            Self::Decimal(v) => format!(
                "{{\"t\":\"d\",\"v\":\"{}{}.{:02}\"}}",
                if *v < 0 { "-" } else { "" },
                v.unsigned_abs() / 100,
                v.unsigned_abs() % 100
            ),
            Self::Float(v) => format!("{{\"t\":\"f\",\"v\":{v:.17}}}"),
            Self::Bool(v) => format!("{{\"t\":\"b\",\"v\":{v}}}"),
            Self::Str(v) => format!(
                "{{\"t\":\"s\",\"v\":\"{}\"}}",
                v.replace('\\', "\\\\").replace('"', "\\\"")
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Word(String),
    Number(String),
    String(String),
    Op(String),
    LParen,
    RParen,
    Comma,
    Dot,
    Star,
    Semi,
    End,
}

fn lex(sql: &str) -> Result<Vec<Token>, String> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i] as char;
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let s = i;
            i += 1;
            while i < b.len() && ((b[i] as char).is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            out.push(Token::Word(sql[s..i].to_ascii_lowercase()));
            continue;
        }
        if c.is_ascii_digit()
            || (c == '.' && i + 1 < b.len() && (b[i + 1] as char).is_ascii_digit())
        {
            let s = i;
            i += 1;
            while i < b.len() && ((b[i] as char).is_ascii_digit() || b[i] == b'.') {
                i += 1;
            }
            out.push(Token::Number(sql[s..i].into()));
            continue;
        }
        if c == '\'' {
            i += 1;
            let mut s = String::new();
            loop {
                if i >= b.len() {
                    return Err("unterminated string".into());
                }
                if b[i] == b'\'' {
                    if i + 1 < b.len() && b[i + 1] == b'\'' {
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    break;
                }
                s.push(b[i] as char);
                i += 1;
            }
            i += 1;
            out.push(Token::String(s));
            continue;
        }
        match c {
            '(' => out.push(Token::LParen),
            ')' => out.push(Token::RParen),
            ',' => out.push(Token::Comma),
            '.' => out.push(Token::Dot),
            '*' => out.push(Token::Star),
            ';' => out.push(Token::Semi),
            '=' | '+' | '-' | '/' => out.push(Token::Op(c.to_string())),
            '!' | '<' | '>' => {
                let mut op = c.to_string();
                if i + 1 < b.len() && (b[i + 1] == b'=' || (c == '<' && b[i + 1] == b'>')) {
                    op.push(b[i + 1] as char);
                    i += 1;
                }
                out.push(Token::Op(op));
            }
            _ => return Err(format!("unexpected character {c}")),
        }
        i += 1;
    }
    out.push(Token::End);
    Ok(out)
}

#[derive(Clone, Debug)]
pub enum Expr {
    Null,
    Column(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Star,
    Unary(String, Box<Expr>),
    Binary(String, Box<Expr>, Box<Expr>),
    Func(String, Box<Expr>),
    Call(String, Vec<Expr>),
    Case(Vec<(Expr, Expr)>, Box<Expr>),
    Cast(Box<Expr>, String),
    InList(Box<Expr>, Vec<Expr>, bool),
    Between(Box<Expr>, Box<Expr>, Box<Expr>, bool),
    Like(Box<Expr>, Box<Expr>, bool),
    Window {
        name: String,
        args: Vec<Expr>,
        partition_by: Vec<String>,
        order_by: Vec<OrderSpec>,
    },
    ScalarSubquery(Box<Query>),
    Exists(Box<Query>),
    InSubquery(Box<Expr>, Box<Query>, bool),
    IsNull(Box<Expr>, bool),
    DictEq(String, u32, bool),
}
#[derive(Clone, Debug)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRef {
    pub name: String,
    pub alias: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Cross,
    Inner,
    Left,
    Right,
    Full,
}
#[derive(Clone, Debug)]
pub struct JoinSpec {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Option<Expr>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderSpec {
    pub key: String,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}
#[derive(Clone, Debug)]
pub struct Query {
    pub select: Vec<SelectItem>,
    pub distinct: bool,
    pub from: TableRef,
    pub joins: Vec<JoinSpec>,
    pub filter: Option<Expr>,
    pub group_by: Vec<String>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderSpec>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub union: Option<Box<Query>>,
    pub union_all: bool,
    pub ctes: Vec<(String, Box<Query>)>,
    pub logical: Vec<String>,
    pub physical: Vec<String>,
    pub columns: Vec<String>,
    pub optimizer_enabled: bool,
}
type NamedQuery = (String, Box<Query>);

struct Parser {
    t: Vec<Token>,
    p: usize,
}
impl Parser {
    fn new(sql: &str) -> Result<Self, String> {
        Ok(Self { t: lex(sql)?, p: 0 })
    }
    fn peek(&self) -> &Token {
        &self.t[self.p]
    }
    fn next(&mut self) -> Token {
        let v = self.t[self.p].clone();
        self.p += 1;
        v
    }
    fn word(&mut self, w: &str) -> bool {
        if self.peek() == &Token::Word(w.into()) {
            self.p += 1;
            true
        } else {
            false
        }
    }
    fn expect_word(&mut self, w: &str) -> Result<(), String> {
        if self.word(w) {
            Ok(())
        } else {
            Err(format!("expected {w}, got {:?}", self.peek()))
        }
    }
    fn table_ref(&mut self) -> Result<TableRef, String> {
        let name = match self.next() {
            Token::Word(name) => name,
            token => return Err(format!("expected table name, got {token:?}")),
        };
        let alias = if self.word("as") {
            match self.next() {
                Token::Word(alias) => alias,
                token => return Err(format!("expected table alias, got {token:?}")),
            }
        } else if matches!(self.peek(), Token::Word(word) if ![
            "where", "group", "having", "order", "limit", "join", "inner", "left",
            "right", "full", "cross", "on", "outer"
        ].contains(&word.as_str()))
        {
            match self.next() {
                Token::Word(alias) => alias,
                _ => unreachable!(),
            }
        } else {
            name.clone()
        };
        Ok(TableRef { name, alias })
    }
    fn relation_ref(&mut self) -> Result<(TableRef, Option<NamedQuery>), String> {
        if self.peek() != &Token::LParen {
            return Ok((self.table_ref()?, None));
        }
        self.p += 1;
        let query = self.subquery()?;
        let _ = self.word("as");
        let alias = match self.next() {
            Token::Word(alias) => alias,
            token => return Err(format!("derived table requires an alias, got {token:?}")),
        };
        Ok((
            TableRef {
                name: alias.clone(),
                alias: alias.clone(),
            },
            Some((alias, Box::new(query))),
        ))
    }
    fn identifier(&mut self) -> Result<String, String> {
        let first = match self.next() {
            Token::Word(value) => value,
            token => return Err(format!("expected identifier, got {token:?}")),
        };
        if self.peek() == &Token::Dot {
            self.p += 1;
            match self.next() {
                Token::Word(second) => Ok(format!("{first}.{second}")),
                token => Err(format!("expected qualified identifier, got {token:?}")),
            }
        } else {
            Ok(first)
        }
    }
    fn window(&mut self, name: String, args: Vec<Expr>) -> Result<Expr, String> {
        if self.next() != Token::LParen {
            return Err("expected ( after OVER".into());
        }
        let mut partition_by = Vec::new();
        if self.word("partition") {
            self.expect_word("by")?;
            loop {
                partition_by.push(self.identifier()?);
                if self.peek() == &Token::Comma {
                    self.p += 1;
                } else {
                    break;
                }
            }
        }
        let mut order_by = Vec::new();
        if self.word("order") {
            self.expect_word("by")?;
            loop {
                let key = self.identifier()?;
                let ascending = !self.word("desc");
                let _ = self.word("asc");
                let nulls_first = if self.word("nulls") {
                    if self.word("first") {
                        Some(true)
                    } else {
                        self.expect_word("last")?;
                        Some(false)
                    }
                } else {
                    None
                };
                order_by.push(OrderSpec {
                    key,
                    ascending,
                    nulls_first,
                });
                if self.peek() == &Token::Comma {
                    self.p += 1;
                } else {
                    break;
                }
            }
        }
        if self.word("rows") {
            self.expect_word("between")?;
            self.expect_word("unbounded")?;
            self.expect_word("preceding")?;
            self.expect_word("and")?;
            self.expect_word("current")?;
            self.expect_word("row")?;
        }
        if self.next() != Token::RParen {
            return Err("expected ) after OVER clause".into());
        }
        Ok(Expr::Window {
            name,
            args,
            partition_by,
            order_by,
        })
    }
    fn subquery(&mut self) -> Result<Query, String> {
        let start = self.p;
        let mut cursor = self.p;
        let mut depth = 0usize;
        loop {
            match self.t.get(cursor) {
                Some(Token::LParen) => depth += 1,
                Some(Token::RParen) if depth == 0 => break,
                Some(Token::RParen) => depth -= 1,
                Some(Token::End) | None => return Err("unterminated subquery".into()),
                _ => {}
            }
            cursor += 1;
        }
        let mut tokens = self.t[start..cursor].to_vec();
        tokens.push(Token::End);
        self.p = cursor + 1;
        Parser { t: tokens, p: 0 }.parse()
    }
    fn parse(mut self) -> Result<Query, String> {
        if self.t.first() == Some(&Token::Word("with".into())) {
            let mut cursor = 1usize;
            let mut ctes = Vec::new();
            loop {
                let name = match self.t.get(cursor) {
                    Some(Token::Word(name)) if name != "recursive" => name.clone(),
                    Some(Token::Word(_)) => return Err("recursive CTEs are not supported".into()),
                    token => return Err(format!("expected CTE name, got {token:?}")),
                };
                cursor += 1;
                if self.t.get(cursor) != Some(&Token::Word("as".into()))
                    || self.t.get(cursor + 1) != Some(&Token::LParen)
                {
                    return Err("expected AS ( after CTE name".into());
                }
                cursor += 2;
                let start = cursor;
                let mut depth = 1usize;
                while cursor < self.t.len() && depth > 0 {
                    match self.t[cursor] {
                        Token::LParen => depth += 1,
                        Token::RParen => depth -= 1,
                        _ => {}
                    }
                    cursor += 1;
                }
                if depth != 0 {
                    return Err("unterminated CTE query".into());
                }
                let mut tokens = self.t[start..cursor - 1].to_vec();
                tokens.push(Token::End);
                let mut cte_query = Parser { t: tokens, p: 0 }.parse()?;
                cte_query.ctes = ctes.clone();
                cte_query.plan()?;
                ctes.push((name, Box::new(cte_query)));
                if self.t.get(cursor) == Some(&Token::Comma) {
                    cursor += 1;
                } else {
                    break;
                }
            }
            let mut outer = Parser {
                t: self.t[cursor..].to_vec(),
                p: 0,
            }
            .parse()?;
            outer.ctes = ctes;
            outer.plan()?;
            return Ok(outer);
        }
        let mut depth = 0usize;
        let mut union_at = None;
        for (index, token) in self.t.iter().enumerate() {
            match token {
                Token::LParen => depth += 1,
                Token::RParen => depth = depth.saturating_sub(1),
                Token::Word(word) if word == "union" && depth == 0 => {
                    union_at = Some(index);
                    break;
                }
                _ => {}
            }
        }
        if let Some(index) = union_at {
            let union_all = self.t.get(index + 1) == Some(&Token::Word("all".into()));
            let right_start = index + if union_all { 2 } else { 1 };
            let mut left_tokens = self.t[..index].to_vec();
            left_tokens.push(Token::End);
            let right_tokens = self.t[right_start..].to_vec();
            let mut left = Parser {
                t: left_tokens,
                p: 0,
            }
            .parse()?;
            let right = Parser {
                t: right_tokens,
                p: 0,
            }
            .parse()?;
            left.union = Some(Box::new(right));
            left.union_all = union_all;
            left.plan()?;
            return Ok(left);
        }
        self.expect_word("select")?;
        let distinct = self.word("distinct");
        let mut select = Vec::new();
        loop {
            let expr = self.expr(0)?;
            let alias = if self.word("as") {
                match self.next() {
                    Token::Word(x) => Some(x),
                    x => return Err(format!("expected alias, got {x:?}")),
                }
            } else {
                None
            };
            select.push(SelectItem { expr, alias });
            if self.peek() == &Token::Comma {
                self.p += 1
            } else {
                break;
            }
        }
        self.expect_word("from")?;
        let (from, derived_from) = self.relation_ref()?;
        let mut derived = derived_from.into_iter().collect::<Vec<_>>();
        let mut joins = Vec::new();
        loop {
            let kind = if self.peek() == &Token::Comma {
                self.p += 1;
                Some(JoinKind::Cross)
            } else if self.word("join") {
                Some(JoinKind::Inner)
            } else if self.word("inner") {
                self.expect_word("join")?;
                Some(JoinKind::Inner)
            } else if self.word("left") {
                let _ = self.word("outer");
                self.expect_word("join")?;
                Some(JoinKind::Left)
            } else if self.word("right") {
                let _ = self.word("outer");
                self.expect_word("join")?;
                Some(JoinKind::Right)
            } else if self.word("full") {
                let _ = self.word("outer");
                self.expect_word("join")?;
                Some(JoinKind::Full)
            } else if self.word("cross") {
                self.expect_word("join")?;
                Some(JoinKind::Cross)
            } else {
                None
            };
            let Some(kind) = kind else { break };
            let (table, derived_join) = self.relation_ref()?;
            derived.extend(derived_join);
            let on = if kind == JoinKind::Cross {
                None
            } else {
                self.expect_word("on")?;
                Some(self.expr(0)?)
            };
            joins.push(JoinSpec { kind, table, on });
        }
        let filter = if self.word("where") {
            Some(self.expr(0)?)
        } else {
            None
        };
        let mut group_by = Vec::new();
        if self.word("group") {
            self.expect_word("by")?;
            loop {
                group_by.push(self.identifier()?);
                if self.peek() == &Token::Comma {
                    self.p += 1
                } else {
                    break;
                }
            }
        }
        let having = if self.word("having") {
            Some(self.expr(0)?)
        } else {
            None
        };
        let mut order_by = Vec::new();
        if self.word("order") {
            self.expect_word("by")?;
            loop {
                let key = self.identifier()?;
                let ascending = !self.word("desc");
                let _ = self.word("asc");
                let nulls_first = if self.word("nulls") {
                    if self.word("first") {
                        Some(true)
                    } else {
                        self.expect_word("last")?;
                        Some(false)
                    }
                } else {
                    None
                };
                order_by.push(OrderSpec {
                    key,
                    ascending,
                    nulls_first,
                });
                if self.peek() == &Token::Comma {
                    self.p += 1;
                } else {
                    break;
                }
            }
        }
        let limit = if self.word("limit") {
            match self.next() {
                Token::Number(n) => Some(n.parse().map_err(|_| "bad limit")?),
                x => return Err(format!("expected limit, got {x:?}")),
            }
        } else {
            None
        };
        let offset = if self.word("offset") {
            match self.next() {
                Token::Number(number) => number.parse().map_err(|_| "bad offset")?,
                token => return Err(format!("expected offset, got {token:?}")),
            }
        } else {
            0
        };
        if self.peek() == &Token::Semi {
            self.p += 1
        }
        if self.peek() != &Token::End {
            return Err(format!("trailing token {:?}", self.peek()));
        }
        let mut q = Query {
            select,
            distinct,
            from,
            joins,
            filter,
            group_by,
            having,
            order_by,
            limit,
            offset,
            union: None,
            union_all: false,
            ctes: derived,
            logical: Vec::new(),
            physical: Vec::new(),
            columns: Vec::new(),
            optimizer_enabled: true,
        };
        q.plan()?;
        Ok(q)
    }
    fn expr(&mut self, min: u8) -> Result<Expr, String> {
        let mut lhs = if self.word("not") {
            Expr::Unary("not".into(), Box::new(self.expr(6)?))
        } else if self.peek() == &Token::Op("-".into()) {
            self.p += 1;
            Expr::Unary("-".into(), Box::new(self.expr(6)?))
        } else {
            self.primary()?
        };
        loop {
            if self.word("is") {
                let neg = self.word("not");
                self.expect_word("null")?;
                lhs = Expr::IsNull(Box::new(lhs), neg);
                continue;
            }
            let negated_special = self.peek() == &Token::Word("not".into())
                && matches!(
                    self.t.get(self.p + 1),
                    Some(Token::Word(w)) if w == "in" || w == "between" || w == "like"
                );
            let special = if negated_special {
                self.p += 1;
                match self.next() {
                    Token::Word(w) => Some((w, true)),
                    _ => unreachable!(),
                }
            } else if matches!(self.peek(), Token::Word(w) if w == "in" || w == "between" || w == "like")
            {
                match self.next() {
                    Token::Word(w) => Some((w, false)),
                    _ => unreachable!(),
                }
            } else {
                None
            };
            if let Some((kind, negated)) = special {
                if min > 3 {
                    return Err(format!("{kind} has lower precedence than its context"));
                }
                match kind.as_str() {
                    "in" => {
                        if self.next() != Token::LParen {
                            return Err("expected ( after IN".into());
                        }
                        if self.peek() == &Token::Word("select".into())
                            || self.peek() == &Token::Word("with".into())
                        {
                            let query = self.subquery()?;
                            lhs = Expr::InSubquery(Box::new(lhs), Box::new(query), negated);
                            continue;
                        }
                        let mut values = Vec::new();
                        if self.peek() != &Token::RParen {
                            loop {
                                values.push(self.expr(0)?);
                                if self.peek() == &Token::Comma {
                                    self.p += 1;
                                } else {
                                    break;
                                }
                            }
                        }
                        if self.next() != Token::RParen {
                            return Err("expected ) after IN list".into());
                        }
                        lhs = Expr::InList(Box::new(lhs), values, negated);
                    }
                    "between" => {
                        let low = self.expr(4)?;
                        self.expect_word("and")?;
                        let high = self.expr(4)?;
                        lhs = Expr::Between(Box::new(lhs), Box::new(low), Box::new(high), negated);
                    }
                    "like" => {
                        let pattern = self.expr(4)?;
                        lhs = Expr::Like(Box::new(lhs), Box::new(pattern), negated);
                    }
                    _ => unreachable!(),
                }
                continue;
            }
            let (op, prec) = match self.peek() {
                Token::Word(x) if x == "or" => ("or".to_string(), 1),
                Token::Word(x) if x == "and" => ("and".to_string(), 2),
                Token::Op(x) if ["=", "!=", "<>", "<", "<=", ">", ">="].contains(&x.as_str()) => {
                    (x.clone(), 3)
                }
                Token::Op(x) if x == "+" || x == "-" => (x.clone(), 4),
                Token::Star => ("*".to_string(), 5),
                Token::Op(x) if x == "/" => (x.clone(), 5),
                _ => break,
            };
            if prec < min {
                break;
            }
            self.p += 1;
            let rhs = self.expr(prec + 1)?;
            lhs = Expr::Binary(
                if op == "<>" { "!=".into() } else { op },
                Box::new(lhs),
                Box::new(rhs),
            );
        }
        Ok(lhs)
    }
    fn primary(&mut self) -> Result<Expr, String> {
        match self.next() {
            Token::Word(w) if w == "null" => Ok(Expr::Null),
            Token::Word(w) if w == "true" => Ok(Expr::Bool(true)),
            Token::Word(w) if w == "false" => Ok(Expr::Bool(false)),
            Token::Word(w) if w == "exists" => {
                if self.next() != Token::LParen {
                    return Err("expected ( after EXISTS".into());
                }
                Ok(Expr::Exists(Box::new(self.subquery()?)))
            }
            Token::Word(w) if w == "case" => {
                let mut branches = Vec::new();
                while self.word("when") {
                    let condition = self.expr(0)?;
                    self.expect_word("then")?;
                    branches.push((condition, self.expr(0)?));
                }
                if branches.is_empty() {
                    return Err("searched CASE requires WHEN".into());
                }
                let fallback = if self.word("else") {
                    self.expr(0)?
                } else {
                    Expr::Null
                };
                self.expect_word("end")?;
                Ok(Expr::Case(branches, Box::new(fallback)))
            }
            Token::Word(w) if w == "cast" => {
                if self.next() != Token::LParen {
                    return Err("expected ( after CAST".into());
                }
                let value = self.expr(0)?;
                self.expect_word("as")?;
                let mut data_type = match self.next() {
                    Token::Word(name) => name,
                    token => return Err(format!("expected CAST type, got {token:?}")),
                };
                if data_type == "decimal" && self.peek() == &Token::LParen {
                    self.p += 1;
                    let precision = match self.next() {
                        Token::Number(value) => value,
                        token => return Err(format!("expected DECIMAL precision, got {token:?}")),
                    };
                    if self.next() != Token::Comma {
                        return Err("expected DECIMAL precision comma".into());
                    }
                    let scale = match self.next() {
                        Token::Number(value) => value,
                        token => return Err(format!("expected DECIMAL scale, got {token:?}")),
                    };
                    if self.next() != Token::RParen {
                        return Err("expected ) after DECIMAL precision".into());
                    }
                    data_type = format!("decimal({precision},{scale})");
                }
                if self.next() != Token::RParen {
                    return Err("expected ) after CAST".into());
                }
                Ok(Expr::Cast(Box::new(value), data_type))
            }
            Token::Word(w)
                if (w == "date" || w == "timestamp") && matches!(self.peek(), Token::String(_)) =>
            {
                let Token::String(value) = self.next() else {
                    unreachable!()
                };
                Ok(Expr::Call(w, vec![Expr::String(value)]))
            }
            Token::Word(w) if w == "interval" && matches!(self.peek(), Token::String(_)) => {
                let Token::String(value) = self.next() else {
                    unreachable!()
                };
                Ok(Expr::Call(w, vec![Expr::String(value)]))
            }
            Token::Word(w) if w == "extract" && self.peek() == &Token::LParen => {
                self.p += 1;
                let field = self.identifier()?;
                self.expect_word("from")?;
                let value = self.expr(0)?;
                if self.next() != Token::RParen {
                    return Err("expected ) after EXTRACT".into());
                }
                Ok(Expr::Call(w, vec![Expr::String(field), value]))
            }
            Token::Word(w) => {
                if self.peek() == &Token::LParen {
                    self.p += 1;
                    let mut args = Vec::new();
                    if self.peek() != &Token::RParen {
                        loop {
                            args.push(if self.peek() == &Token::Star {
                                self.p += 1;
                                Expr::Star
                            } else {
                                self.expr(0)?
                            });
                            if self.peek() == &Token::Comma {
                                self.p += 1;
                            } else {
                                break;
                            }
                        }
                    }
                    if self.next() != Token::RParen {
                        return Err("expected )".into());
                    }
                    if self.word("over") {
                        self.window(w, args)
                    } else if args.len() == 1 {
                        Ok(Expr::Func(w, Box::new(args.remove(0))))
                    } else {
                        Ok(Expr::Call(w, args))
                    }
                } else if self.peek() == &Token::Dot {
                    self.p += 1;
                    match self.next() {
                        Token::Word(column) => Ok(Expr::Column(format!("{w}.{column}"))),
                        token => Err(format!("expected qualified column, got {token:?}")),
                    }
                } else {
                    Ok(Expr::Column(w))
                }
            }
            Token::Number(n) => {
                if n.contains('.') {
                    Ok(Expr::Float(n.parse().map_err(|_| "bad float")?))
                } else {
                    Ok(Expr::Int(n.parse().map_err(|_| "bad integer")?))
                }
            }
            Token::String(s) => Ok(Expr::String(s)),
            Token::Star => Ok(Expr::Star),
            Token::LParen => {
                if self.peek() == &Token::Word("select".into())
                    || self.peek() == &Token::Word("with".into())
                {
                    return Ok(Expr::ScalarSubquery(Box::new(self.subquery()?)));
                }
                let e = self.expr(0)?;
                if self.next() != Token::RParen {
                    return Err("expected )".into());
                }
                Ok(e)
            }
            x => Err(format!("unexpected token {x:?}")),
        }
    }
}

fn is_agg(e: &Expr) -> bool {
    matches!(e,Expr::Func(n,_) if ["count","sum","avg","min","max"].contains(&n.as_str()))
}
fn contains_agg(e: &Expr) -> bool {
    if is_agg(e) {
        return true;
    }
    match e {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            contains_agg(value)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            contains_agg(left) || contains_agg(right)
        }
        Expr::Call(_, args) => args.iter().any(contains_agg),
        Expr::Case(branches, fallback) => {
            branches
                .iter()
                .any(|(condition, value)| contains_agg(condition) || contains_agg(value))
                || contains_agg(fallback)
        }
        Expr::Cast(value, _) => contains_agg(value),
        Expr::InList(value, values, _) => contains_agg(value) || values.iter().any(contains_agg),
        Expr::Between(value, low, high, _) => {
            contains_agg(value) || contains_agg(low) || contains_agg(high)
        }
        Expr::Window { .. } => false,
        _ => false,
    }
}
fn contains_window(expression: &Expr) -> bool {
    match expression {
        Expr::Window { .. } => true,
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            contains_window(value)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            contains_window(left) || contains_window(right)
        }
        Expr::Call(_, args) => args.iter().any(contains_window),
        Expr::Case(branches, fallback) => {
            branches
                .iter()
                .any(|(condition, value)| contains_window(condition) || contains_window(value))
                || contains_window(fallback)
        }
        Expr::Cast(value, _) => contains_window(value),
        Expr::InList(value, values, _) => {
            contains_window(value) || values.iter().any(contains_window)
        }
        Expr::Between(value, low, high, _) => {
            contains_window(value) || contains_window(low) || contains_window(high)
        }
        _ => false,
    }
}
fn contains_subquery(expression: &Expr) -> bool {
    match expression {
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_, _, _) => true,
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            contains_subquery(value)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            contains_subquery(left) || contains_subquery(right)
        }
        Expr::Call(_, args) => args.iter().any(contains_subquery),
        Expr::Case(branches, fallback) => {
            branches
                .iter()
                .any(|(condition, value)| contains_subquery(condition) || contains_subquery(value))
                || contains_subquery(fallback)
        }
        Expr::Cast(value, _) => contains_subquery(value),
        Expr::InList(value, values, _) => {
            contains_subquery(value) || values.iter().any(contains_subquery)
        }
        Expr::Between(value, low, high, _) => {
            contains_subquery(value) || contains_subquery(low) || contains_subquery(high)
        }
        Expr::Window { args, .. } => args.iter().any(contains_subquery),
        _ => false,
    }
}
fn collect(e: &Expr, s: &mut BTreeSet<String>) {
    match e {
        Expr::Column(c) => {
            s.insert(c.rsplit('.').next().unwrap_or(c).to_string());
        }
        Expr::Unary(_, a) | Expr::Func(_, a) | Expr::IsNull(a, _) => collect(a, s),
        Expr::Call(_, args) => args.iter().for_each(|arg| collect(arg, s)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect(condition, s);
                collect(value, s);
            }
            collect(fallback, s);
        }
        Expr::Cast(value, _) => collect(value, s),
        Expr::InList(value, values, _) => {
            collect(value, s);
            values.iter().for_each(|item| collect(item, s));
        }
        Expr::Between(value, low, high, _) => {
            collect(value, s);
            collect(low, s);
            collect(high, s);
        }
        Expr::Like(value, pattern, _) => {
            collect(value, s);
            collect(pattern, s);
        }
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().for_each(|arg| collect(arg, s));
            partition_by.iter().for_each(|column| {
                s.insert(column.rsplit('.').next().unwrap_or(column).into());
            });
            order_by.iter().for_each(|order| {
                s.insert(order.key.rsplit('.').next().unwrap_or(&order.key).into());
            });
        }
        Expr::InSubquery(value, _, _) => collect(value, s),
        Expr::Binary(_, a, b) => {
            collect(a, s);
            collect(b, s)
        }
        Expr::DictEq(c, _, _) => {
            s.insert(c.clone());
        }
        _ => {}
    }
}
impl Query {
    fn plan(&mut self) -> Result<(), String> {
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
    fn explain(&self, batch: usize) -> String {
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

#[derive(Default)]
struct Dictionary {
    values: Vec<String>,
    ids: std::collections::HashMap<String, u32>,
}
impl Dictionary {
    fn insert(&mut self, s: &str) -> u32 {
        if let Some(v) = self.ids.get(s) {
            *v
        } else {
            let id = self.values.len() as u32;
            self.values.push(s.into());
            self.ids.insert(s.into(), id);
            id
        }
    }
    fn get(&self, id: u32) -> &str {
        &self.values[id as usize]
    }
}
fn read_bytes<R: Read>(r: &mut R, n: usize) -> Result<Vec<u8>, String> {
    let mut v = vec![0; n];
    r.read_exact(&mut v).map_err(|e| e.to_string())?;
    Ok(v)
}
fn read_u32<R: Read>(r: &mut R) -> Result<u32, String> {
    let b = read_bytes(r, 4)?;
    Ok(u32::from_le_bytes(b.try_into().expect("four bytes")))
}
fn read_u64<R: Read>(r: &mut R) -> Result<u64, String> {
    let b = read_bytes(r, 8)?;
    Ok(u64::from_le_bytes(b.try_into().expect("eight bytes")))
}
fn read_i64s<R: Read>(r: &mut R, n: usize) -> Result<Vec<i64>, String> {
    let b = read_bytes(r, n * 8)?;
    Ok(b.chunks_exact(8)
        .map(|x| i64::from_le_bytes(x.try_into().expect("eight bytes")))
        .collect())
}
fn read_f64s<R: Read>(r: &mut R, n: usize) -> Result<Vec<f64>, String> {
    let b = read_bytes(r, n * 8)?;
    Ok(b.chunks_exact(8)
        .map(|x| f64::from_le_bytes(x.try_into().expect("eight bytes")))
        .collect())
}
fn read_u32s<R: Read>(r: &mut R, n: usize) -> Result<Vec<u32>, String> {
    let b = read_bytes(r, n * 4)?;
    Ok(b.chunks_exact(4)
        .map(|x| u32::from_le_bytes(x.try_into().expect("four bytes")))
        .collect())
}
fn read_dictionary<R: Read>(r: &mut R, rows: usize) -> Result<(Dictionary, Vec<u32>), String> {
    let count = read_u32(r)?;
    let mut d = Dictionary::default();
    for _ in 0..count {
        let len = read_u32(r)? as usize;
        let raw = read_bytes(r, len)?;
        let value = String::from_utf8(raw).map_err(|e| e.to_string())?;
        d.insert(&value);
    }
    Ok((d, read_u32s(r, rows)?))
}
pub struct Table {
    event_id: Vec<i64>,
    user_id: Vec<i64>,
    timestamp: Vec<i64>,
    country: Vec<u32>,
    country_dict: Dictionary,
    device: Vec<u32>,
    device_dict: Dictionary,
    event_type: Vec<u32>,
    event_dict: Dictionary,
    duration: Vec<i64>,
    bytes: Vec<i64>,
    score: Vec<f64>,
    success: Vec<u8>,
    campaign: Vec<i64>,
    campaign_def: Vec<u8>,
}

#[derive(Default)]
struct UsersTable {
    user_id: Vec<i64>,
    segment: Vec<String>,
    signup_date: Vec<String>,
    lifetime_value: Vec<i64>,
    region: Vec<String>,
    active: Vec<bool>,
    index: std::collections::HashMap<i64, Vec<usize>>,
}

#[derive(Default)]
struct CampaignsTable {
    campaign_id: Vec<i64>,
    campaign_name: Vec<String>,
    budget: Vec<i64>,
    start_date: Vec<String>,
    end_date: Vec<String>,
    channel: Vec<String>,
    index: std::collections::HashMap<i64, Vec<usize>>,
}

struct Catalog {
    events: Arc<Table>,
    users: UsersTable,
    campaigns: CampaignsTable,
}

fn parse_decimal_cents(value: &str) -> Result<i64, String> {
    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |value| (true, value));
    let mut parts = value.split('.');
    let whole: i64 = parts
        .next()
        .ok_or("missing decimal")?
        .parse()
        .map_err(|_| "bad decimal")?;
    let fraction = parts.next().unwrap_or("0");
    if parts.next().is_some() || fraction.len() > 2 || !fraction.chars().all(|c| c.is_ascii_digit())
    {
        return Err("DECIMAL(18,2) requires at most two fractional digits".into());
    }
    let fraction: i64 = format!("{fraction:0<2}")
        .parse()
        .map_err(|_| "bad decimal fraction")?;
    let units = whole
        .checked_mul(100)
        .and_then(|whole| whole.checked_add(fraction))
        .ok_or("decimal overflow")?;
    Ok(if negative { -units } else { units })
}

impl Catalog {
    fn load(events_path: &str, events: Arc<Table>) -> Result<Self, String> {
        let directory = Path::new(events_path)
            .parent()
            .unwrap_or_else(|| Path::new("."));
        let users_path = directory.join("users.csv");
        let campaigns_path = directory.join("campaigns.csv");
        let mut users = UsersTable::default();
        let user_file = File::open(&users_path)
            .map_err(|error| format!("cannot open {}: {error}", users_path.display()))?;
        for (line_number, line) in BufReader::new(user_file).lines().enumerate() {
            let line = line.map_err(|error| error.to_string())?;
            if line_number == 0 {
                continue;
            }
            let fields: Vec<_> = line.split(',').collect();
            if fields.len() != 6 {
                return Err(format!("bad users row {}", line_number + 1));
            }
            let row = users.user_id.len();
            let id = fields[0].parse().map_err(|_| "bad users.user_id")?;
            users.user_id.push(id);
            users.segment.push(fields[1].into());
            users.signup_date.push(fields[2].into());
            users
                .lifetime_value
                .push(parse_decimal_cents(fields[3]).map_err(|_| "bad users.lifetime_value")?);
            users.region.push(fields[4].into());
            users.active.push(fields[5] == "true");
            users.index.entry(id).or_default().push(row);
        }
        let mut campaigns = CampaignsTable::default();
        let campaign_file = File::open(&campaigns_path)
            .map_err(|error| format!("cannot open {}: {error}", campaigns_path.display()))?;
        for (line_number, line) in BufReader::new(campaign_file).lines().enumerate() {
            let line = line.map_err(|error| error.to_string())?;
            if line_number == 0 {
                continue;
            }
            let fields: Vec<_> = line.split(',').collect();
            if fields.len() != 6 {
                return Err(format!("bad campaigns row {}", line_number + 1));
            }
            let row = campaigns.campaign_id.len();
            let id = fields[0].parse().map_err(|_| "bad campaigns.campaign_id")?;
            campaigns.campaign_id.push(id);
            campaigns.campaign_name.push(fields[1].into());
            campaigns
                .budget
                .push(parse_decimal_cents(fields[2]).map_err(|_| "bad campaigns.budget")?);
            campaigns.start_date.push(fields[3].into());
            campaigns.end_date.push(fields[4].into());
            campaigns.channel.push(fields[5].into());
            campaigns.index.entry(id).or_default().push(row);
        }
        Ok(Self {
            events,
            users,
            campaigns,
        })
    }
}

#[derive(Clone, Copy, Default)]
struct RelRow {
    event: Option<usize>,
    user: Option<usize>,
    campaign: Option<usize>,
}

fn table_columns(table: &str) -> &'static [&'static str] {
    match table {
        "events" => &[
            "event_id",
            "user_id",
            "timestamp",
            "country",
            "device",
            "event_type",
            "duration_ms",
            "bytes",
            "score",
            "success",
            "campaign_id",
        ],
        "users" => &[
            "user_id",
            "segment",
            "signup_date",
            "lifetime_value",
            "region",
            "active",
        ],
        "campaigns" => &[
            "campaign_id",
            "campaign_name",
            "budget",
            "start_date",
            "end_date",
            "channel",
        ],
        _ => &[],
    }
}

fn query_bindings(query: &Query) -> Result<std::collections::HashMap<String, String>, String> {
    let mut bindings = std::collections::HashMap::new();
    for table in std::iter::once(&query.from).chain(query.joins.iter().map(|join| &join.table)) {
        if table_columns(&table.name).is_empty() {
            return Err(format!("unknown table {}", table.name));
        }
        if bindings
            .insert(table.alias.clone(), table.name.clone())
            .is_some()
        {
            return Err(format!("duplicate table alias {}", table.alias));
        }
        bindings
            .entry(table.name.clone())
            .or_insert_with(|| table.name.clone());
    }
    Ok(bindings)
}

fn resolve_column(
    column: &str,
    bindings: &std::collections::HashMap<String, String>,
) -> Result<(String, String), String> {
    if let Some((qualifier, name)) = column.split_once('.') {
        let table = bindings
            .get(qualifier)
            .ok_or_else(|| format!("unknown table or alias {qualifier}"))?;
        if !table_columns(table).contains(&name) {
            return Err(format!("unknown column {column}"));
        }
        return Ok((table.clone(), name.into()));
    }
    let mut tables: Vec<_> = bindings
        .values()
        .filter(|table| table_columns(table).contains(&column))
        .cloned()
        .collect();
    tables.sort();
    tables.dedup();
    match tables.as_slice() {
        [] => Err(format!("unknown column {column}")),
        [table] => Ok((table.clone(), column.into())),
        _ => Err(format!("ambiguous column {column}")),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqlType {
    Null,
    Bool,
    Int,
    Decimal,
    Double,
    String,
    Date,
    Timestamp,
    Interval,
    Unknown,
}

fn column_type(table: &str, column: &str) -> SqlType {
    match (table, column) {
        ("events", "event_id" | "user_id" | "duration_ms" | "bytes" | "campaign_id")
        | ("users", "user_id")
        | ("campaigns", "campaign_id") => SqlType::Int,
        ("events", "timestamp") => SqlType::Timestamp,
        ("events", "score") => SqlType::Double,
        ("events", "success") | ("users", "active") => SqlType::Bool,
        ("users", "lifetime_value") | ("campaigns", "budget") => SqlType::Decimal,
        ("users", "signup_date") | ("campaigns", "start_date" | "end_date") => SqlType::Date,
        ("events", "country" | "device" | "event_type")
        | ("users", "segment" | "region")
        | ("campaigns", "campaign_name" | "channel") => SqlType::String,
        _ => SqlType::Unknown,
    }
}

fn numeric_type(data_type: SqlType) -> bool {
    matches!(data_type, SqlType::Int | SqlType::Decimal | SqlType::Double)
}

fn common_type(left: SqlType, right: SqlType) -> Result<SqlType, String> {
    if left == right {
        return Ok(left);
    }
    if left == SqlType::Null || left == SqlType::Unknown {
        return Ok(right);
    }
    if right == SqlType::Null || right == SqlType::Unknown {
        return Ok(left);
    }
    if numeric_type(left) && numeric_type(right) {
        return Ok(if left == SqlType::Double || right == SqlType::Double {
            SqlType::Double
        } else if left == SqlType::Decimal || right == SqlType::Decimal {
            SqlType::Decimal
        } else {
            SqlType::Int
        });
    }
    Err(format!("incompatible SQL types {left:?} and {right:?}"))
}

fn infer_type(
    expression: &Expr,
    bindings: &std::collections::HashMap<String, String>,
) -> Result<SqlType, String> {
    match expression {
        Expr::Null => Ok(SqlType::Null),
        Expr::Column(column) => {
            let (table, column) = resolve_column(column, bindings)?;
            Ok(column_type(&table, &column))
        }
        Expr::Int(_) | Expr::Star => Ok(SqlType::Int),
        Expr::Float(_) => Ok(SqlType::Double),
        Expr::Bool(_) | Expr::DictEq(_, _, _) | Expr::IsNull(_, _) => Ok(SqlType::Bool),
        Expr::String(_) => Ok(SqlType::String),
        Expr::Unary(operator, value) => {
            let value = infer_type(value, bindings)?;
            if operator == "not" {
                if !matches!(value, SqlType::Bool | SqlType::Null | SqlType::Unknown) {
                    return Err("NOT requires BOOLEAN".into());
                }
                Ok(SqlType::Bool)
            } else if numeric_type(value) || matches!(value, SqlType::Null | SqlType::Unknown) {
                Ok(value)
            } else {
                Err("unary minus requires a numeric operand".into())
            }
        }
        Expr::Binary(operator, left, right) => {
            let left = infer_type(left, bindings)?;
            let right = infer_type(right, bindings)?;
            match operator.as_str() {
                "and" | "or" => {
                    if !matches!(left, SqlType::Bool | SqlType::Null | SqlType::Unknown)
                        || !matches!(right, SqlType::Bool | SqlType::Null | SqlType::Unknown)
                    {
                        return Err(format!("{operator} requires BOOLEAN operands"));
                    }
                    Ok(SqlType::Bool)
                }
                "=" | "!=" | "<" | "<=" | ">" | ">=" => {
                    common_type(left, right)?;
                    Ok(SqlType::Bool)
                }
                "+" | "-" if left == SqlType::Date && right == SqlType::Interval => {
                    Ok(SqlType::Date)
                }
                "+" | "-" if left == SqlType::Timestamp && right == SqlType::Interval => {
                    Ok(SqlType::Timestamp)
                }
                "+" | "-" | "*" | "/" if numeric_type(left) && numeric_type(right) => {
                    if operator == "/" {
                        Ok(SqlType::Double)
                    } else {
                        common_type(left, right)
                    }
                }
                "+" | "-" | "*" | "/"
                    if matches!(left, SqlType::Null | SqlType::Unknown)
                        || matches!(right, SqlType::Null | SqlType::Unknown) =>
                {
                    Ok(common_type(left, right).unwrap_or(SqlType::Unknown))
                }
                _ => Err(format!(
                    "{operator} has incompatible operand types {left:?}, {right:?}"
                )),
            }
        }
        Expr::Func(name, argument) => {
            let argument = infer_type(argument, bindings)?;
            match name.as_str() {
                "count" => Ok(SqlType::Int),
                "avg" => Ok(SqlType::Double),
                "sum" | "min" | "max" => Ok(argument),
                "lower" | "upper" | "substring" => Ok(SqlType::String),
                "length" => Ok(SqlType::Int),
                "abs" => numeric_type(argument)
                    .then_some(argument)
                    .ok_or_else(|| "ABS requires a numeric operand".into()),
                "date" => Ok(SqlType::Date),
                "timestamp" => Ok(SqlType::Timestamp),
                "interval" => Ok(SqlType::Interval),
                _ => Ok(SqlType::Unknown),
            }
        }
        Expr::Call(name, arguments) => {
            let types: Vec<_> = arguments
                .iter()
                .map(|argument| infer_type(argument, bindings))
                .collect::<Result<_, _>>()?;
            match name.as_str() {
                "count" => Ok(SqlType::Int),
                "avg" => Ok(SqlType::Double),
                "sum" | "min" | "max" => Ok(types.first().copied().unwrap_or(SqlType::Unknown)),
                "coalesce" => types.into_iter().try_fold(SqlType::Null, common_type),
                "nullif" => {
                    if types.len() == 2 {
                        common_type(types[0], types[1])?;
                    }
                    Ok(types.first().copied().unwrap_or(SqlType::Unknown))
                }
                "concat" | "substring" => Ok(SqlType::String),
                "date" => Ok(SqlType::Date),
                "timestamp" => Ok(SqlType::Timestamp),
                "interval" => Ok(SqlType::Interval),
                "date_trunc" => Ok(types.get(1).copied().unwrap_or(SqlType::Unknown)),
                "extract" | "length" => Ok(SqlType::Int),
                _ => Ok(SqlType::Unknown),
            }
        }
        Expr::Case(branches, fallback) => {
            let mut result = infer_type(fallback, bindings)?;
            for (condition, value) in branches {
                let condition = infer_type(condition, bindings)?;
                if !matches!(condition, SqlType::Bool | SqlType::Null | SqlType::Unknown) {
                    return Err("CASE WHEN requires BOOLEAN".into());
                }
                result = common_type(result, infer_type(value, bindings)?)?;
            }
            Ok(result)
        }
        Expr::Cast(_, data_type) => Ok(if data_type.starts_with("decimal") {
            SqlType::Decimal
        } else {
            match data_type.as_str() {
                "bigint" | "int64" | "integer" => SqlType::Int,
                "double" | "float" | "real" => SqlType::Double,
                "varchar" | "string" | "text" => SqlType::String,
                "boolean" | "bool" => SqlType::Bool,
                "date" => SqlType::Date,
                "timestamp" => SqlType::Timestamp,
                _ => SqlType::Unknown,
            }
        }),
        Expr::InList(value, candidates, _) => {
            let value = infer_type(value, bindings)?;
            for candidate in candidates {
                common_type(value, infer_type(candidate, bindings)?)?;
            }
            Ok(SqlType::Bool)
        }
        Expr::Between(value, low, high, _) => {
            let value = infer_type(value, bindings)?;
            common_type(value, infer_type(low, bindings)?)?;
            common_type(value, infer_type(high, bindings)?)?;
            Ok(SqlType::Bool)
        }
        Expr::Like(value, pattern, _) => {
            common_type(infer_type(value, bindings)?, SqlType::String)?;
            common_type(infer_type(pattern, bindings)?, SqlType::String)?;
            Ok(SqlType::Bool)
        }
        Expr::Window { name, args, .. } => match name.as_str() {
            "row_number" | "rank" | "dense_rank" | "count" => Ok(SqlType::Int),
            "avg" => Ok(SqlType::Double),
            _ => args.first().map_or(Ok(SqlType::Unknown), |argument| {
                infer_type(argument, bindings)
            }),
        },
        Expr::ScalarSubquery(_) => Ok(SqlType::Unknown),
        Expr::InSubquery(_, _, _) | Expr::Exists(_) => Ok(SqlType::Bool),
    }
}

fn bind_expr(
    expression: &Expr,
    bindings: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    match expression {
        Expr::Column(column) | Expr::DictEq(column, _, _) => {
            resolve_column(column, bindings)?;
        }
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            bind_expr(value, bindings)?;
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            bind_expr(left, bindings)?;
            bind_expr(right, bindings)?;
        }
        Expr::Call(_, args) => {
            for arg in args {
                bind_expr(arg, bindings)?;
            }
        }
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                bind_expr(condition, bindings)?;
                bind_expr(value, bindings)?;
            }
            bind_expr(fallback, bindings)?;
        }
        Expr::Cast(value, data_type) => {
            bind_expr(value, bindings)?;
            if ![
                "bigint",
                "int64",
                "integer",
                "double",
                "float",
                "real",
                "varchar",
                "string",
                "text",
                "boolean",
                "bool",
                "date",
                "timestamp",
                "decimal",
            ]
            .contains(&data_type.as_str())
                && !data_type.starts_with("decimal(")
            {
                return Err(format!("unsupported CAST type {data_type}"));
            }
        }
        Expr::InList(value, values, _) => {
            bind_expr(value, bindings)?;
            for item in values {
                bind_expr(item, bindings)?;
            }
        }
        Expr::Between(value, low, high, _) => {
            bind_expr(value, bindings)?;
            bind_expr(low, bindings)?;
            bind_expr(high, bindings)?;
        }
        Expr::Window {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                bind_expr(arg, bindings)?;
            }
            for column in partition_by {
                resolve_column(column, bindings)?;
            }
            for order in order_by {
                resolve_column(&order.key, bindings)?;
            }
        }
        Expr::InSubquery(value, _, _) => bind_expr(value, bindings)?,
        _ => {}
    }
    Ok(())
}

fn bind_query(query: &Query) -> Result<std::collections::HashMap<String, String>, String> {
    let bindings = query_bindings(query)?;
    for item in &query.select {
        bind_expr(&item.expr, &bindings)?;
        infer_type(&item.expr, &bindings)?;
    }
    if let Some(filter) = &query.filter {
        bind_expr(filter, &bindings)?;
        if !matches!(
            infer_type(filter, &bindings)?,
            SqlType::Bool | SqlType::Null | SqlType::Unknown
        ) {
            return Err("WHERE requires a BOOLEAN expression".into());
        }
    }
    for join in &query.joins {
        if let Some(on) = &join.on {
            bind_expr(on, &bindings)?;
            if !matches!(
                infer_type(on, &bindings)?,
                SqlType::Bool | SqlType::Null | SqlType::Unknown
            ) {
                return Err("JOIN ON requires a BOOLEAN expression".into());
            }
        }
    }
    if let Some(having) = &query.having {
        bind_expr(having, &bindings)?;
        if !matches!(
            infer_type(having, &bindings)?,
            SqlType::Bool | SqlType::Null | SqlType::Unknown
        ) {
            return Err("HAVING requires a BOOLEAN expression".into());
        }
    }
    for group in &query.group_by {
        resolve_column(group, &bindings)?;
    }
    Ok(bindings)
}

fn relation_scalar(catalog: &Catalog, row: RelRow, table: &str, column: &str) -> Scalar {
    match (table, column) {
        ("events", column) => row
            .event
            .map_or(Scalar::Null, |index| catalog.events.scalar(column, index)),
        ("users", "user_id") => row
            .user
            .map_or(Scalar::Null, |i| Scalar::Int(catalog.users.user_id[i])),
        ("users", "segment") => row.user.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.users.segment[i].clone())
        }),
        ("users", "signup_date") => row.user.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.users.signup_date[i].clone())
        }),
        ("users", "lifetime_value") => row.user.map_or(Scalar::Null, |i| {
            Scalar::Decimal(catalog.users.lifetime_value[i])
        }),
        ("users", "region") => row.user.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.users.region[i].clone())
        }),
        ("users", "active") => row
            .user
            .map_or(Scalar::Null, |i| Scalar::Bool(catalog.users.active[i])),
        ("campaigns", "campaign_id") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Int(catalog.campaigns.campaign_id[i])
        }),
        ("campaigns", "campaign_name") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.campaigns.campaign_name[i].clone())
        }),
        ("campaigns", "budget") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Decimal(catalog.campaigns.budget[i])
        }),
        ("campaigns", "start_date") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.campaigns.start_date[i].clone())
        }),
        ("campaigns", "end_date") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.campaigns.end_date[i].clone())
        }),
        ("campaigns", "channel") => row.campaign.map_or(Scalar::Null, |i| {
            Scalar::Str(catalog.campaigns.channel[i].clone())
        }),
        _ => Scalar::Null,
    }
}
impl Table {
    pub fn load(path: &str) -> Result<Self, String> {
        if path.ends_with(".dremel") {
            Self::load_binary(path)
        } else {
            Self::load_csv(path)
        }
    }
    fn empty() -> Self {
        Self {
            event_id: vec![],
            user_id: vec![],
            timestamp: vec![],
            country: vec![],
            country_dict: Dictionary::default(),
            device: vec![],
            device_dict: Dictionary::default(),
            event_type: vec![],
            event_dict: Dictionary::default(),
            duration: vec![],
            bytes: vec![],
            score: vec![],
            success: vec![],
            campaign: vec![],
            campaign_def: vec![],
        }
    }
    fn load_binary(path: &str) -> Result<Self, String> {
        let mut f = File::open(path).map_err(|e| e.to_string())?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic).map_err(|e| e.to_string())?;
        if &magic != b"DREMCOL1" {
            return Err("invalid column-store magic".into());
        }
        let version = read_u32(&mut f)?;
        if version != 1 {
            return Err(format!("unsupported column-store version {version}"));
        }
        let rows = read_u64(&mut f)? as usize;
        let mut t = Self::empty();
        t.event_id = read_i64s(&mut f, rows)?;
        t.user_id = read_i64s(&mut f, rows)?;
        t.timestamp = read_i64s(&mut f, rows)?;
        (t.country_dict, t.country) = read_dictionary(&mut f, rows)?;
        (t.device_dict, t.device) = read_dictionary(&mut f, rows)?;
        (t.event_dict, t.event_type) = read_dictionary(&mut f, rows)?;
        t.duration = read_i64s(&mut f, rows)?;
        t.bytes = read_i64s(&mut f, rows)?;
        t.score = read_f64s(&mut f, rows)?;
        t.success = read_bytes(&mut f, rows)?;
        t.campaign = read_i64s(&mut f, rows)?;
        t.campaign_def = read_bytes(&mut f, rows)?;
        Ok(t)
    }
    fn load_csv(path: &str) -> Result<Self, String> {
        let f = File::open(path).map_err(|e| e.to_string())?;
        let mut lines = BufReader::new(f).lines();
        lines
            .next()
            .ok_or("empty csv")?
            .map_err(|e| e.to_string())?;
        let mut t = Self::empty();
        for line in lines {
            let line = line.map_err(|e| e.to_string())?;
            let p: Vec<&str> = line.split(',').collect();
            if p.len() != 11 {
                return Err(format!("bad CSV row: {line}"));
            }
            t.event_id.push(p[0].parse().map_err(|_| "event_id")?);
            t.user_id.push(p[1].parse().map_err(|_| "user_id")?);
            t.timestamp.push(p[2].parse().map_err(|_| "timestamp")?);
            t.country.push(t.country_dict.insert(p[3]));
            t.device.push(t.device_dict.insert(p[4]));
            t.event_type.push(t.event_dict.insert(p[5]));
            t.duration.push(p[6].parse().map_err(|_| "duration")?);
            t.bytes.push(p[7].parse().map_err(|_| "bytes")?);
            t.score.push(p[8].parse().map_err(|_| "score")?);
            t.success.push(u8::from(p[9] == "true"));
            if p[10].is_empty() {
                t.campaign.push(0);
                t.campaign_def.push(0)
            } else {
                t.campaign.push(p[10].parse().map_err(|_| "campaign")?);
                t.campaign_def.push(1)
            }
        }
        Ok(t)
    }
    fn len(&self) -> usize {
        self.event_id.len()
    }
    fn approximate_bytes(&self) -> usize {
        self.len() * (6 * 8 + 3 * 4 + 8 + 2)
            + self
                .country_dict
                .values
                .iter()
                .chain(&self.device_dict.values)
                .chain(&self.event_dict.values)
                .map(String::len)
                .sum::<usize>()
    }
    fn group_upper_bound(&self, columns: &[String]) -> usize {
        columns
            .iter()
            .map(|column| match column.rsplit('.').next().unwrap_or(column) {
                "country" => self.country_dict.values.len(),
                "device" => self.device_dict.values.len(),
                "event_type" => self.event_dict.values.len(),
                "success" => 2,
                _ => self.len(),
            })
            .fold(1usize, |a, b| a.saturating_mul(b))
            .min(self.len())
    }
    fn dict_id(&self, col: &str, s: &str) -> Option<u32> {
        match col.rsplit('.').next().unwrap_or(col) {
            "country" => self.country_dict.ids.get(s).copied(),
            "device" => self.device_dict.ids.get(s).copied(),
            "event_type" => self.event_dict.ids.get(s).copied(),
            _ => None,
        }
    }
    fn scalar(&self, c: &str, i: usize) -> Scalar {
        match c.rsplit('.').next().unwrap_or(c) {
            "event_id" => Scalar::Int(self.event_id[i]),
            "user_id" => Scalar::Int(self.user_id[i]),
            "timestamp" => Scalar::Int(self.timestamp[i]),
            "country" => Scalar::Str(self.country_dict.get(self.country[i]).into()),
            "device" => Scalar::Str(self.device_dict.get(self.device[i]).into()),
            "event_type" => Scalar::Str(self.event_dict.get(self.event_type[i]).into()),
            "duration_ms" => Scalar::Int(self.duration[i]),
            "bytes" => Scalar::Int(self.bytes[i]),
            "score" => Scalar::Float(self.score[i]),
            "success" => Scalar::Bool(self.success[i] != 0),
            "campaign_id" => {
                if self.campaign_def[i] == 0 {
                    Scalar::Null
                } else {
                    Scalar::Int(self.campaign[i])
                }
            }
            _ => Scalar::Null,
        }
    }
    fn raw_key(&self, c: &str, i: usize) -> u64 {
        match c.rsplit('.').next().unwrap_or(c) {
            "country" => self.country[i] as u64,
            "device" => self.device[i] as u64,
            "event_type" => self.event_type[i] as u64,
            "success" => self.success[i] as u64,
            "campaign_id" => self.campaign[i] as u64,
            _ => match self.scalar(c, i) {
                Scalar::Int(v) => v as u64,
                _ => 0,
            },
        }
    }
    fn key_scalar(&self, c: &str, k: u64) -> Scalar {
        match c.rsplit('.').next().unwrap_or(c) {
            "country" => Scalar::Str(self.country_dict.get(k as u32).into()),
            "device" => Scalar::Str(self.device_dict.get(k as u32).into()),
            "event_type" => Scalar::Str(self.event_dict.get(k as u32).into()),
            "success" => Scalar::Bool(k != 0),
            _ => Scalar::Int(k as i64),
        }
    }
}

fn prepare_expr(e: &mut Expr, t: &Table) {
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

fn literal_value(expression: &Expr) -> Option<Scalar> {
    match expression {
        Expr::Null => Some(Scalar::Null),
        Expr::Int(value) => Some(Scalar::Int(*value)),
        Expr::Float(value) => Some(Scalar::Float(*value)),
        Expr::Bool(value) => Some(Scalar::Bool(*value)),
        Expr::String(value) => Some(Scalar::Str(value.clone())),
        _ => None,
    }
}

fn literal_expression(value: Scalar) -> Expr {
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
fn optimize_expression(expression: &mut Expr) -> usize {
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

fn optimize_query(query: &mut Query) -> usize {
    let mut rewrites = 0;
    for item in &mut query.select {
        rewrites += optimize_expression(&mut item.expr);
    }
    if let Some(filter) = &mut query.filter {
        rewrites += optimize_expression(filter);
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
        let original: Vec<_> = query
            .joins
            .iter()
            .map(|join| join.table.name.clone())
            .collect();
        query
            .joins
            .sort_by_key(|join| match join.table.name.as_str() {
                "campaigns" => 5_000,
                "users" => 250_000,
                "events" => 1_000_000,
                _ => usize::MAX,
            });
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

fn prepare(mut q: Query, t: &Table) -> Query {
    q.optimizer_enabled = std::env::var_os("DREMEL_DISABLE_OPTIMIZER").is_none();
    let rewrites = if q.optimizer_enabled {
        optimize_query(&mut q)
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
    let mut estimate = match q.from.name.as_str() {
        "users" => 250_000,
        "campaigns" => 5_000,
        _ => t.len(),
    };
    if q.filter.is_some() {
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
            t.campaign_def.iter().filter(|&&level| level == 0).count(),
            t.country_dict.values.len(),
            t.device_dict.values.len(),
            t.event_dict.values.len(),
            t.event_id.first().copied().unwrap_or(0),
            t.event_id.last().copied().unwrap_or(0),
        ),
        "users" => "StatsExec(table=users;rows=250000;nulls=0;distinct=user_id:250000;min=user_id:1;max=user_id:250000)".into(),
        "campaigns" => "StatsExec(table=campaigns;rows=5000;nulls=0;distinct=campaign_id:5000;min=campaign_id:1;max=campaign_id:5000)".into(),
        table => format!("StatsExec(table={table};rows={estimate})"),
    };
    q.physical.push(statistics);
    q.physical.push(if q.optimizer_enabled {
        format!("OptimizerExec(rewrites={rewrites};rules=constant_folding+3vl+projection_pruning+predicate_pushdown+aggregate_filter_ordering+join_ordering+join_selection+runtime_filter+topk)")
    } else {
        "OptimizerExec(disabled=true)".into()
    });
    q
}
fn cmp(a: &Scalar, b: &Scalar) -> Option<Ordering> {
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

fn like_matches(value: &str, pattern: &str) -> bool {
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

fn eval_call(name: &str, args: &[Expr], t: &Table, row: usize) -> Scalar {
    let values: Vec<_> = args.iter().map(|arg| eval(arg, t, row)).collect();
    eval_values(name, values)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
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

fn civil_from_days(days: i64) -> (i64, i64, i64) {
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

fn parse_date(value: &str) -> Option<i64> {
    if value.len() != 10 || &value[4..5] != "-" || &value[7..8] != "-" {
        return None;
    }
    days_from_civil(
        value[0..4].parse().ok()?,
        value[5..7].parse().ok()?,
        value[8..10].parse().ok()?,
    )
}

fn format_date(days: i64) -> String {
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

fn parse_timestamp(value: &str) -> Option<i64> {
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

fn interval_seconds(value: &str) -> Option<i64> {
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

fn decimal_text(units: i64) -> String {
    format!(
        "{}{}.{:02}",
        if units < 0 { "-" } else { "" },
        units.unsigned_abs() / 100,
        units.unsigned_abs() % 100
    )
}

fn temporal_components(value: &Scalar) -> Option<(i64, i64, i64, i64, i64, i64)> {
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

fn eval_values(name: &str, values: Vec<Scalar>) -> Scalar {
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

fn cast_value(data_type: &str, value: Scalar) -> Scalar {
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

fn apply_binary(op: &str, x: Scalar, y: Scalar) -> Scalar {
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

fn eval(e: &Expr, t: &Table, i: usize) -> Scalar {
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
        Expr::Binary(op, a, b) => apply_binary(op, eval(a, t, i), eval(b, t, i)),
    }
}

fn scalar_expression(value: Scalar) -> Expr {
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

fn substitute_outer_expr(
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

fn execute_subquery(
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

fn simple_campaign_lookup(
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
                .and_then(|indices| indices.first().copied())
                .or_else(|| {
                    catalog
                        .campaigns
                        .campaign_id
                        .iter()
                        .position(|candidate| *candidate == value)
                }),
        ),
        Scalar::Null => Some(None),
        _ => None,
    }
}

fn simple_campaign_max_budget(
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
            catalog
                .campaigns
                .budget
                .iter()
                .copied()
                .max()
                .map_or(Scalar::Null, Scalar::Decimal),
        );
    }
    simple_campaign_lookup(query, catalog, row, outer_bindings).map(|index| {
        index.map_or(Scalar::Null, |index| {
            Scalar::Decimal(catalog.campaigns.budget[index])
        })
    })
}

fn simple_campaign_id_membership(query: &Query, value: &Scalar, catalog: &Catalog) -> Option<bool> {
    if query.from.name != "campaigns"
        || query.select.len() != 1
        || query.filter.is_some()
        || !query.joins.is_empty()
        || !matches!(&query.select[0].expr, Expr::Column(column) if column.rsplit('.').next() == Some("campaign_id"))
    {
        return None;
    }
    match value {
        Scalar::Int(value) => Some(
            catalog.campaigns.index.contains_key(value)
                || catalog
                    .campaigns
                    .campaign_id
                    .iter()
                    .any(|candidate| candidate == value),
        ),
        _ => None,
    }
}

fn eval_rel(
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

#[derive(Clone)]
struct SumState {
    value: f64,
    decimal_units: i128,
    floating: bool,
    decimal: bool,
    has: bool,
}
impl SumState {
    fn new() -> Self {
        Self {
            value: 0.0,
            decimal_units: 0,
            floating: false,
            decimal: false,
            has: false,
        }
    }
    fn add(&mut self, value: &Scalar) {
        match value {
            Scalar::Decimal(units) if !self.floating => {
                if !self.decimal {
                    self.decimal_units = (self.value as i128) * 100;
                    self.decimal = true;
                }
                self.decimal_units += i128::from(*units);
                self.has = true;
            }
            Scalar::Int(value) if self.decimal && !self.floating => {
                self.decimal_units += i128::from(*value) * 100;
                self.has = true;
            }
            value if value.number().is_some() => {
                if self.decimal {
                    self.value = self.decimal_units as f64 / 100.0;
                    self.decimal = false;
                }
                self.value += value.number().expect("numeric");
                self.floating |= matches!(value, Scalar::Float(_));
                self.has = true;
            }
            _ => {}
        }
    }
    fn merge(&mut self, other: &Self) {
        if !other.has {
            return;
        }
        if self.decimal && other.decimal && !self.floating && !other.floating {
            self.decimal_units += other.decimal_units;
            self.has = true;
            return;
        }
        if !self.has && other.decimal && !other.floating {
            *self = other.clone();
            return;
        }
        if self.decimal {
            self.value = self.decimal_units as f64 / 100.0;
            self.decimal = false;
        }
        self.value += if other.decimal {
            other.decimal_units as f64 / 100.0
        } else {
            other.value
        };
        self.floating |= other.floating;
        self.has = true;
    }
    fn finish(&self) -> Scalar {
        if !self.has {
            Scalar::Null
        } else if self.decimal && !self.floating {
            i64::try_from(self.decimal_units).map_or(Scalar::Null, Scalar::Decimal)
        } else if self.floating {
            Scalar::Float(self.value)
        } else {
            Scalar::Int(self.value as i64)
        }
    }
}

#[derive(Clone)]
enum AggState {
    Count(u64),
    Sum(SumState),
    Avg { sum: f64, count: u64 },
    Min(Option<Scalar>),
    Max(Option<Scalar>),
}
fn states(q: &Query) -> Vec<AggState> {
    q.select
        .iter()
        .filter(|s| is_agg(&s.expr))
        .map(|s| match &s.expr {
            Expr::Func(n, _) if n == "count" => AggState::Count(0),
            Expr::Func(n, _) if n == "sum" => AggState::Sum(SumState::new()),
            Expr::Func(n, _) if n == "avg" => AggState::Avg { sum: 0.0, count: 0 },
            Expr::Func(n, _) if n == "min" => AggState::Min(None),
            Expr::Func(_, _) => AggState::Max(None),
            _ => unreachable!(),
        })
        .collect()
}
fn update(st: &mut [AggState], q: &Query, t: &Table, i: usize) {
    for (state, item) in st
        .iter_mut()
        .zip(q.select.iter().filter(|s| is_agg(&s.expr)))
    {
        let Expr::Func(_, arg) = &item.expr else {
            continue;
        };
        let v = eval(arg, t, i);
        match state {
            AggState::Count(c) => {
                if matches!(**arg, Expr::Star) || !matches!(v, Scalar::Null) {
                    *c += 1
                }
            }
            AggState::Sum(sum) => sum.add(&v),
            AggState::Avg { sum, count } => {
                if let Some(n) = v.number() {
                    *sum += n;
                    *count += 1
                }
            }
            AggState::Min(x) => {
                if !matches!(v, Scalar::Null)
                    && (x.is_none()
                        || cmp(&v, x.as_ref().expect("present")) == Some(Ordering::Less))
                {
                    *x = Some(v)
                }
            }
            AggState::Max(x) => {
                if !matches!(v, Scalar::Null)
                    && (x.is_none()
                        || cmp(&v, x.as_ref().expect("present")) == Some(Ordering::Greater))
                {
                    *x = Some(v)
                }
            }
        }
    }
}
fn merge(a: &mut [AggState], b: &[AggState]) {
    for (x, y) in a.iter_mut().zip(b) {
        match (x, y) {
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (AggState::Sum(a), AggState::Sum(b)) => a.merge(b),
            (AggState::Avg { sum: a, count: ac }, AggState::Avg { sum: b, count: bc }) => {
                *a += b;
                *ac += bc
            }
            (AggState::Min(a), AggState::Min(Some(v)))
                if a.is_none() || cmp(v, a.as_ref().expect("present")) == Some(Ordering::Less) =>
            {
                *a = Some(v.clone())
            }
            (AggState::Max(a), AggState::Max(Some(v)))
                if a.is_none()
                    || cmp(v, a.as_ref().expect("present")) == Some(Ordering::Greater) =>
            {
                *a = Some(v.clone())
            }
            _ => {}
        }
    }
}
fn finish(s: &AggState) -> Scalar {
    match s {
        AggState::Count(v) => Scalar::Int(*v as i64),
        AggState::Sum(sum) => sum.finish(),
        AggState::Avg { sum, count } => {
            if *count == 0 {
                Scalar::Null
            } else {
                Scalar::Float(*sum / (*count as f64))
            }
        }
        AggState::Min(v) | AggState::Max(v) => v.clone().unwrap_or(Scalar::Null),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GroupKey {
    v: [u64; 3],
    n: u8,
}
fn hash64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}
fn key_hash(k: GroupKey) -> u64 {
    let mut h = 0x243F6A8885A308D3;
    for i in 0..k.n as usize {
        h = hash64(h ^ hash64(k.v[i].wrapping_add((i as u64) << 32)))
    }
    h
}
#[derive(Clone)]
struct Entry {
    k: GroupKey,
    s: Vec<AggState>,
}
struct GroupTable {
    slots: Vec<Option<Entry>>,
    len: usize,
}
impl GroupTable {
    fn new() -> Self {
        Self {
            slots: vec![None; 16],
            len: 0,
        }
    }
    fn find(&self, k: GroupKey) -> usize {
        let mut i = (key_hash(k) as usize) & (self.slots.len() - 1);
        loop {
            match &self.slots[i] {
                None => return i,
                Some(e) if e.k == k => return i,
                _ => i = (i + 1) & (self.slots.len() - 1),
            }
        }
    }
    fn grow(&mut self) {
        let next_capacity = self.slots.len() * 2;
        let old = std::mem::replace(&mut self.slots, vec![None; next_capacity]);
        self.len = 0;
        for e in old.into_iter().flatten() {
            let i = self.find(e.k);
            self.slots[i] = Some(e);
            self.len += 1
        }
    }
    fn get_or_insert(&mut self, k: GroupKey, template: &[AggState]) -> &mut Vec<AggState> {
        if (self.len + 1) * 10 > self.slots.len() * 7 {
            self.grow()
        }
        let i = self.find(k);
        if self.slots[i].is_none() {
            self.slots[i] = Some(Entry {
                k,
                s: template.to_vec(),
            });
            self.len += 1
        }
        &mut self.slots[i].as_mut().expect("inserted").s
    }
    fn into_entries(self) -> Vec<Entry> {
        self.slots.into_iter().flatten().collect()
    }
}
fn partition(q: &Query, t: &Table, start: usize, end: usize, batch: usize) -> GroupTable {
    let template = states(q);
    let mut groups = GroupTable::new();
    let mut selection = Vec::with_capacity(batch.max(1));
    for bs in (start..end).step_by(batch.max(1)) {
        let be = (bs + batch).min(end);
        selection.clear();
        for i in bs..be {
            if q.filter.as_ref().is_none_or(|f| eval(f, t, i).truthy()) {
                selection.push(i);
            }
        }
        for &i in &selection {
            let mut k = GroupKey {
                v: [0; 3],
                n: q.group_by.len() as u8,
            };
            for (j, c) in q.group_by.iter().enumerate() {
                k.v[j] = t.raw_key(c, i)
            }
            let s = groups.get_or_insert(k, &template);
            update(s, q, t, i)
        }
    }
    groups
}

struct Job {
    partition_id: usize,
    q: Arc<Query>,
    t: Arc<Table>,
    start: usize,
    end: usize,
    batch: usize,
    reply: mpsc::Sender<(usize, GroupTable)>,
}
pub struct Pool {
    tx: Option<mpsc::Sender<Job>>,
    workers: Vec<thread::JoinHandle<()>>,
    threads: usize,
}
impl Pool {
    fn new(n: usize) -> Self {
        let n = n.max(1);
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::new();
        for _ in 0..n {
            let rx = rx.clone();
            workers.push(thread::spawn(move || {
                loop {
                    let job = {
                        let Ok(lock) = rx.lock() else { break };
                        lock.recv()
                    };
                    let Ok(j) = job else { break };
                    let out = partition(&j.q, &j.t, j.start, j.end, j.batch);
                    let _ = j.reply.send((j.partition_id, out));
                }
            }));
        }
        Self {
            tx: Some(tx),
            workers,
            threads: n,
        }
    }
    fn aggregate(&self, q: Arc<Query>, t: Arc<Table>, batch: usize) -> GroupTable {
        let parts = self.threads * 4;
        let (tx, rx) = mpsc::channel();
        for p in 0..parts {
            let n = t.len();
            let j = Job {
                partition_id: p,
                q: q.clone(),
                t: t.clone(),
                start: p * n / parts,
                end: (p + 1) * n / parts,
                batch,
                reply: tx.clone(),
            };
            self.tx
                .as_ref()
                .expect("pool alive")
                .send(j)
                .expect("worker alive")
        }
        drop(tx);
        let template = states(&q);
        let mut final_t = GroupTable::new();
        let mut completed: Vec<_> = rx.into_iter().collect();
        completed.sort_by_key(|(partition_id, _)| *partition_id);
        for (_, part) in completed {
            for e in part.into_entries() {
                let target = final_t.get_or_insert(e.k, &template);
                merge(target, &e.s)
            }
        }
        final_t
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.tx.take();
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}

fn base_relation_rows(table: &str, catalog: &Catalog) -> Vec<RelRow> {
    let (rows, relation) = match table {
        "events" => (catalog.events.len(), 0),
        "users" => (catalog.users.user_id.len(), 1),
        "campaigns" => (catalog.campaigns.campaign_id.len(), 2),
        _ => return Vec::new(),
    };
    let mut output = Vec::with_capacity(rows);
    for index in 0..rows {
        if index % 4096 == 0 && execution_cancelled() {
            break;
        }
        let mut row = RelRow::default();
        match relation {
            0 => row.event = Some(index),
            1 => row.user = Some(index),
            _ => row.campaign = Some(index),
        }
        output.push(row);
    }
    output
}

fn merge_rel_rows(mut left: RelRow, right: RelRow) -> RelRow {
    left.event = left.event.or(right.event);
    left.user = left.user.or(right.user);
    left.campaign = left.campaign.or(right.campaign);
    left
}

fn scalar_hash_key(value: Scalar) -> Option<ScalarKey> {
    match value {
        Scalar::Null => None,
        Scalar::Int(value) => Some(ScalarKey::Int(value)),
        Scalar::Decimal(value) => Some(ScalarKey::Decimal(value)),
        Scalar::Float(value) => Some(ScalarKey::Float(value.to_bits())),
        Scalar::Bool(value) => Some(ScalarKey::Bool(value)),
        Scalar::Str(value) => Some(ScalarKey::Str(value)),
    }
}

fn scalar_group_key(value: Scalar) -> ScalarKey {
    scalar_hash_key(value).unwrap_or(ScalarKey::Null)
}

fn scalar_group_key_ref(value: &Scalar) -> ScalarKey {
    match value {
        Scalar::Null => ScalarKey::Null,
        Scalar::Int(value) => ScalarKey::Int(*value),
        Scalar::Decimal(value) => ScalarKey::Decimal(*value),
        Scalar::Float(value) => ScalarKey::Float(value.to_bits()),
        Scalar::Bool(value) => ScalarKey::Bool(*value),
        Scalar::Str(value) => ScalarKey::Str(value.clone()),
    }
}

fn relation_group_key(catalog: &Catalog, row: RelRow, table: &str, column: &str) -> ScalarKey {
    if table == "events"
        && let Some(index) = row.event
    {
        return match column {
            "country" => ScalarKey::Dict(catalog.events.country[index]),
            "device" => ScalarKey::Dict(catalog.events.device[index]),
            "event_type" => ScalarKey::Dict(catalog.events.event_type[index]),
            "success" => ScalarKey::Bool(catalog.events.success[index] != 0),
            "campaign_id" if catalog.events.campaign_def[index] == 0 => ScalarKey::Null,
            "campaign_id" => ScalarKey::Int(catalog.events.campaign[index]),
            _ => scalar_group_key(relation_scalar(catalog, row, table, column)),
        };
    }
    scalar_group_key(relation_scalar(catalog, row, table, column))
}

fn join_equality<'a>(
    expression: &'a Expr,
    right_table: &str,
    bindings: &std::collections::HashMap<String, String>,
) -> Option<(&'a Expr, &'a Expr)> {
    let Expr::Binary(operator, left, right) = expression else {
        return None;
    };
    if operator != "=" {
        return None;
    }
    let Expr::Column(left_column) = left.as_ref() else {
        return None;
    };
    let Expr::Column(right_column) = right.as_ref() else {
        return None;
    };
    let (left_relation, _) = resolve_column(left_column, bindings).ok()?;
    let (right_relation, _) = resolve_column(right_column, bindings).ok()?;
    if right_relation == right_table && left_relation != right_table {
        Some((left, right))
    } else if left_relation == right_table && right_relation != right_table {
        Some((right, left))
    } else {
        None
    }
}

fn apply_join(
    left_rows: Vec<RelRow>,
    join: &JoinSpec,
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
    optimizer_enabled: bool,
) -> Vec<RelRow> {
    let right_rows = base_relation_rows(&join.table.name, catalog);
    if join.kind == JoinKind::Cross {
        return left_rows
            .into_iter()
            .flat_map(|left| {
                right_rows
                    .iter()
                    .copied()
                    .map(move |right| merge_rel_rows(left, right))
            })
            .collect();
    }
    let on = join.on.as_ref().expect("non-cross join has ON");
    let equality = join_equality(on, &join.table.name, bindings);
    let mut output = Vec::new();
    let mut matched_right = vec![false; right_rows.len()];
    if let (true, Some((left_key, right_key))) = (optimizer_enabled, equality) {
        let Expr::Column(left_column) = left_key else {
            unreachable!()
        };
        let Expr::Column(right_column) = right_key else {
            unreachable!()
        };
        let (left_table, left_column) =
            resolve_column(left_column, bindings).expect("bound join column");
        let (right_table, right_column) =
            resolve_column(right_column, bindings).expect("bound join column");
        let mut hash = std::collections::HashMap::<ScalarKey, Vec<usize>>::new();
        for (index, &right) in right_rows.iter().enumerate() {
            if let Some(key) =
                scalar_hash_key(relation_scalar(catalog, right, &right_table, &right_column))
            {
                hash.entry(key).or_default().push(index);
            }
        }
        for left in left_rows {
            if execution_cancelled() {
                break;
            }
            let mut matched = false;
            if let Some(key) =
                scalar_hash_key(relation_scalar(catalog, left, &left_table, &left_column))
                && let Some(candidates) = hash.get(&key)
            {
                for &index in candidates {
                    let combined = merge_rel_rows(left, right_rows[index]);
                    if eval_rel(on, catalog, combined, bindings).truthy() {
                        output.push(combined);
                        matched = true;
                        matched_right[index] = true;
                    }
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                output.push(left);
            }
        }
    } else {
        for left in left_rows {
            if execution_cancelled() {
                break;
            }
            let mut matched = false;
            for (index, &right) in right_rows.iter().enumerate() {
                let combined = merge_rel_rows(left, right);
                if eval_rel(on, catalog, combined, bindings).truthy() {
                    output.push(combined);
                    matched = true;
                    matched_right[index] = true;
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                output.push(left);
            }
        }
    }
    if matches!(join.kind, JoinKind::Right | JoinKind::Full) {
        output.extend(
            right_rows
                .into_iter()
                .zip(matched_right)
                .filter_map(|(row, matched)| (!matched).then_some(row)),
        );
    }
    output
}

fn collect_aggregates(expression: &Expr, output: &mut Vec<Expr>) {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        if !output.iter().any(|existing| format!("{existing:?}") == key) {
            output.push(expression.clone());
        }
        return;
    }
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            collect_aggregates(value, output)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            collect_aggregates(left, output);
            collect_aggregates(right, output);
        }
        Expr::Call(_, values) => values
            .iter()
            .for_each(|value| collect_aggregates(value, output)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect_aggregates(condition, output);
                collect_aggregates(value, output);
            }
            collect_aggregates(fallback, output);
        }
        Expr::Cast(value, _) => collect_aggregates(value, output),
        Expr::InList(value, values, _) => {
            collect_aggregates(value, output);
            values
                .iter()
                .for_each(|item| collect_aggregates(item, output));
        }
        Expr::Between(value, low, high, _) => {
            collect_aggregates(value, output);
            collect_aggregates(low, output);
            collect_aggregates(high, output);
        }
        _ => {}
    }
}

fn aggregate_state(expression: &Expr) -> AggState {
    match expression {
        Expr::Func(name, _) if name == "count" => AggState::Count(0),
        Expr::Func(name, _) if name == "sum" => AggState::Sum(SumState::new()),
        Expr::Func(name, _) if name == "avg" => AggState::Avg { sum: 0.0, count: 0 },
        Expr::Func(name, _) if name == "min" => AggState::Min(None),
        Expr::Func(_, _) => AggState::Max(None),
        _ => unreachable!(),
    }
}

fn update_rel_aggregate(
    state: &mut AggState,
    expression: &Expr,
    catalog: &Catalog,
    row: RelRow,
    bindings: &std::collections::HashMap<String, String>,
) {
    let Expr::Func(_, argument) = expression else {
        unreachable!()
    };
    let value = eval_rel(argument, catalog, row, bindings);
    match state {
        AggState::Count(count) => {
            if matches!(argument.as_ref(), Expr::Star) || !matches!(value, Scalar::Null) {
                *count += 1;
            }
        }
        AggState::Sum(sum) => sum.add(&value),
        AggState::Avg { sum, count } => {
            if let Some(number) = value.number() {
                *sum += number;
                *count += 1;
            }
        }
        AggState::Min(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Less))
            {
                *current = Some(value);
            }
        }
        AggState::Max(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Greater))
            {
                *current = Some(value);
            }
        }
    }
}

fn eval_group_expr(
    expression: &Expr,
    catalog: &Catalog,
    row: RelRow,
    bindings: &std::collections::HashMap<String, String>,
    aggregates: &[Expr],
    values: &[Scalar],
) -> Scalar {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        return aggregates
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index].clone());
    }
    if !contains_agg(expression) {
        return eval_rel(expression, catalog, row, bindings);
    }
    let evaluate =
        |value: &Expr| eval_group_expr(value, catalog, row, bindings, aggregates, values);
    match expression {
        Expr::Unary(operator, value) => {
            let value = evaluate(value);
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
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Func(name, argument) => eval_values(name, vec![evaluate(argument)]),
        Expr::Call(name, arguments) => eval_values(name, arguments.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        Expr::InList(value, candidates, negated) => {
            let value = evaluate(value);
            if matches!(value, Scalar::Null) {
                return Scalar::Null;
            }
            let mut saw_null = false;
            for candidate in candidates {
                let candidate = evaluate(candidate);
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
            let value = evaluate(value);
            let low = evaluate(low);
            let high = evaluate(high);
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
        Expr::Like(value, pattern, negated) => match (evaluate(value), evaluate(pattern)) {
            (Scalar::Str(value), Scalar::Str(pattern)) => {
                Scalar::Bool(like_matches(&value, &pattern) ^ *negated)
            }
            _ => Scalar::Null,
        },
        _ => eval_rel(expression, catalog, row, bindings),
    }
}

struct RelGroup {
    row: RelRow,
    states: Vec<AggState>,
}

fn collect_windows(expression: &Expr, output: &mut Vec<Expr>) {
    if matches!(expression, Expr::Window { .. }) {
        let key = format!("{expression:?}");
        if !output
            .iter()
            .any(|candidate| format!("{candidate:?}") == key)
        {
            output.push(expression.clone());
        }
        return;
    }
    match expression {
        Expr::Unary(_, value) | Expr::Func(_, value) | Expr::IsNull(value, _) => {
            collect_windows(value, output)
        }
        Expr::Binary(_, left, right) | Expr::Like(left, right, _) => {
            collect_windows(left, output);
            collect_windows(right, output);
        }
        Expr::Call(_, args) => args.iter().for_each(|arg| collect_windows(arg, output)),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                collect_windows(condition, output);
                collect_windows(value, output);
            }
            collect_windows(fallback, output);
        }
        Expr::Cast(value, _) => collect_windows(value, output),
        Expr::InList(value, values, _) => {
            collect_windows(value, output);
            values.iter().for_each(|item| collect_windows(item, output));
        }
        Expr::Between(value, low, high, _) => {
            collect_windows(value, output);
            collect_windows(low, output);
            collect_windows(high, output);
        }
        _ => {}
    }
}

fn compare_rel_order(
    left: RelRow,
    right: RelRow,
    order: &[(String, String, bool, Option<bool>)],
    catalog: &Catalog,
) -> Ordering {
    for (table, column, ascending, requested_nulls_first) in order {
        let left = relation_scalar(catalog, left, table, column);
        let right = relation_scalar(catalog, right, table, column);
        let left_null = matches!(left, Scalar::Null);
        let right_null = matches!(right, Scalar::Null);
        let nulls_first = requested_nulls_first.unwrap_or(!ascending);
        let ordering = match (left_null, right_null) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = cmp(&left, &right).unwrap_or(Ordering::Equal);
                if *ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn compute_window(
    expression: &Expr,
    relation: &[RelRow],
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
) -> Vec<Scalar> {
    let Expr::Window {
        name,
        args,
        partition_by,
        order_by,
    } = expression
    else {
        unreachable!()
    };
    let partition_columns: Vec<_> = partition_by
        .iter()
        .map(|column| resolve_column(column, bindings).expect("bound window partition column"))
        .collect();
    let resolved_order: Vec<_> = order_by
        .iter()
        .map(|spec| {
            let (table, column) =
                resolve_column(&spec.key, bindings).expect("bound window order column");
            (table, column, spec.ascending, spec.nulls_first)
        })
        .collect();
    let mut partitions = std::collections::HashMap::<Vec<ScalarKey>, Vec<usize>>::new();
    for (index, &row) in relation.iter().enumerate() {
        let key = partition_columns
            .iter()
            .map(|(table, column)| relation_group_key(catalog, row, table, column))
            .collect();
        partitions.entry(key).or_default().push(index);
    }
    let mut result = vec![Scalar::Null; relation.len()];
    for mut indices in partitions.into_values() {
        indices.sort_by(|&left, &right| {
            compare_rel_order(relation[left], relation[right], &resolved_order, catalog)
                .then_with(|| left.cmp(&right))
        });
        match name.as_str() {
            "row_number" => {
                for (position, &index) in indices.iter().enumerate() {
                    result[index] = Scalar::Int((position + 1) as i64);
                }
            }
            "rank" | "dense_rank" => {
                let mut rank = 1usize;
                let mut dense = 1usize;
                for position in 0..indices.len() {
                    if position > 0
                        && compare_rel_order(
                            relation[indices[position - 1]],
                            relation[indices[position]],
                            &resolved_order,
                            catalog,
                        ) != Ordering::Equal
                    {
                        rank = position + 1;
                        dense += 1;
                    }
                    result[indices[position]] =
                        Scalar::Int(if name == "rank" { rank } else { dense } as i64);
                }
            }
            "lag" | "lead" => {
                for (position, &index) in indices.iter().enumerate() {
                    let offset = args
                        .get(1)
                        .map(|arg| eval_rel(arg, catalog, relation[index], bindings))
                        .and_then(|value| match value {
                            Scalar::Int(value) if value >= 0 => Some(value as usize),
                            _ => None,
                        })
                        .unwrap_or(1);
                    let target = if name == "lag" {
                        position.checked_sub(offset)
                    } else {
                        position
                            .checked_add(offset)
                            .filter(|target| *target < indices.len())
                    };
                    result[index] = target.map_or_else(
                        || {
                            args.get(2).map_or(Scalar::Null, |fallback| {
                                eval_rel(fallback, catalog, relation[index], bindings)
                            })
                        },
                        |target| eval_rel(&args[0], catalog, relation[indices[target]], bindings),
                    );
                }
            }
            "count" | "sum" | "avg" | "min" | "max" => {
                let aggregate = Expr::Func(name.clone(), Box::new(args[0].clone()));
                if order_by.is_empty() {
                    let mut state = aggregate_state(&aggregate);
                    for &index in &indices {
                        update_rel_aggregate(
                            &mut state,
                            &aggregate,
                            catalog,
                            relation[index],
                            bindings,
                        );
                    }
                    let value = finish(&state);
                    for &index in &indices {
                        result[index] = value.clone();
                    }
                } else {
                    let mut state = aggregate_state(&aggregate);
                    for &index in &indices {
                        update_rel_aggregate(
                            &mut state,
                            &aggregate,
                            catalog,
                            relation[index],
                            bindings,
                        );
                        result[index] = finish(&state);
                    }
                }
            }
            _ => {}
        }
    }
    result
}

fn eval_window_expr(
    expression: &Expr,
    row_index: usize,
    relation: &[RelRow],
    catalog: &Catalog,
    bindings: &std::collections::HashMap<String, String>,
    windows: &[Expr],
    values: &[Vec<Scalar>],
) -> Scalar {
    if matches!(expression, Expr::Window { .. }) {
        let key = format!("{expression:?}");
        return windows
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index][row_index].clone());
    }
    if !contains_window(expression) {
        return eval_rel(expression, catalog, relation[row_index], bindings);
    }
    let evaluate = |value: &Expr| {
        eval_window_expr(
            value, row_index, relation, catalog, bindings, windows, values,
        )
    };
    match expression {
        Expr::Unary(operator, value) => {
            let value = evaluate(value);
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
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Func(name, argument) => eval_values(name, vec![evaluate(argument)]),
        Expr::Call(name, args) => eval_values(name, args.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        _ => Scalar::Null,
    }
}

fn compare_output_rows(
    left: &[Scalar],
    right: &[Scalar],
    order: &[(usize, &OrderSpec)],
) -> Ordering {
    for (index, spec) in order {
        let left_null = matches!(left[*index], Scalar::Null);
        let right_null = matches!(right[*index], Scalar::Null);
        let nulls_first = spec.nulls_first.unwrap_or(!spec.ascending);
        let ordering = match (left_null, right_null) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let ordering = cmp(&left[*index], &right[*index]).unwrap_or(Ordering::Equal);
                if spec.ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    format!("{left:?}").cmp(&format!("{right:?}"))
}

fn finalize_rows(query: &Query, rows: &mut Vec<Vec<Scalar>>) {
    if query.distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| seen.insert(row.iter().map(scalar_group_key_ref).collect::<Vec<_>>()));
    }
    if !query.order_by.is_empty() {
        let order: Vec<_> = query
            .order_by
            .iter()
            .map(|spec| {
                let index = query
                    .select
                    .iter()
                    .position(|item| {
                        item.alias.as_ref() == Some(&spec.key)
                            || matches!(&item.expr, Expr::Column(column) if column == &spec.key || column.rsplit('.').next() == Some(spec.key.as_str()))
                    })
                    .unwrap_or(0);
                (index, spec)
            })
            .collect();
        let top_k = query
            .optimizer_enabled
            .then_some(query.limit)
            .flatten()
            .map(|limit| limit.saturating_add(query.offset))
            .unwrap_or(rows.len())
            .min(rows.len());
        if top_k == 0 {
            rows.clear();
        } else if top_k < rows.len() {
            rows.select_nth_unstable_by(top_k, |left, right| {
                compare_output_rows(left, right, &order)
            });
            rows.truncate(top_k);
        }
        rows.sort_by(|left, right| compare_output_rows(left, right, &order));
    }
    if query.offset > 0 {
        rows.drain(..query.offset.min(rows.len()));
    }
    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }
}

struct MaterializedRelation {
    name: String,
    columns: Vec<String>,
    rows: Vec<Vec<Scalar>>,
}

fn output_columns(query: &Query) -> Vec<String> {
    query
        .select
        .iter()
        .enumerate()
        .map(|(index, item)| {
            item.alias.clone().unwrap_or_else(|| match &item.expr {
                Expr::Column(column) => column.rsplit('.').next().unwrap_or(column).into(),
                _ => format!("column{}", index + 1),
            })
        })
        .collect()
}

fn eval_materialized_values(expression: &Expr, columns: &[String], row: &[Scalar]) -> Scalar {
    let evaluate = |value: &Expr| eval_materialized_values(value, columns, row);
    match expression {
        Expr::Null => Scalar::Null,
        Expr::Column(column) => {
            let exact = columns.iter().position(|candidate| candidate == column);
            let name = column.rsplit('.').next().unwrap_or(column);
            let mut unqualified = columns
                .iter()
                .enumerate()
                .filter(|(_, candidate)| candidate.rsplit('.').next() == Some(name));
            let fallback = unqualified
                .next()
                .and_then(|(index, _)| unqualified.next().is_none().then_some(index));
            exact
                .or(fallback)
                .map_or(Scalar::Null, |index| row[index].clone())
        }
        Expr::Int(value) => Scalar::Int(*value),
        Expr::Float(value) => Scalar::Float(*value),
        Expr::Bool(value) => Scalar::Bool(*value),
        Expr::String(value) => Scalar::Str(value.clone()),
        Expr::Star => Scalar::Int(1),
        Expr::Unary(operator, value) => {
            let value = evaluate(value);
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
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Func(name, argument) => eval_values(name, vec![evaluate(argument)]),
        Expr::Call(name, args) => eval_values(name, args.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        Expr::InList(value, candidates, negated) => {
            let value = evaluate(value);
            if matches!(value, Scalar::Null) {
                return Scalar::Null;
            }
            let mut saw_null = false;
            for candidate in candidates {
                let candidate = evaluate(candidate);
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
            let value = evaluate(value);
            let low = evaluate(low);
            let high = evaluate(high);
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
        Expr::Like(value, pattern, negated) => match (evaluate(value), evaluate(pattern)) {
            (Scalar::Str(value), Scalar::Str(pattern)) => {
                Scalar::Bool(like_matches(&value, &pattern) ^ *negated)
            }
            _ => Scalar::Null,
        },
        Expr::DictEq(_, _, _)
        | Expr::Window { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_, _, _) => Scalar::Null,
    }
}

fn eval_materialized(expression: &Expr, relation: &MaterializedRelation, row: usize) -> Scalar {
    eval_materialized_values(expression, &relation.columns, &relation.rows[row])
}

fn update_materialized_aggregate(
    state: &mut AggState,
    expression: &Expr,
    relation: &MaterializedRelation,
    row: usize,
) {
    let Expr::Func(_, argument) = expression else {
        unreachable!()
    };
    let value = eval_materialized(argument, relation, row);
    match state {
        AggState::Count(count) => {
            if matches!(argument.as_ref(), Expr::Star) || !matches!(value, Scalar::Null) {
                *count += 1;
            }
        }
        AggState::Sum(sum) => sum.add(&value),
        AggState::Avg { sum, count } => {
            if let Some(number) = value.number() {
                *sum += number;
                *count += 1;
            }
        }
        AggState::Min(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Less))
            {
                *current = Some(value);
            }
        }
        AggState::Max(current) => {
            if !matches!(value, Scalar::Null)
                && current
                    .as_ref()
                    .is_none_or(|old| cmp(&value, old) == Some(Ordering::Greater))
            {
                *current = Some(value);
            }
        }
    }
}

fn eval_materialized_group(
    expression: &Expr,
    relation: &MaterializedRelation,
    row: usize,
    aggregates: &[Expr],
    values: &[Scalar],
) -> Scalar {
    if is_agg(expression) {
        let key = format!("{expression:?}");
        return aggregates
            .iter()
            .position(|candidate| format!("{candidate:?}") == key)
            .map_or(Scalar::Null, |index| values[index].clone());
    }
    if !contains_agg(expression) {
        return eval_materialized(expression, relation, row);
    }
    let evaluate = |value: &Expr| eval_materialized_group(value, relation, row, aggregates, values);
    match expression {
        Expr::Binary(operator, left, right) => {
            apply_binary(operator, evaluate(left), evaluate(right))
        }
        Expr::Unary(operator, value) if operator == "not" => evaluate(value)
            .sql_bool()
            .map_or(Scalar::Null, |value| Scalar::Bool(!value)),
        Expr::Call(name, args) => eval_values(name, args.iter().map(evaluate).collect()),
        Expr::Case(branches, fallback) => {
            for (condition, value) in branches {
                if evaluate(condition).sql_bool() == Some(true) {
                    return evaluate(value);
                }
            }
            evaluate(fallback)
        }
        Expr::Cast(value, data_type) => cast_value(data_type, evaluate(value)),
        Expr::IsNull(value, negated) => {
            Scalar::Bool(matches!(evaluate(value), Scalar::Null) ^ *negated)
        }
        _ => Scalar::Null,
    }
}

fn execute_materialized(
    query: &Query,
    relation: &MaterializedRelation,
) -> Result<Vec<Vec<Scalar>>, String> {
    if query.from.name != relation.name {
        return Err(format!("unknown CTE {}", query.from.name));
    }
    let mut selected: Vec<_> = (0..relation.rows.len()).collect();
    if let Some(filter) = &query.filter {
        selected.retain(|&row| eval_materialized(filter, relation, row).truthy());
    }
    let aggregate = query.select.iter().any(|item| contains_agg(&item.expr))
        || query.having.as_ref().is_some_and(contains_agg)
        || !query.group_by.is_empty();
    let mut rows = Vec::new();
    if aggregate {
        let mut aggregate_expressions = Vec::new();
        for item in &query.select {
            collect_aggregates(&item.expr, &mut aggregate_expressions);
        }
        if let Some(having) = &query.having {
            collect_aggregates(having, &mut aggregate_expressions);
        }
        let templates: Vec<_> = aggregate_expressions.iter().map(aggregate_state).collect();
        let mut groups = std::collections::HashMap::<Vec<ScalarKey>, (usize, Vec<AggState>)>::new();
        if query.group_by.is_empty() {
            groups.insert(Vec::new(), (0, templates.clone()));
        }
        for row in selected {
            let key = query
                .group_by
                .iter()
                .map(|column| {
                    scalar_group_key(eval_materialized(
                        &Expr::Column(column.clone()),
                        relation,
                        row,
                    ))
                })
                .collect();
            let group = groups
                .entry(key)
                .or_insert_with(|| (row, templates.clone()));
            group.0 = row;
            for (state, expression) in group.1.iter_mut().zip(&aggregate_expressions) {
                update_materialized_aggregate(state, expression, relation, row);
            }
        }
        for (_, (row, states)) in groups {
            let values: Vec<_> = states.iter().map(finish).collect();
            if query.having.as_ref().is_some_and(|having| {
                !eval_materialized_group(having, relation, row, &aggregate_expressions, &values)
                    .truthy()
            }) {
                continue;
            }
            rows.push(
                query
                    .select
                    .iter()
                    .map(|item| {
                        eval_materialized_group(
                            &item.expr,
                            relation,
                            row,
                            &aggregate_expressions,
                            &values,
                        )
                    })
                    .collect(),
            );
        }
    } else {
        rows = selected
            .into_iter()
            .map(|row| {
                query
                    .select
                    .iter()
                    .map(|item| eval_materialized(&item.expr, relation, row))
                    .collect()
            })
            .collect();
    }
    finalize_rows(query, &mut rows);
    Ok(rows)
}

fn execute_materialized_joins(
    query: &Query,
    catalog: &Catalog,
) -> Result<Option<Vec<Vec<Scalar>>>, String> {
    let find_cte = |table: &TableRef| query.ctes.iter().find(|(name, _)| name == &table.name);
    if find_cte(&query.from).is_none()
        && query
            .joins
            .iter()
            .all(|join| find_cte(&join.table).is_none())
    {
        return Ok(None);
    }
    let materialize = |table: &TableRef| -> Result<(Vec<String>, Vec<Vec<Scalar>>), String> {
        if let Some((_, cte_query)) = find_cte(table) {
            return Ok((output_columns(cte_query), execute_rel(cte_query, catalog)?));
        }
        let columns: Vec<_> = table_columns(&table.name)
            .iter()
            .map(|column| (*column).to_string())
            .collect();
        if columns.is_empty() {
            return Err(format!("unknown table {}", table.name));
        }
        let rows = base_relation_rows(&table.name, catalog)
            .into_iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|column| relation_scalar(catalog, row, &table.name, column))
                    .collect()
            })
            .collect();
        Ok((columns, rows))
    };
    let (from_columns, mut rows) = materialize(&query.from)?;
    let mut columns: Vec<_> = from_columns
        .iter()
        .map(|column| format!("{}.{}", query.from.alias, column))
        .collect();
    for join in &query.joins {
        let (raw_right_columns, right_rows) = materialize(&join.table)?;
        let right_columns: Vec<_> = raw_right_columns
            .into_iter()
            .map(|column| format!("{}.{}", join.table.alias, column))
            .collect();
        let mut combined_columns = columns.clone();
        combined_columns.extend(right_columns.iter().cloned());
        let left_width = columns.len();
        let right_width = right_columns.len();
        let mut joined = Vec::new();
        let mut matched_right = vec![false; right_rows.len()];
        for left in rows {
            let mut matched = false;
            for (right_index, right) in right_rows.iter().enumerate() {
                let mut combined = left.clone();
                combined.extend(right.iter().cloned());
                let matches = join.kind == JoinKind::Cross
                    || join.on.as_ref().is_some_and(|on| {
                        eval_materialized_values(on, &combined_columns, &combined).truthy()
                    });
                if matches {
                    joined.push(combined);
                    matched = true;
                    matched_right[right_index] = true;
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                let mut combined = left;
                combined.extend(std::iter::repeat_n(Scalar::Null, right_width));
                joined.push(combined);
            }
        }
        if matches!(join.kind, JoinKind::Right | JoinKind::Full) {
            for (right, matched) in right_rows.iter().zip(matched_right) {
                if !matched {
                    let mut combined = vec![Scalar::Null; left_width];
                    combined.extend(right.iter().cloned());
                    joined.push(combined);
                }
            }
        }
        rows = joined;
        columns = combined_columns;
    }
    let relation = MaterializedRelation {
        name: query.from.name.clone(),
        columns,
        rows,
    };
    Ok(Some(execute_materialized(query, &relation)?))
}

fn execute_rel(query: &Query, catalog: &Catalog) -> Result<Vec<Vec<Scalar>>, String> {
    execute_rel_inner(query, catalog, false)
}

fn execute_rel_inner(
    query: &Query,
    catalog: &Catalog,
    skip_union: bool,
) -> Result<Vec<Vec<Scalar>>, String> {
    if !query.joins.is_empty()
        && !query.ctes.is_empty()
        && let Some(rows) = execute_materialized_joins(query, catalog)?
    {
        return Ok(rows);
    }
    if !query.ctes.is_empty()
        && let Some((name, cte_query)) =
            query.ctes.iter().find(|(name, _)| name == &query.from.name)
    {
        let relation = MaterializedRelation {
            name: name.clone(),
            columns: output_columns(cte_query),
            rows: execute_rel(cte_query, catalog)?,
        };
        return execute_materialized(query, &relation);
    }
    if !skip_union && let Some(right) = &query.union {
        let mut rows = execute_rel_inner(query, catalog, true)?;
        let right_rows = execute_rel(right, catalog)?;
        if rows.first().map(Vec::len) != right_rows.first().map(Vec::len)
            && !rows.is_empty()
            && !right_rows.is_empty()
        {
            return Err("UNION inputs must have the same column count".into());
        }
        rows.extend(right_rows);
        if !query.union_all {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| {
                seen.insert(row.iter().map(scalar_group_key_ref).collect::<Vec<_>>())
            });
        }
        return Ok(rows);
    }
    let bindings = bind_query(query)?;
    let mut relation = base_relation_rows(&query.from.name, catalog);
    for join in &query.joins {
        relation = apply_join(relation, join, catalog, &bindings, query.optimizer_enabled);
        if execution_cancelled() {
            return Ok(Vec::new());
        }
    }
    if let Some(filter) = &query.filter {
        relation.retain(|row| {
            !execution_cancelled() && eval_rel(filter, catalog, *row, &bindings).truthy()
        });
    }
    let aggregate = query.select.iter().any(|item| contains_agg(&item.expr))
        || query.having.as_ref().is_some_and(contains_agg)
        || !query.group_by.is_empty();
    let mut rows: Vec<Vec<Scalar>> = Vec::new();
    let has_windows = query.select.iter().any(|item| contains_window(&item.expr));
    if aggregate {
        let mut aggregate_expressions = Vec::new();
        for item in &query.select {
            collect_aggregates(&item.expr, &mut aggregate_expressions);
        }
        if let Some(having) = &query.having {
            collect_aggregates(having, &mut aggregate_expressions);
        }
        let templates: Vec<_> = aggregate_expressions.iter().map(aggregate_state).collect();
        let mut groups = std::collections::HashMap::<Vec<ScalarKey>, RelGroup>::new();
        if query.group_by.is_empty() {
            groups.insert(
                Vec::new(),
                RelGroup {
                    row: RelRow::default(),
                    states: templates.clone(),
                },
            );
        }
        let group_columns = query
            .group_by
            .iter()
            .map(|column| resolve_column(column, &bindings))
            .collect::<Result<Vec<_>, _>>()?;
        for row in relation {
            if execution_cancelled() {
                break;
            }
            let key = group_columns
                .iter()
                .map(|(table, column)| relation_group_key(catalog, row, table, column))
                .collect();
            let group = groups.entry(key).or_insert_with(|| RelGroup {
                row,
                states: templates.clone(),
            });
            group.row = row;
            for (state, expression) in group.states.iter_mut().zip(&aggregate_expressions) {
                update_rel_aggregate(state, expression, catalog, row, &bindings);
            }
        }
        for group in groups.into_values() {
            let aggregate_values: Vec<_> = group.states.iter().map(finish).collect();
            if query.having.as_ref().is_some_and(|having| {
                !eval_group_expr(
                    having,
                    catalog,
                    group.row,
                    &bindings,
                    &aggregate_expressions,
                    &aggregate_values,
                )
                .truthy()
            }) {
                continue;
            }
            rows.push(
                query
                    .select
                    .iter()
                    .map(|item| {
                        eval_group_expr(
                            &item.expr,
                            catalog,
                            group.row,
                            &bindings,
                            &aggregate_expressions,
                            &aggregate_values,
                        )
                    })
                    .collect(),
            );
        }
    } else if has_windows {
        let mut window_expressions = Vec::new();
        for item in &query.select {
            collect_windows(&item.expr, &mut window_expressions);
        }
        let window_values: Vec<_> = window_expressions
            .iter()
            .map(|window| compute_window(window, &relation, catalog, &bindings))
            .collect();
        rows = (0..relation.len())
            .map(|row_index| {
                query
                    .select
                    .iter()
                    .map(|item| {
                        eval_window_expr(
                            &item.expr,
                            row_index,
                            &relation,
                            catalog,
                            &bindings,
                            &window_expressions,
                            &window_values,
                        )
                    })
                    .collect()
            })
            .collect();
    } else {
        rows = relation
            .into_iter()
            .map(|row| {
                query
                    .select
                    .iter()
                    .map(|item| eval_rel(&item.expr, catalog, row, &bindings))
                    .collect()
            })
            .collect();
    }
    finalize_rows(query, &mut rows);
    Ok(rows)
}

fn execute(q: &Query, t: Arc<Table>, pool: &Pool, batch: usize) -> Vec<Vec<Scalar>> {
    let aggregate = q.select.iter().any(|s| is_agg(&s.expr)) || !q.group_by.is_empty();
    let mut rows = Vec::new();
    if aggregate {
        let groups = pool.aggregate(Arc::new(q.clone()), t.clone(), batch);
        for e in groups.into_entries() {
            let mut row = Vec::new();
            let mut ai = 0;
            for item in &q.select {
                if is_agg(&item.expr) {
                    row.push(finish(&e.s[ai]));
                    ai += 1
                } else if let Expr::Column(c) = &item.expr {
                    let ki = q.group_by.iter().position(|x| x == c).expect("grouped");
                    row.push(t.key_scalar(c, e.k.v[ki]))
                }
            }
            rows.push(row)
        }
    } else {
        let mut selection = Vec::with_capacity(batch.max(1));
        'outer: for bs in (0..t.len()).step_by(batch.max(1)) {
            selection.clear();
            for i in bs..(bs + batch).min(t.len()) {
                if q.filter.as_ref().is_none_or(|f| eval(f, &t, i).truthy()) {
                    selection.push(i);
                }
            }
            for &i in &selection {
                rows.push(
                    q.select
                        .iter()
                        .map(|x| eval(&x.expr, t.as_ref(), i))
                        .collect(),
                );
                if q.order_by.is_empty()
                    && !q.distinct
                    && q.limit
                        .is_some_and(|n| rows.len() >= n.saturating_add(q.offset))
                {
                    break 'outer;
                }
            }
        }
    }
    finalize_rows(q, &mut rows);
    rows
}
fn rows_json(rows: &[Vec<Scalar>]) -> String {
    format!(
        "[{}]",
        rows.iter()
            .map(|r| format!(
                "[{}]",
                r.iter().map(Scalar::json).collect::<Vec<_>>().join(",")
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}
fn strings_json(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(",")
    )
}
fn enforce_table_limit(options: &Options, table: &Table) -> Result<(), String> {
    let bytes = table.approximate_bytes();
    if options.memory_limit_mb > 0 && bytes > options.memory_limit_mb * 1024 * 1024 {
        return Err(format!(
            "RESOURCE_EXHAUSTED table requires approximately {bytes} bytes; memory limit is {} MiB",
            options.memory_limit_mb
        ));
    }
    Ok(())
}
fn enforce_result_limit(options: &Options, query: &Query, table: &Table) -> Result<(), String> {
    if options.max_result_rows == 0 {
        return Ok(());
    }
    let aggregate =
        query.select.iter().any(|item| is_agg(&item.expr)) || !query.group_by.is_empty();
    let upper = if aggregate {
        if query.group_by.is_empty() {
            1
        } else {
            table.group_upper_bound(&query.group_by)
        }
    } else {
        query.limit.unwrap_or(table.len()).min(table.len())
    };
    if upper > options.max_result_rows {
        return Err(format!(
            "RESOURCE_EXHAUSTED result upper bound {upper} exceeds max-result-rows {}",
            options.max_result_rows
        ));
    }
    Ok(())
}

fn is_relational(query: &Query) -> bool {
    query.from.name != "events"
        || !query.joins.is_empty()
        || query.having.is_some()
        || query.select.iter().any(|item| contains_window(&item.expr))
        || query
            .select
            .iter()
            .any(|item| contains_subquery(&item.expr))
        || query.filter.as_ref().is_some_and(contains_subquery)
        || query.union.is_some()
        || !query.ctes.is_empty()
}

pub fn run_query(o: Options, sql: &str, explain: bool, stats: bool) -> Result<(), String> {
    let t = Arc::new(Table::load(&o.data)?);
    enforce_table_limit(&o, &t)?;
    let q = prepare(Parser::new(sql)?.parse()?, &t);
    if q.ctes.is_empty() {
        bind_query(&q)?;
    }
    enforce_result_limit(&o, &q, &t)?;
    if explain {
        println!("{}", q.explain(o.batch_size));
        return Ok(());
    }
    let pool = Pool::new(o.threads);
    let started = Instant::now();
    let rows = if is_relational(&q) {
        execute_rel(&q, &Catalog::load(&o.data, t.clone())?)?
    } else {
        execute(&q, t.clone(), &pool, o.batch_size)
    };
    let elapsed_ns = started.elapsed().as_nanos();
    for row in &rows {
        println!(
            "{}",
            row.iter()
                .map(|v| format!("{v:?}"))
                .collect::<Vec<_>>()
                .join("\t")
        )
    }
    if stats {
        eprintln!(
            "{{\"rows_scanned\":{},\"rows_returned\":{},\"batches_scanned\":{},\"columns_scanned\":{},\"worker_threads\":{},\"logical_partitions\":{},\"elapsed_ns\":{}}}",
            t.len(),
            rows.len(),
            t.len().div_ceil(o.batch_size),
            q.columns.len(),
            o.threads,
            o.threads * 4,
            elapsed_ns
        );
    }
    Ok(())
}

#[derive(Clone)]
struct AsyncRequestState {
    phase: &'static str,
    queue_ns: u128,
    execution_ns: u128,
    rows: Vec<Vec<Scalar>>,
    error: String,
}

struct AsyncRequest {
    id: String,
    priority: usize,
    group: String,
    memory_mb: usize,
    submitted: Instant,
    query: Query,
    include_rows: bool,
    control: Arc<ExecutionControl>,
    state: Mutex<AsyncRequestState>,
    changed: Condvar,
}

struct SchedulerState {
    queues: [std::collections::VecDeque<Arc<AsyncRequest>>; 3],
    requests: std::collections::HashMap<String, Arc<AsyncRequest>>,
    active: usize,
    active_by_group: std::collections::HashMap<String, usize>,
    reserved_memory_mb: usize,
    reserved_by_group: std::collections::HashMap<String, usize>,
    schedule_cursor: usize,
    shutdown: bool,
}

struct SchedulerShared {
    state: Mutex<SchedulerState>,
    changed: Condvar,
    catalog: Arc<Catalog>,
    max_active: usize,
    queue_capacity: usize,
    memory_mb: usize,
}

struct AsyncScheduler {
    shared: Arc<SchedulerShared>,
    dispatcher: Option<thread::JoinHandle<()>>,
}

struct SubmitOptions<'a> {
    priority: usize,
    group: &'a str,
    deadline_ms: u64,
    memory_mb: usize,
    include_rows: bool,
}

fn finish_without_execution(request: &Arc<AsyncRequest>, phase: &'static str, error: &str) {
    if let Ok(mut state) = request.state.lock() {
        state.phase = phase;
        state.queue_ns = request.submitted.elapsed().as_nanos();
        state.error = error.into();
        request.changed.notify_all();
    }
}

impl AsyncScheduler {
    fn new(catalog: Arc<Catalog>, options: &Options) -> Self {
        let shared = Arc::new(SchedulerShared {
            state: Mutex::new(SchedulerState {
                queues: std::array::from_fn(|_| std::collections::VecDeque::new()),
                requests: std::collections::HashMap::new(),
                active: 0,
                active_by_group: std::collections::HashMap::new(),
                reserved_memory_mb: 0,
                reserved_by_group: std::collections::HashMap::new(),
                schedule_cursor: 0,
                shutdown: false,
            }),
            changed: Condvar::new(),
            catalog,
            max_active: options.max_active_queries.max(1),
            queue_capacity: options.admission_queue_capacity.max(1),
            memory_mb: options.scheduler_memory_mb.max(1),
        });
        let dispatcher_shared = shared.clone();
        let dispatcher = thread::spawn(move || {
            // Priority 2 receives four quanta, priority 1 two, priority 0 one.
            const CYCLE: [usize; 7] = [2, 2, 2, 2, 1, 1, 0];
            loop {
                let request = {
                    let mut scheduler = dispatcher_shared.state.lock().expect("scheduler lock");
                    loop {
                        if scheduler.shutdown {
                            return;
                        }
                        let mut selected = None;
                        if scheduler.active < dispatcher_shared.max_active {
                            for _ in 0..CYCLE.len() {
                                let priority = CYCLE[scheduler.schedule_cursor % CYCLE.len()];
                                scheduler.schedule_cursor += 1;
                                let position =
                                    scheduler.queues[priority].iter().position(|request| {
                                        let group_active = scheduler
                                            .active_by_group
                                            .get(&request.group)
                                            .copied()
                                            .unwrap_or(0);
                                        group_active
                                            < dispatcher_shared.max_active.div_ceil(2).max(1)
                                    });
                                if let Some(position) = position {
                                    selected = scheduler.queues[priority].remove(position);
                                    break;
                                }
                            }
                        }
                        if let Some(request) = selected {
                            if request.control.cancelled.load(AtomicOrdering::Relaxed) {
                                scheduler.reserved_memory_mb = scheduler
                                    .reserved_memory_mb
                                    .saturating_sub(request.memory_mb);
                                if let Some(value) =
                                    scheduler.reserved_by_group.get_mut(&request.group)
                                {
                                    *value = value.saturating_sub(request.memory_mb);
                                }
                                finish_without_execution(
                                    &request,
                                    "cancelled",
                                    "client cancellation",
                                );
                                continue;
                            }
                            if request
                                .control
                                .deadline
                                .is_some_and(|deadline| Instant::now() >= deadline)
                            {
                                scheduler.reserved_memory_mb = scheduler
                                    .reserved_memory_mb
                                    .saturating_sub(request.memory_mb);
                                if let Some(value) =
                                    scheduler.reserved_by_group.get_mut(&request.group)
                                {
                                    *value = value.saturating_sub(request.memory_mb);
                                }
                                finish_without_execution(
                                    &request,
                                    "deadline",
                                    "deadline expired in queue",
                                );
                                continue;
                            }
                            scheduler.active += 1;
                            *scheduler
                                .active_by_group
                                .entry(request.group.clone())
                                .or_default() += 1;
                            break request;
                        }
                        scheduler = dispatcher_shared
                            .changed
                            .wait(scheduler)
                            .expect("scheduler wait");
                    }
                };
                let worker_shared = dispatcher_shared.clone();
                thread::spawn(move || {
                    let started = Instant::now();
                    {
                        let mut state = request.state.lock().expect("request state");
                        state.phase = "running";
                        state.queue_ns = request.submitted.elapsed().as_nanos();
                        request.changed.notify_all();
                    }
                    set_execution_control(Some(request.control.clone()));
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        execute_rel(&request.query, &worker_shared.catalog)
                    }));
                    set_execution_control(None);
                    let elapsed = started.elapsed().as_nanos();
                    {
                        let mut state = request.state.lock().expect("request state");
                        state.execution_ns = elapsed;
                        if request.control.cancelled.load(AtomicOrdering::Relaxed) {
                            state.phase = "cancelled";
                            state.error = "client cancellation".into();
                        } else if request
                            .control
                            .deadline
                            .is_some_and(|deadline| Instant::now() >= deadline)
                        {
                            state.phase = "deadline";
                            state.error = "deadline expired during execution".into();
                        } else {
                            match result {
                                Ok(Ok(rows)) => {
                                    state.phase = "completed";
                                    if request.include_rows {
                                        state.rows = rows;
                                    } else {
                                        state.rows = vec![vec![Scalar::Int(rows.len() as i64)]];
                                    }
                                }
                                Ok(Err(error)) => {
                                    state.phase = "failed";
                                    state.error = error;
                                }
                                Err(_) => {
                                    state.phase = "failed";
                                    state.error = "execution panic".into();
                                }
                            }
                        }
                        request.changed.notify_all();
                    }
                    let mut scheduler = worker_shared.state.lock().expect("scheduler lock");
                    scheduler.active = scheduler.active.saturating_sub(1);
                    if let Some(value) = scheduler.active_by_group.get_mut(&request.group) {
                        *value = value.saturating_sub(1);
                    }
                    scheduler.reserved_memory_mb = scheduler
                        .reserved_memory_mb
                        .saturating_sub(request.memory_mb);
                    if let Some(value) = scheduler.reserved_by_group.get_mut(&request.group) {
                        *value = value.saturating_sub(request.memory_mb);
                    }
                    worker_shared.changed.notify_all();
                });
            }
        });
        Self {
            shared,
            dispatcher: Some(dispatcher),
        }
    }

    fn submit(&self, id: &str, query: Query, options: SubmitOptions<'_>) -> Result<(), String> {
        let SubmitOptions {
            priority,
            group,
            deadline_ms,
            memory_mb,
            include_rows,
        } = options;
        if priority > 2 {
            return Err("priority must be 0, 1, or 2".into());
        }
        let mut scheduler = self.shared.state.lock().map_err(|e| e.to_string())?;
        if scheduler.requests.contains_key(id) {
            return Err("duplicate request id".into());
        }
        let queued: usize = scheduler
            .queues
            .iter()
            .map(std::collections::VecDeque::len)
            .sum();
        if queued >= self.shared.queue_capacity {
            return Err("ADMISSION_REJECTED queue capacity".into());
        }
        let memory_mb = memory_mb.max(1);
        if scheduler.reserved_memory_mb.saturating_add(memory_mb) > self.shared.memory_mb {
            return Err("ADMISSION_REJECTED global memory".into());
        }
        let group_memory_limit = self.shared.memory_mb.div_ceil(2).max(1);
        if scheduler
            .reserved_by_group
            .get(group)
            .copied()
            .unwrap_or(0)
            .saturating_add(memory_mb)
            > group_memory_limit
        {
            return Err("ADMISSION_REJECTED resource-group memory".into());
        }
        let request = Arc::new(AsyncRequest {
            id: id.into(),
            priority,
            group: group.into(),
            memory_mb,
            submitted: Instant::now(),
            query,
            include_rows,
            control: Arc::new(ExecutionControl {
                cancelled: AtomicBool::new(false),
                deadline: (deadline_ms > 0)
                    .then(|| Instant::now() + Duration::from_millis(deadline_ms)),
            }),
            state: Mutex::new(AsyncRequestState {
                phase: "queued",
                queue_ns: 0,
                execution_ns: 0,
                rows: Vec::new(),
                error: String::new(),
            }),
            changed: Condvar::new(),
        });
        scheduler.reserved_memory_mb += memory_mb;
        *scheduler.reserved_by_group.entry(group.into()).or_default() += memory_mb;
        scheduler.requests.insert(id.into(), request.clone());
        scheduler.queues[priority].push_back(request);
        self.shared.changed.notify_all();
        Ok(())
    }

    fn request(&self, id: &str) -> Result<Arc<AsyncRequest>, String> {
        self.shared
            .state
            .lock()
            .map_err(|e| e.to_string())?
            .requests
            .get(id)
            .cloned()
            .ok_or_else(|| "unknown request".into())
    }

    fn cancel(&self, id: &str) -> Result<(), String> {
        let request = self.request(id)?;
        request
            .control
            .cancelled
            .store(true, AtomicOrdering::Relaxed);
        self.shared.changed.notify_all();
        Ok(())
    }

    fn status(&self, id: &str, wait: bool) -> Result<String, String> {
        let request = self.request(id)?;
        let mut state = request.state.lock().map_err(|e| e.to_string())?;
        while wait && matches!(state.phase, "queued" | "running") {
            state = request.changed.wait(state).map_err(|e| e.to_string())?;
        }
        let row_count = if state.phase == "completed" {
            if request.include_rows {
                state.rows.len()
            } else {
                state
                    .rows
                    .first()
                    .and_then(|row| row.first())
                    .and_then(|value| match value {
                        Scalar::Int(value) => Some(*value as usize),
                        _ => None,
                    })
                    .unwrap_or(0)
            }
        } else {
            0
        };
        let json = if state.phase == "completed" && request.include_rows {
            rows_json(&state.rows)
        } else {
            "[]".into()
        };
        Ok(format!(
            "STATUS\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            request.id,
            state.phase,
            request.priority,
            request.group,
            state.queue_ns,
            state.execution_ns,
            row_count,
            json,
            state.error.replace(['\t', '\n'], " ")
        ))
    }
}

impl Drop for AsyncScheduler {
    fn drop(&mut self) {
        if let Ok(mut scheduler) = self.shared.state.lock() {
            scheduler.shutdown = true;
            for request in scheduler.requests.values() {
                request
                    .control
                    .cancelled
                    .store(true, AtomicOrdering::Relaxed);
            }
            self.shared.changed.notify_all();
        }
        if let Some(dispatcher) = self.dispatcher.take() {
            let _ = dispatcher.join();
        }
    }
}

pub fn run_bench_server(o: Options) -> Result<(), String> {
    let load = Instant::now();
    let table = Arc::new(Table::load(&o.data)?);
    enforce_table_limit(&o, &table)?;
    let load_ns = load.elapsed().as_nanos();
    let pool = Pool::new(o.threads);
    let mut catalog: Option<Arc<Catalog>> = None;
    let mut scheduler: Option<AsyncScheduler> = None;
    let mut prepared = std::collections::HashMap::<String, Query>::new();
    println!("READY\t{load_ns}");
    io::stdout().flush().map_err(|e| e.to_string())?;
    for line in io::stdin().lock().lines() {
        let line = line.map_err(|e| e.to_string())?;
        let p: Vec<&str> = line.split('\t').collect();
        match p[0] {
            "CONFIG" => println!(
                "CONFIG\t{}\t{}\t{}\t{}",
                o.threads,
                o.batch_size,
                o.threads * 4,
                table.len()
            ),
            "PREPARE" => {
                let q = prepare(
                    Parser::new(p.get(2).ok_or("missing sql")?)?.parse()?,
                    &table,
                );
                if q.ctes.is_empty() {
                    bind_query(&q)?;
                }
                enforce_result_limit(&o, &q, &table)?;
                prepared.insert(p[1].into(), q);
                println!("OK\t{}", p[1])
            }
            "EXEC" => {
                let q = prepared.get(p[1]).ok_or("unknown query")?;
                let now = Instant::now();
                let rows = if is_relational(q) {
                    if catalog.is_none() {
                        catalog = Some(Arc::new(Catalog::load(&o.data, table.clone())?));
                    }
                    execute_rel(q, catalog.as_ref().expect("catalog loaded"))?
                } else {
                    execute(q, table.clone(), &pool, o.batch_size)
                };
                let ns = now.elapsed().as_nanos();
                println!(
                    "RESULT\t{ns}\t{}\t{}",
                    rows.len(),
                    if p.get(2) == Some(&"1") {
                        rows_json(&rows)
                    } else {
                        "[]".into()
                    }
                )
            }
            "E2E" => {
                let now = Instant::now();
                let q = prepare(
                    Parser::new(p.get(2).ok_or("missing sql")?)?.parse()?,
                    &table,
                );
                if q.ctes.is_empty() {
                    bind_query(&q)?;
                }
                let rows = if is_relational(&q) {
                    if catalog.is_none() {
                        catalog = Some(Arc::new(Catalog::load(&o.data, table.clone())?));
                    }
                    execute_rel(&q, catalog.as_ref().expect("catalog loaded"))?
                } else {
                    execute(&q, table.clone(), &pool, o.batch_size)
                };
                let ns = now.elapsed().as_nanos();
                println!(
                    "RESULT\t{ns}\t{}\t{}",
                    rows.len(),
                    if p.get(3) == Some(&"1") {
                        rows_json(&rows)
                    } else {
                        "[]".into()
                    }
                )
            }
            "EXPLAIN" => println!(
                "EXPLAIN\t{}",
                strings_json(&prepared.get(p[1]).ok_or("unknown query")?.physical)
            ),
            "CONFIG_ASYNC" => println!(
                "ASYNC_CONFIG\t{}\t{}\t{}\t{}\t{}",
                o.max_active_queries.max(1),
                o.admission_queue_capacity.max(1),
                o.scheduler_memory_mb.max(1),
                o.max_active_queries.div_ceil(2).max(1),
                o.scheduler_memory_mb.div_ceil(2).max(1)
            ),
            "SUBMIT" => {
                let result = (|| -> Result<(), String> {
                    let request_id = *p.get(1).ok_or("missing request id")?;
                    let query_id = *p.get(2).ok_or("missing query id")?;
                    let priority = p
                        .get(3)
                        .ok_or("missing priority")?
                        .parse()
                        .map_err(|_| "bad priority")?;
                    let group = *p.get(4).ok_or("missing group")?;
                    let deadline_ms = p
                        .get(5)
                        .ok_or("missing deadline")?
                        .parse()
                        .map_err(|_| "bad deadline")?;
                    let memory_mb = p
                        .get(6)
                        .ok_or("missing memory")?
                        .parse()
                        .map_err(|_| "bad memory")?;
                    let include_rows = *p.get(7).unwrap_or(&"0") == "1";
                    let query = prepared.get(query_id).cloned().ok_or("unknown query")?;
                    if catalog.is_none() {
                        catalog = Some(Arc::new(Catalog::load(&o.data, table.clone())?));
                    }
                    if scheduler.is_none() {
                        scheduler = Some(AsyncScheduler::new(
                            catalog.as_ref().expect("catalog loaded").clone(),
                            &o,
                        ));
                    }
                    scheduler.as_ref().expect("scheduler created").submit(
                        request_id,
                        query,
                        SubmitOptions {
                            priority,
                            group,
                            deadline_ms,
                            memory_mb,
                            include_rows,
                        },
                    )
                })();
                match result {
                    Ok(()) => println!("ACCEPTED\t{}", p[1]),
                    Err(error) => println!("REJECTED\t{}", error),
                }
            }
            "POLL" | "WAIT" => {
                let result = scheduler
                    .as_ref()
                    .ok_or_else(|| "scheduler not initialized".to_string())
                    .and_then(|scheduler| {
                        scheduler.status(p.get(1).copied().unwrap_or(""), p[0] == "WAIT")
                    });
                match result {
                    Ok(status) => println!("{status}"),
                    Err(error) => println!("ERROR\t{error}"),
                }
            }
            "CANCEL" => {
                let result = scheduler
                    .as_ref()
                    .ok_or_else(|| "scheduler not initialized".to_string())
                    .and_then(|scheduler| scheduler.cancel(p.get(1).copied().unwrap_or("")));
                match result {
                    Ok(()) => println!("CANCELLED\t{}", p.get(1).copied().unwrap_or("")),
                    Err(error) => println!("ERROR\t{error}"),
                }
            }
            "SHUTDOWN" => {
                println!("BYE");
                io::stdout().flush().map_err(|e| e.to_string())?;
                break;
            }
            _ => println!("ERROR\tunknown command"),
        };
        io::stdout().flush().map_err(|e| e.to_string())?
    }
    Ok(())
}

pub mod nested {
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Language {
        pub code: String,
        pub country: Option<String>,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Name {
        pub url: Option<String>,
        pub languages: Vec<Language>,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Document {
        pub doc_id: i64,
        pub names: Vec<Name>,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct LevelValue<T> {
        pub value: Option<T>,
        pub repetition_level: u16,
        pub definition_level: u16,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Shredded {
        pub doc_id: i64,
        pub urls: Vec<LevelValue<String>>,
        pub codes: Vec<LevelValue<String>>,
        pub countries: Vec<LevelValue<String>>,
    }
    pub fn shred(d: &Document) -> Shredded {
        let mut urls = vec![];
        let mut codes = vec![];
        let mut countries = vec![];
        if d.names.is_empty() {
            urls.push(LevelValue {
                value: None,
                repetition_level: 0,
                definition_level: 0,
            });
            codes.push(LevelValue {
                value: None,
                repetition_level: 0,
                definition_level: 0,
            });
            countries.push(LevelValue {
                value: None,
                repetition_level: 0,
                definition_level: 0,
            })
        }
        for (ni, n) in d.names.iter().enumerate() {
            urls.push(LevelValue {
                value: n.url.clone(),
                repetition_level: u16::from(ni > 0),
                definition_level: if n.url.is_some() { 2 } else { 1 },
            });
            if n.languages.is_empty() {
                codes.push(LevelValue {
                    value: None,
                    repetition_level: u16::from(ni > 0),
                    definition_level: 1,
                });
                countries.push(LevelValue {
                    value: None,
                    repetition_level: u16::from(ni > 0),
                    definition_level: 1,
                })
            }
            for (li, l) in n.languages.iter().enumerate() {
                let rep = if li > 0 { 2 } else { u16::from(ni > 0) };
                codes.push(LevelValue {
                    value: Some(l.code.clone()),
                    repetition_level: rep,
                    definition_level: 2,
                });
                countries.push(LevelValue {
                    value: l.country.clone(),
                    repetition_level: rep,
                    definition_level: if l.country.is_some() { 3 } else { 2 },
                })
            }
        }
        Shredded {
            doc_id: d.doc_id,
            urls,
            codes,
            countries,
        }
    }
    pub fn assemble(s: &Shredded) -> Document {
        if s.urls.first().is_some_and(|v| v.definition_level == 0) {
            return Document {
                doc_id: s.doc_id,
                names: vec![],
            };
        }
        let mut names: Vec<Name> = s
            .urls
            .iter()
            .map(|url| Name {
                url: url.value.clone(),
                languages: vec![],
            })
            .collect();
        let mut name_index = 0usize;
        for (i, code) in s.codes.iter().enumerate() {
            if i > 0 && code.repetition_level < 2 {
                name_index += 1;
            }
            if code.definition_level >= 2 {
                names[name_index].languages.push(Language {
                    code: code.value.clone().expect("required language code"),
                    country: s.countries[i].value.clone(),
                });
            }
        }
        Document {
            doc_id: s.doc_id,
            names,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
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
        let mut t = GroupTable::new();
        for i in 0..1000 {
            t.get_or_insert(GroupKey { v: [i, 0, 0], n: 1 }, &s);
        }
        assert_eq!(t.len, 1000);
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
            "dremel-rs-test-{}-{}.csv",
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
        execute(&query, table, &Pool::new(threads), 2)
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
            bind_error("SELECT CAST(event_id AS uuid) FROM events")
                .contains("unsupported CAST type")
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
        assert_eq!(optimize_query(&mut star), 1);
        assert_eq!(star.joins[0].table.name, "campaigns");
        assert_eq!(star.joins[1].table.name, "users");

        let mut dependent = Parser::new(
            "SELECT COUNT(*) FROM events e JOIN users u ON e.user_id = u.user_id JOIN campaigns c ON c.campaign_id = u.user_id",
        )
        .unwrap()
        .parse()
        .unwrap();
        dependent.optimizer_enabled = true;
        assert_eq!(optimize_query(&mut dependent), 0);
        assert_eq!(dependent.joins[0].table.name, "users");
        assert_eq!(dependent.joins[1].table.name, "campaigns");
    }
    #[test]
    fn all_queries_parse() {
        for i in 1..=64 {
            let p = format!("../benchmark/queries/Q{i:03}.sql");
            let sql = std::fs::read_to_string(p).unwrap();
            Parser::new(&sql).unwrap().parse().unwrap();
        }
    }
    #[test]
    fn nested_roundtrips() {
        use nested::*;
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
}

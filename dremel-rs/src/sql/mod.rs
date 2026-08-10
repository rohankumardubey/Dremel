use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Token {
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

pub(crate) fn lex(sql: &str) -> Result<Vec<Token>, String> {
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
pub(crate) type NamedQuery = (String, Box<Query>);

pub(crate) struct Parser {
    pub(crate) t: Vec<Token>,
    pub(crate) p: usize,
}
impl Parser {
    pub(crate) fn new(sql: &str) -> Result<Self, String> {
        Ok(Self { t: lex(sql)?, p: 0 })
    }
    pub(crate) fn peek(&self) -> &Token {
        &self.t[self.p]
    }
    pub(crate) fn next(&mut self) -> Token {
        let v = self.t[self.p].clone();
        self.p += 1;
        v
    }
    pub(crate) fn word(&mut self, w: &str) -> bool {
        if self.peek() == &Token::Word(w.into()) {
            self.p += 1;
            true
        } else {
            false
        }
    }
    pub(crate) fn expect_word(&mut self, w: &str) -> Result<(), String> {
        if self.word(w) {
            Ok(())
        } else {
            Err(format!("expected {w}, got {:?}", self.peek()))
        }
    }
    pub(crate) fn table_ref(&mut self) -> Result<TableRef, String> {
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
    pub(crate) fn relation_ref(&mut self) -> Result<(TableRef, Option<NamedQuery>), String> {
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
    pub(crate) fn identifier(&mut self) -> Result<String, String> {
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
    pub(crate) fn window(&mut self, name: String, args: Vec<Expr>) -> Result<Expr, String> {
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
    pub(crate) fn subquery(&mut self) -> Result<Query, String> {
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
    pub(crate) fn parse(mut self) -> Result<Query, String> {
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
    pub(crate) fn expr(&mut self, min: u8) -> Result<Expr, String> {
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
    pub(crate) fn primary(&mut self) -> Result<Expr, String> {
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

pub(crate) fn is_agg(e: &Expr) -> bool {
    matches!(e,Expr::Func(n,_) if ["count","sum","avg","min","max"].contains(&n.as_str()))
}
pub(crate) fn contains_agg(e: &Expr) -> bool {
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
pub(crate) fn contains_window(expression: &Expr) -> bool {
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
pub(crate) fn contains_subquery(expression: &Expr) -> bool {
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
pub(crate) fn collect(e: &Expr, s: &mut BTreeSet<String>) {
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

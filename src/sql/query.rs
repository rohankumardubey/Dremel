use super::*;

impl Parser {
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
}

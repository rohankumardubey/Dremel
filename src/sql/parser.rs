use super::*;

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
}

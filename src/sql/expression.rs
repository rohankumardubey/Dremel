use super::*;

impl Parser {
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

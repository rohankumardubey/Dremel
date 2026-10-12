use super::*;

impl Parser {
    pub(super) fn unnest_ref(&mut self) -> Result<(TableRef, UnnestSpec), String> {
        self.expect_word("unnest")?;
        if self.next() != Token::LParen {
            return Err("UNNEST requires (field_path)".into());
        }
        let path = self.identifier()?;
        if self.next() != Token::RParen {
            return Err("UNNEST supports exactly one LIST field path".into());
        }
        let ordinality = if self.word("with") {
            self.expect_word("ordinality")?;
            Some("ordinality".to_string())
        } else {
            None
        };
        let _ = self.word("as");
        let alias = match self.next() {
            Token::Word(alias)
                if !["on", "where", "join", "order", "group", "limit"]
                    .contains(&alias.as_str()) =>
            {
                alias
            }
            _ => return Err("UNNEST requires a relation alias".into()),
        };
        let mut spec = UnnestSpec {
            path,
            column: "value".into(),
            ordinality,
        };
        if self.peek() == &Token::LParen {
            self.p += 1;
            spec.column = self.identifier()?;
            if spec.column.contains('.') {
                return Err("UNNEST column alias must be a simple identifier".into());
            }
            if spec.ordinality.is_some() {
                if self.next() != Token::Comma {
                    return Err("WITH ORDINALITY requires two column aliases".into());
                }
                let name = self.identifier()?;
                if name.contains('.') || name == spec.column {
                    return Err("UNNEST column aliases must be distinct simple identifiers".into());
                }
                spec.ordinality = Some(name);
            }
            if self.next() != Token::RParen {
                return Err("wrong number of UNNEST column aliases".into());
            }
        }
        Ok((
            TableRef {
                name: "__unnest".into(),
                alias,
            },
            spec,
        ))
    }
}

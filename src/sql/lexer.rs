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

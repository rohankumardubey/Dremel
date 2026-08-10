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

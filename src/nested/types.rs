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

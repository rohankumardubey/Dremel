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

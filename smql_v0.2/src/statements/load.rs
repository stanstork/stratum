#[derive(Debug, Clone)]
pub struct Load {
    pub tables: Vec<String>,
    pub matches: Vec<MatchSpec>,
}

#[derive(Debug, Clone)]
pub struct MatchPair(pub String, pub String, pub String, pub String);

#[derive(Debug, Clone)]
pub struct MatchSpec {
    pub mappings: Vec<MatchPair>,
}

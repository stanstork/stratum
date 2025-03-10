#[derive(Debug, Clone)]
pub struct FetchRowsRequest {
    pub table: String,
    pub columns: Vec<String>,
    pub limit: usize,
    pub offset: Option<usize>,
}

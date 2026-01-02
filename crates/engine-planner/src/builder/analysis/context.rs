use crate::builder::{
    analyzers::sample::SampleConfig,
    infra::metadata_cache::MetadataCache,
    utils::{MaskingPolicy, dialect_for_adapter},
};
use crate::plan::sample::method::SamplingMethod;
use connectors::adapter::Adapter;
use engine_core::schema::plan::SchemaPlan;
use model::core::value::Value;
use model::transform::mapping::TransformationMetadata;
use query_builder::dialect::Dialect;
use std::sync::Arc;
use std::time::Duration;

pub struct AnalysisContextConfig {
    pub metadata_timeout: Duration,
    pub enable_sampling: bool,
    pub sample_size: usize,
    pub sample_method: SamplingMethod,
    pub sample_ids: Option<Vec<Value>>,
    pub id_column: String,
    pub auto_mask_sensitive: bool,
    pub mask_columns: Vec<String>,
    pub use_exact_where: bool,
}

/// Shared context passed to all analyzers during pipeline analysis
pub struct AnalysisContext {
    pub source_adapter: Arc<Adapter>,
    pub dest_adapter: Arc<Adapter>,
    pub source_cache: Arc<MetadataCache>,
    pub dest_cache: Arc<MetadataCache>,
    pub schema_plan: Arc<SchemaPlan>,
    pub mapping: Arc<TransformationMetadata>,

    /// Timeout for database operations
    pub timeout: Duration,

    pub sampling: SampleConfig,

    /// Masking policy for sensitive data
    pub masking: MaskingPolicy,

    /// Use exact COUNT for filtered rows (slower but accurate) vs EXPLAIN estimates (faster)
    pub use_exact_where: bool,

    pub source_dialect: Arc<dyn Dialect>,
    pub dest_dialect: Arc<dyn Dialect>,
}

impl AnalysisContext {
    pub fn new(
        source_adapter: Arc<Adapter>,
        dest_adapter: Arc<Adapter>,
        schema_plan: Arc<SchemaPlan>,
        mapping: Arc<TransformationMetadata>,
        config: AnalysisContextConfig,
    ) -> Self {
        // Create metadata caches
        let source_cache = Arc::new(MetadataCache::new(
            Arc::clone(&source_adapter),
            config.metadata_timeout,
        ));
        let dest_cache = Arc::new(MetadataCache::new(
            Arc::clone(&dest_adapter),
            config.metadata_timeout,
        ));

        // Resolve SQL dialects
        let source_dialect = dialect_for_adapter(&source_adapter);
        let dest_dialect = dialect_for_adapter(&dest_adapter);

        // Create masking policy
        let masking = MaskingPolicy::new(config.auto_mask_sensitive, config.mask_columns.clone());

        // Create sampling configuration
        let sampling = SampleConfig {
            enabled: config.enable_sampling,
            size: config.sample_size,
            method: config.sample_method,
            mask_columns: config.mask_columns,
            auto_mask_sensitive: config.auto_mask_sensitive,
            sample_ids: config.sample_ids,
            id_column: config.id_column,
        };

        Self {
            source_adapter,
            dest_adapter,
            source_cache,
            dest_cache,
            schema_plan,
            mapping,
            timeout: config.metadata_timeout,
            sampling,
            masking,
            use_exact_where: config.use_exact_where,
            source_dialect,
            dest_dialect,
        }
    }
}

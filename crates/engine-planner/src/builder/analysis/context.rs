use crate::builder::{
    analyzers::sample::SampleConfig, infra::metadata_cache::MetadataCache, utils::MaskingPolicy,
};
use crate::plan::sample::method::SamplingMethod;
use engine_core::schema::plan::SchemaPlan;
use engine_core::schema::type_registry::Dialect;
use engine_processing::io::driver::SchemaDriver;
use model::core::value::Value;
use model::transform::mapping::TransformationMetadata;
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
pub struct AnalysisContext<S: SchemaDriver, D: SchemaDriver> {
    pub src_driver: Arc<S>,
    pub dest_driver: Arc<D>,
    pub source_cache: Arc<MetadataCache<S>>,
    pub dest_cache: Arc<MetadataCache<D>>,
    pub schema_plan: Arc<SchemaPlan>,
    pub mapping: Arc<TransformationMetadata>,

    /// Timeout for database operations
    pub timeout: Duration,

    pub sampling: SampleConfig,

    /// Masking policy for sensitive data
    pub masking: MaskingPolicy,

    /// Use exact COUNT for filtered rows (slower but accurate) vs EXPLAIN estimates (faster)
    pub use_exact_where: bool,

    pub source_dialect: Dialect,
    pub dest_dialect: Dialect,
}

impl<S: SchemaDriver, D: SchemaDriver> AnalysisContext<S, D> {
    pub fn new(
        src_driver: Arc<S>,
        src_dialect: Dialect,
        dest_driver: Arc<D>,
        dest_dialect: Dialect,
        schema_plan: Arc<SchemaPlan>,
        mapping: Arc<TransformationMetadata>,
        config: AnalysisContextConfig,
    ) -> Self {
        // Create metadata caches
        let source_cache = Arc::new(MetadataCache::new(
            Arc::clone(&src_driver),
            src_dialect,
            config.metadata_timeout,
        ));
        let dest_cache = Arc::new(MetadataCache::new(
            Arc::clone(&dest_driver),
            dest_dialect,
            config.metadata_timeout,
        ));

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
            src_driver,
            dest_driver,
            source_cache,
            dest_cache,
            schema_plan,
            mapping,
            timeout: config.metadata_timeout,
            sampling,
            masking,
            use_exact_where: config.use_exact_where,
            source_dialect: src_dialect,
            dest_dialect,
        }
    }
}

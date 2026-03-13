use serde::Serialize;

/// Flexible row count - can be exact or estimated
#[derive(Serialize, Debug, Clone, Default)]
pub struct RowCount {
    pub value: u64,
    pub is_estimated: bool,
    /// Confidence level between 0.0 (no confidence) and 1.0 (certain)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
}

impl RowCount {
    pub fn exact(value: u64) -> Self {
        Self {
            value,
            is_estimated: false,
            confidence: None,
        }
    }

    pub fn estimated(value: u64, confidence: f32) -> Self {
        Self {
            value,
            is_estimated: true,
            confidence: Some(confidence),
        }
    }

    pub fn unknown() -> Self {
        Self {
            value: 0,
            is_estimated: true,
            confidence: None,
        }
    }

    pub fn is_unknown(&self) -> bool {
        self.value == 0 && self.is_estimated
    }

    /// Format for display: "1.2M" or "~1.2M"
    pub fn display(&self) -> String {
        let num = match self.value {
            n if n >= 1_000_000_000 => format!("{:.1}B", n as f64 / 1e9),
            n if n >= 1_000_000 => format!("{:.1}M", n as f64 / 1e6),
            n if n >= 1_000 => format!("{:.1}K", n as f64 / 1e3),
            n => n.to_string(),
        };

        if self.is_estimated {
            format!("~{}", num)
        } else {
            num
        }
    }

    /// Sum multiple RowCounts, treating unknowns as zero
    pub fn sum<'a, I>(counts: I) -> Self
    where
        I: IntoIterator<Item = &'a RowCount>,
    {
        let mut total = 0u64;
        let mut any_estimated = false;
        let mut min_confidence = 1.0f32;
        let mut has_confidence = false;

        for rc in counts {
            total += rc.value;
            if rc.is_estimated {
                any_estimated = true;
            }
            if let Some(conf) = rc.confidence {
                has_confidence = true;
                if conf < min_confidence {
                    min_confidence = conf;
                }
            }
        }
        RowCount {
            value: total,
            is_estimated: any_estimated,
            confidence: if has_confidence {
                Some(min_confidence)
            } else {
                None
            },
        }
    }
}

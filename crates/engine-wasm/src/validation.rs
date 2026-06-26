use model::core::types::Type;

#[derive(Debug, Clone, PartialEq)]
pub enum Compat {
    Ok,
    /// Allowed but may lose information; carries a human-readable note.
    Lossy(String),
    Incompatible,
}

/// Is a value of `source` type acceptable where the plugin declared `expected`?
pub fn check(source: &Type, expected: &Type) -> Compat {
    use Type::*;

    if source == expected {
        return Compat::Ok;
    }

    match (source, expected) {
        // Anything -> string is fine; the JSON exchange stringifies losslessly.
        (_, Text { .. }) | (_, Varchar { .. }) | (_, Char { .. }) => Compat::Ok,

        // Same family, different size/params - accept silently.
        (Int { .. }, Int { .. }) | (Float { .. }, Float { .. }) => Compat::Ok,
        (Decimal { .. }, Decimal { .. }) => Compat::Ok,

        // Numeric widening to float - safe-ish but worth a note.
        (Int { .. }, Float { .. }) | (Decimal { .. }, Float { .. }) => {
            Compat::Lossy("integer/decimal -> float may lose precision".to_string())
        }

        // Binary families are interchangeable enough for plan time.
        (
            Binary { .. } | Varbinary { .. } | Blob { .. },
            Binary { .. } | Varbinary { .. } | Blob { .. },
        ) => Compat::Ok,

        // Temporal - exact match handled above; cross-temporal is lossy.
        (Date, Timestamp { .. }) | (Timestamp { .. }, Date) => {
            Compat::Lossy("date <-> timestamp truncation".to_string())
        }

        // Unknown on either side: don't block - emit Lossy so the row is
        // checked at runtime instead.
        (Unknown { .. }, _) | (_, Unknown { .. }) => {
            Compat::Lossy("type is not fully introspectable; runtime check only".to_string())
        }

        _ => Compat::Incompatible,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::core::types::{FloatSize, IntSize};

    fn i64_ty() -> Type {
        Type::Int {
            bits: IntSize::I64,
            unsigned: false,
            auto_increment: false,
        }
    }
    fn i32_ty() -> Type {
        Type::Int {
            bits: IntSize::I32,
            unsigned: false,
            auto_increment: false,
        }
    }
    fn f64_ty() -> Type {
        Type::Float {
            bits: FloatSize::F64,
        }
    }
    fn text_ty() -> Type {
        Type::Text { charset: None }
    }

    #[test]
    fn exact_match_is_ok() {
        assert_eq!(check(&i64_ty(), &i64_ty()), Compat::Ok);
        assert_eq!(check(&Type::Boolean, &Type::Boolean), Compat::Ok);
        assert_eq!(check(&Type::Date, &Type::Date), Compat::Ok);
    }

    #[test]
    fn anything_to_text_is_ok() {
        assert_eq!(check(&i64_ty(), &text_ty()), Compat::Ok);
        assert_eq!(check(&Type::Boolean, &text_ty()), Compat::Ok);
        assert_eq!(check(&Type::Date, &text_ty()), Compat::Ok);
    }

    #[test]
    fn int_to_float_is_lossy_not_incompatible() {
        match check(&i64_ty(), &f64_ty()) {
            Compat::Lossy(note) => assert!(note.to_lowercase().contains("precision")),
            other => panic!("expected Lossy, got {:?}", other),
        }
    }

    #[test]
    fn same_int_family_is_ok() {
        assert_eq!(check(&i64_ty(), &i32_ty()), Compat::Ok);
    }

    #[test]
    fn bool_to_int_is_incompatible() {
        assert_eq!(check(&Type::Boolean, &i64_ty()), Compat::Incompatible);
    }

    #[test]
    fn date_to_timestamp_is_lossy() {
        assert!(matches!(
            check(
                &Type::Date,
                &Type::Timestamp {
                    precision: None,
                    with_tz: false
                }
            ),
            Compat::Lossy(_)
        ));
    }

    #[test]
    fn unknown_does_not_block() {
        let unk = Type::Unknown {
            source_name: "weird".to_string(),
            fallback_ddl: String::new(),
        };
        assert!(matches!(check(&unk, &i64_ty()), Compat::Lossy(_)));
        assert!(matches!(check(&i64_ty(), &unk), Compat::Lossy(_)));
    }
}

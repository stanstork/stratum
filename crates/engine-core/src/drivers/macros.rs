// Driver dispatch macros - generate the cartesian product of all driver
// combinations for monomorphized generic calls.
//
// Adding a new driver requires 3 changes:
//   1. Add variant to `DriverRef` enum
//   2. Add mapping line in `dispatch_drivers!` macro
//   3. Add arm in `resolve_driver()`
//
// Everything else (source creation, destination creation, settings
// validation, metadata caching) is handled automatically.

/// Dispatches a body across all (Source, Destination) driver type combinations.
///
/// The body receives `src`/`dst` as `&Arc<ConcreteDriver>` and can reference
/// `Src`/`Dst` as type aliases for the concrete driver types.
///
/// ```ignore
/// let result = dispatch_drivers!(&source_driver, &dest_driver, |src, dst| {
///     some_generic_fn::<Src, Dst>(src.clone(), dst.clone()).await?
/// });
/// ```
#[macro_export]
macro_rules! dispatch_drivers {
    ($src:expr, $dst:expr, |$s:ident, $d:ident| $body:expr) => {{
        dispatch_drivers!(@src $src, $dst, $s, $d, $body,
            [Postgres =>  connectors::drivers::postgres::driver::PgDriver],
            [MySql => connectors::drivers::mysql::driver::MySqlDriver],
        )
    }};

    // Internal: match source driver
    (@src $src:expr, $dst:expr, $s:ident, $d:ident, $body:expr,
        $([$sv:ident => $st:ty]),*,
    ) => {
        match $src {
            $(
                DriverRef::$sv($s) => {
                    dispatch_drivers!(@dst $dst, $s, $d, $body, $st,
                        [Postgres => connectors::drivers::postgres::driver::PgDriver],
                        [MySql => connectors::drivers::mysql::driver::MySqlDriver],
                    )
                }
            )*
        }
    };

    // Internal: match dest driver (receives source type $st from previous level)
    (@dst $dst:expr, $s:ident, $d:ident, $body:expr, $st:ty,
        $([$dv:ident => $dt:ty]),*,
    ) => {
        match $dst {
            $(
                DriverRef::$dv($d) => {
                    dispatch_drivers!(@invoke $s, $d, $body, $st, $dt)
                }
            )*
        }
    };

    // Internal: expose Src/Dst type aliases and execute the body
    (@invoke $s:ident, $d:ident, $body:expr, $S:ty, $D:ty) => {{
        #[allow(dead_code)]
        type Src = $S;
        #[allow(dead_code)]
        type Dst = $D;
        $body
    }};
}

/// Dispatches a body across all single-driver variants.
///
/// Use for operations that apply to one driver at a time (source creation,
/// metadata fetching, etc).
///
/// ```ignore
/// dispatch_driver!(&driver_ref, |d| {
///     Source::new(d.clone(), pipeline, mapping, offset.clone()).await
/// })
/// ```
#[macro_export]
macro_rules! dispatch_driver {
    ($driver:expr, |$d:ident| $body:expr) => {{
        // ── Must stay in sync with dispatch_drivers! variants ──
        match $driver {
            $crate::drivers::DriverRef::Postgres($d) => $body,
            $crate::drivers::DriverRef::MySql($d) => $body,
        }
    }};
}

//! Tests for expression parsing and AST building

use smql_syntax::builder::parse;

#[test]
fn test_parse_expressions() {
    let input = r#"
        define {
            a = 1 + 2
            b = 3 * 4
            c = (5 + 6) * 7
            d = true && false
            e = x > 10
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 5);
}

#[test]
fn test_parse_function_calls() {
    let input = r#"
        connection "db" {
            url = env("DB_URL")
            created = now()
            date = date("2024-01-01")
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let conn = &doc.connections[0];
    assert_eq!(conn.attributes.len(), 3);
}

#[test]
fn test_parse_array_literals() {
    let input = r#"
        define {
            numbers = [1, 2, 3]
            strings = ["a", "b", "c"]
            empty = []
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 3);
}

#[test]
fn test_parse_when_expression() {
    let input = r#"
        define {
            status = when {
                count > 100 then "high"
                count > 50 then "medium"
                else "low"
            }
        }
    "#;

    let result = parse(input);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let doc = result.unwrap();
    let define = doc.define_block.unwrap();
    assert_eq!(define.attributes.len(), 1);
}

#[test]
fn test_build_plugin_call_in_select_block() {
    use smql_syntax::ast::expr::ExpressionKind;

    let input = r#"
        pipeline "p" {
            from { connection = connection.src }
            to   { connection = connection.dst }
            select {
                score = plugin.score_risk({ amount: charges.amount, country: charges.country })
            }
        }
    "#;
    let doc = parse(input).expect("should parse");
    let pipeline = &doc.pipelines[0];
    let select = pipeline.select_block.as_ref().expect("select block");
    assert_eq!(select.fields.len(), 1);
    let value = &select.fields[0].value;
    match &value.kind {
        ExpressionKind::PluginCall(call) => {
            assert_eq!(call.plugin_name, "score_risk");
            assert_eq!(call.inputs.len(), 2);
            assert_eq!(call.inputs[0].plugin_field, "amount");
            assert_eq!(
                call.inputs[0].source_ref.segments,
                vec!["charges", "amount"]
            );
            assert_eq!(call.inputs[1].plugin_field, "country");
            assert_eq!(
                call.inputs[1].source_ref.segments,
                vec!["charges", "country"]
            );
        }
        other => panic!("expected PluginCall, got {:?}", other),
    }
}

#[test]
fn test_build_plugin_call_with_bare_ident_input() {
    use smql_syntax::ast::expr::ExpressionKind;

    let input = r#"
        pipeline "p" {
            from { connection = connection.src }
            to   { connection = connection.dst }
            select {
                out = plugin.my_plugin({ x: col_name })
            }
        }
    "#;
    let doc = parse(input).expect("should parse");
    let value = &doc.pipelines[0].select_block.as_ref().unwrap().fields[0].value;
    match &value.kind {
        ExpressionKind::PluginCall(call) => {
            assert_eq!(call.plugin_name, "my_plugin");
            assert_eq!(call.inputs.len(), 1);
            assert_eq!(call.inputs[0].plugin_field, "x");
            assert_eq!(call.inputs[0].source_ref.segments, vec!["col_name"]);
        }
        other => panic!("expected PluginCall, got {:?}", other),
    }
}

#[test]
fn test_build_wasm_rule_inside_validate_block() {
    let input = r#"
        pipeline "p" {
            from { connection = connection.src }
            to   { connection = connection.dst }
            validate {
                rule "fraud_screen" {
                    filter  = plugin.check_fraud({ amount: charges.amount })
                    on_fail = skip
                }
            }
        }
    "#;
    let doc = parse(input).expect("should parse");
    let validate = doc.pipelines[0]
        .validate_block
        .as_ref()
        .expect("validate block");
    assert!(validate.checks.is_empty(), "no asserts in this fixture");
    assert_eq!(validate.wasm_rules.len(), 1);
    let r = &validate.wasm_rules[0];
    assert_eq!(r.name, "fraud_screen");
    assert_eq!(r.filter.plugin_name, "check_fraud");
    assert_eq!(r.filter.inputs.len(), 1);
    assert_eq!(r.filter.inputs[0].plugin_field, "amount");
    assert_eq!(r.on_fail, "skip");
}

#[test]
fn test_build_validate_block_mixed_assert_and_wasm() {
    let input = r#"
        pipeline "p" {
            from { connection = connection.src }
            to   { connection = connection.dst }
            validate {
                assert "positive_amount" {
                    check   = charges.amount > 0
                    message = "amount must be positive"
                }
                rule "fraud_screen" {
                    filter  = plugin.check_fraud({ amount: charges.amount })
                    on_fail = skip
                }
            }
        }
    "#;
    let doc = parse(input).expect("should parse");
    let v = doc.pipelines[0].validate_block.as_ref().unwrap();
    assert_eq!(v.checks.len(), 1);
    assert_eq!(v.checks[0].label, "positive_amount");
    assert_eq!(v.wasm_rules.len(), 1);
    assert_eq!(v.wasm_rules[0].name, "fraud_screen");
}

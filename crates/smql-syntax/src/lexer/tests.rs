use super::*;

#[test]
fn test_all_keywords() {
    let input = "define connection pipeline from to where with select when validate on_error before after paginate settings";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    let keywords = vec![
        TokenKind::Define,
        TokenKind::Connection,
        TokenKind::Pipeline,
        TokenKind::From,
        TokenKind::To,
        TokenKind::Where,
        TokenKind::With,
        TokenKind::Select,
        TokenKind::When,
        TokenKind::Validate,
        TokenKind::OnError,
        TokenKind::Before,
        TokenKind::After,
        TokenKind::Paginate,
        TokenKind::Settings,
    ];

    for (i, expected_kind) in keywords.iter().enumerate() {
        assert_eq!(&tokens[i].kind, expected_kind, "Token {} mismatch", i);
    }
}

#[test]
fn test_keyword_word_boundaries() {
    // "defined" should not match "define"
    let input = "defined";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    // Should be parsed as identifier, not Define keyword
    assert!(matches!(tokens[0].kind, TokenKind::Identifier(_)));
}

#[test]
fn test_block_delimiters() {
    let input = "{ } [ ]";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::LeftBrace);
    assert_eq!(tokens[1].kind, TokenKind::RightBrace);
    assert_eq!(tokens[2].kind, TokenKind::LeftBracket);
    assert_eq!(tokens[3].kind, TokenKind::RightBracket);
}

#[test]
fn test_comparison_operators() {
    let input = "= == != > < >= <=";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::Assign);
    assert_eq!(tokens[1].kind, TokenKind::Equal);
    assert_eq!(tokens[2].kind, TokenKind::NotEqual);
    assert_eq!(tokens[3].kind, TokenKind::GreaterThan);
    assert_eq!(tokens[4].kind, TokenKind::LessThan);
    assert_eq!(tokens[5].kind, TokenKind::GreaterOrEqual);
    assert_eq!(tokens[6].kind, TokenKind::LessOrEqual);
}

#[test]
fn test_logical_operators() {
    let input = "&& || !";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::And);
    assert_eq!(tokens[1].kind, TokenKind::Or);
    assert_eq!(tokens[2].kind, TokenKind::Not);
}

#[test]
fn test_string_literals_double_quotes() {
    let input = r#""Hello, World!""#;
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::String(s) => assert_eq!(s, "Hello, World!"),
        _ => panic!("Expected String token"),
    }
}

#[test]
fn test_string_literals_single_quotes() {
    let input = r#"'Hello, World!'"#;
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::String(s) => assert_eq!(s, "Hello, World!"),
        _ => panic!("Expected String token"),
    }
}

#[test]
fn test_number_integer() {
    let input = "42";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match tokens[0].kind {
        TokenKind::Number(n) => assert_eq!(n, 42.0),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_number_float() {
    let input = "3.14159";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match tokens[0].kind {
        TokenKind::Number(n) => assert!((n - 3.14159).abs() < 0.00001),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_number_negative() {
    let input = "-42";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match tokens[0].kind {
        TokenKind::Number(n) => assert_eq!(n, -42.0),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_number_scientific() {
    let input = "1.5e10";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match tokens[0].kind {
        TokenKind::Number(n) => assert_eq!(n, 1.5e10),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_boolean_true() {
    let input = "true";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::Boolean(true));
}

#[test]
fn test_boolean_false() {
    let input = "false";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::Boolean(false));
}

#[test]
fn test_null_literal() {
    let input = "null";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::Null);
}

#[test]
fn test_identifier() {
    let input = "my_variable";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::Identifier(s) => assert_eq!(s, "my_variable"),
        _ => panic!("Expected Identifier token"),
    }
}

#[test]
fn test_identifier_underscore_start() {
    let input = "_private";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::Identifier(s) => assert_eq!(s, "_private"),
        _ => panic!("Expected Identifier token"),
    }
}

#[test]
fn test_identifier_with_numbers() {
    let input = "var123";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::Identifier(s) => assert_eq!(s, "var123"),
        _ => panic!("Expected Identifier token"),
    }
}

#[test]
fn test_dot_notation_simple() {
    let input = "table.column";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::DotNotation(s) => assert_eq!(s, "table.column"),
        _ => panic!("Expected DotNotation token"),
    }
}

#[test]
fn test_dot_notation_nested() {
    let input = "database.schema.table.column";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::DotNotation(s) => assert_eq!(s, "database.schema.table.column"),
        _ => panic!("Expected DotNotation token"),
    }
}

#[test]
fn test_line_comment() {
    let input = r#"
        // This is a comment
        define
    "#;
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    // Comments should be ignored by the lexer
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Define)));
}

#[test]
fn test_block_comment() {
    let input = r#"
        /* This is a 
           multi-line comment */
        define
    "#;
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    // Comments should be ignored
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Define)));
}

#[test]
fn test_function_call_no_args() {
    let input = "now()";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::FunctionCall(name) => assert_eq!(name, "now"),
        _ => panic!("Expected FunctionCall token, got {:?}", tokens[0].kind),
    }
}

#[test]
fn test_function_call_with_args() {
    let input = r#"env("API_KEY")"#;
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    // First token should be function call
    match &tokens[0].kind {
        TokenKind::FunctionCall(name) => assert_eq!(name, "env"),
        _ => panic!("Expected FunctionCall token"),
    }

    // Should have lparen, string arg, rparen
    assert_eq!(tokens[1].kind, TokenKind::LeftParen);
    assert!(matches!(tokens[2].kind, TokenKind::String(_)));
    assert_eq!(tokens[3].kind, TokenKind::RightParen);
}

#[test]
fn test_function_date() {
    let input = r#"date("2024-01-01")"#;
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    match &tokens[0].kind {
        TokenKind::FunctionCall(name) => assert_eq!(name, "date"),
        _ => panic!("Expected FunctionCall token"),
    }
}

#[test]
fn test_array_literal_numbers() {
    let input = "[1, 2, 3]";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::LeftBracket);
    assert!(matches!(tokens[1].kind, TokenKind::Number(1.0)));
    assert_eq!(tokens[2].kind, TokenKind::Comma);
    assert!(matches!(tokens[3].kind, TokenKind::Number(2.0)));
    assert_eq!(tokens[4].kind, TokenKind::Comma);
    assert!(matches!(tokens[5].kind, TokenKind::Number(3.0)));
    assert_eq!(tokens[6].kind, TokenKind::RightBracket);
}

#[test]
fn test_array_literal_strings() {
    let input = r#"["a", "b", "c"]"#;
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::LeftBracket);
    assert!(matches!(tokens[1].kind, TokenKind::String(_)));
    assert_eq!(tokens[2].kind, TokenKind::Comma);
    assert!(matches!(tokens[3].kind, TokenKind::String(_)));
    assert_eq!(tokens[4].kind, TokenKind::Comma);
    assert!(matches!(tokens[5].kind, TokenKind::String(_)));
    assert_eq!(tokens[6].kind, TokenKind::RightBracket);
}

#[test]
fn test_array_empty() {
    let input = "[]";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::LeftBracket);
    assert_eq!(tokens[1].kind, TokenKind::RightBracket);
}

#[test]
fn test_error_reporting_line_column() {
    let input = r#"
        define connection test
        invalid @#$ syntax
    "#;

    let mut lexer = Lexer::new();
    let result = lexer.tokenize(input);

    assert!(result.is_err());

    if let Err(err) = result {
        match err {
            LexerError::ParseError { line, column, .. } => {
                assert!(line > 0);
                assert!(column > 0);
            }
            _ => {}
        }

        let formatted = err.format_error();
        assert!(formatted.contains("line"));
        assert!(formatted.contains("column"));
    }
}

#[test]
fn test_complete_define_connection() {
    let input = r#"
        define connection postgres_db {
            type = "postgresql"
            host = env("DB_HOST")
            port = 5432
            enabled = true
        }
    "#;

    let mut lexer = Lexer::new();
    let result = lexer.tokenize(input);
    assert!(result.is_ok());

    let tokens = result.unwrap();

    // Verify key tokens exist
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Define)));
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t.kind, TokenKind::Connection))
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t.kind, TokenKind::LeftBrace))
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t.kind, TokenKind::RightBrace))
    );
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Assign)));
}

#[test]
fn test_complete_define_pipeline() {
    let input = r#"
        define pipeline user_sync {
            from {
                connection = db
                table = "users"
            }
            
            where {
                age >= 18
                status == "active"
            }
            
            to {
                table = "synced_users"
            }
        }
    "#;

    let mut lexer = Lexer::new();
    let result = lexer.tokenize(input);
    assert!(result.is_ok());

    let tokens = result.unwrap();

    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Define)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Pipeline)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::From)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Where)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::To)));
}

#[test]
fn test_token_position_tracking() {
    let input = "define connection test";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    // Check line numbers
    assert_eq!(tokens[0].line, 1);
    assert_eq!(tokens[1].line, 1);
    assert_eq!(tokens[2].line, 1);

    // Check columns increase
    assert!(tokens[1].column > tokens[0].column);
    assert!(tokens[2].column > tokens[1].column);
}

#[test]
fn test_multiline_position_tracking() {
    let input = "define\nconnection\ntest";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].line, 1);
    assert_eq!(tokens[1].line, 2);
    assert_eq!(tokens[2].line, 3);
}

#[test]
fn test_all_block_keywords_in_context() {
    let input = r#"
        define pipeline test {
            from {}
            to {}
            where {}
            with {}
            select {}
            when {}
            validate {}
            on_error {}
            before {}
            after {}
            paginate {}
            settings {}
        }
    "#;

    let mut lexer = Lexer::new();
    let result = lexer.tokenize(input);
    assert!(result.is_ok());

    let tokens = result.unwrap();

    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::From)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::To)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Where)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::With)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Select)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::When)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Validate)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::OnError)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Before)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::After)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Paginate)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Settings)));
}

#[test]
fn test_complex_expression() {
    let input = r#"age >= 18 && age <= 65 || status == "admin""#;
    let mut lexer = Lexer::new();
    let result = lexer.tokenize(input);
    assert!(result.is_ok());

    let tokens = result.unwrap();

    assert!(
        tokens
            .iter()
            .any(|t| matches!(t.kind, TokenKind::GreaterOrEqual))
    );
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::And)));
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t.kind, TokenKind::LessOrEqual))
    );
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Or)));
    assert!(tokens.iter().any(|t| matches!(t.kind, TokenKind::Equal)));
}

#[test]
fn test_negation_operator() {
    let input = "!deleted";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::Not);
    assert!(matches!(tokens[1].kind, TokenKind::Identifier(_)));
}

#[test]
fn test_parentheses() {
    let input = "(age > 18)";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens[0].kind, TokenKind::LeftParen);
    assert_eq!(tokens[4].kind, TokenKind::RightParen);
}

#[test]
fn test_trailing_comma_in_array() {
    let input = "[1, 2, 3,]";
    let mut lexer = Lexer::new();
    let result = lexer.tokenize(input);
    assert!(result.is_ok());
}

#[test]
fn test_eof_token() {
    let input = "define";
    let mut lexer = Lexer::new();
    let tokens = lexer.tokenize(input).unwrap();

    assert_eq!(tokens.last().unwrap().kind, TokenKind::Eof);
}

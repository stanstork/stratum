use crate::{
    ast::expr::{BinaryOp, BinaryOperator, Expr, FunctionCall, Ident},
    render::{Render, Renderer},
};

impl Render for Expr {
    fn render(&self, r: &mut Renderer) {
        match self {
            Expr::Identifier(ident) => ident.render(r),
            Expr::Value(val) => r.add_param(val.clone()),
            Expr::BinaryOp(op) => op.render(r),
            Expr::FunctionCall(func) => func.render(r),
            Expr::Alias { expr, alias } => {
                expr.render(r);
                r.sql.push_str(" AS ");
                r.sql.push_str(&r.dialect.quote_identifier(alias));
            }
            Expr::Cast { expr, data_type } => {
                r.sql.push_str("CAST(");
                expr.render(r); // Render the inner expression (e.g., the placeholder)
                r.sql.push_str(" AS ");
                r.sql.push_str(data_type);
                r.sql.push(')');
            }
        }
    }
}

impl Render for Ident {
    fn render(&self, r: &mut Renderer) {
        if let Some(qualifier) = &self.qualifier {
            r.sql.push_str(&r.dialect.quote_identifier(qualifier));
            r.sql.push('.');
        }
        r.sql.push_str(&r.dialect.quote_identifier(&self.name));
    }
}

impl Render for BinaryOp {
    fn render(&self, r: &mut Renderer) {
        r.sql.push('(');
        self.left.render(r);

        let op_str = match self.op {
            BinaryOperator::Eq => " = ",
            BinaryOperator::NotEq => " <> ",
            BinaryOperator::Lt => " < ",
            BinaryOperator::LtEq => " <= ",
            BinaryOperator::Gt => " > ",
            BinaryOperator::GtEq => " >= ",
            BinaryOperator::And => " AND ",
            BinaryOperator::Or => " OR ",
        };
        r.sql.push_str(op_str);

        self.right.render(r);
        r.sql.push(')');
    }
}

impl Render for FunctionCall {
    fn render(&self, r: &mut Renderer) {
        r.sql.push_str(&self.name);
        r.sql.push('(');
        if self.wildcard {
            r.sql.push('*');
        } else {
            for (i, arg) in self.args.iter().enumerate() {
                if i > 0 {
                    r.sql.push_str(", ");
                }
                arg.render(r);
            }
        }
        r.sql.push(')');
    }
}

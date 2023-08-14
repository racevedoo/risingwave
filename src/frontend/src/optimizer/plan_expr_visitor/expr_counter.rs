// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use crate::expr::{ExprImpl, ExprType, ExprVisitor, FunctionCall};

/// `ExprCounter` is used by `CseRewriter`.
#[derive(Default)]
pub struct CseExprCounter {
    // Only count pure function call and not const.
    pub counter: HashMap<FunctionCall, usize>,
}

impl ExprVisitor<()> for CseExprCounter {
    fn merge(_: (), _: ()) {}

    fn visit_expr(&mut self, expr: &ExprImpl) {
        // Considering this sql, `In` expression needs to ensure its in-clauses to be const.
        // If we extract it into a common sub-expression (finally be a `InputRef`) which will
        // violate this assumption, so ban this case. SELECT x,
        //        tand(x) IN ('-Infinity'::float8,-1,0,1,'Infinity'::float8) AS tand_exact,
        //        cotd(x) IN ('-Infinity'::float8,-1,0,1,'Infinity'::float8) AS cotd_exact
        // FROM (VALUES (0), (45), (90), (135), (180),(225), (270), (315), (360)) AS t(x);
        if expr.is_const() {
            return;
        }

        match expr {
            ExprImpl::InputRef(inner) => self.visit_input_ref(inner),
            ExprImpl::Literal(inner) => self.visit_literal(inner),
            ExprImpl::FunctionCall(inner) => self.visit_function_call(inner),
            ExprImpl::AggCall(inner) => self.visit_agg_call(inner),
            ExprImpl::Subquery(inner) => self.visit_subquery(inner),
            ExprImpl::CorrelatedInputRef(inner) => self.visit_correlated_input_ref(inner),
            ExprImpl::TableFunction(inner) => self.visit_table_function(inner),
            ExprImpl::WindowFunction(inner) => self.visit_window_function(inner),
            ExprImpl::UserDefinedFunction(inner) => self.visit_user_defined_function(inner),
            ExprImpl::Parameter(inner) => self.visit_parameter(inner),
            ExprImpl::Now(inner) => self.visit_now(inner),
        }
    }

    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        // Short cut semantic func type cannot be extracted into common sub-expression.
        // E.g. select true or (1 / a)::bool or (1 / a)::bool from x
        // If a is zero, common sub-expression (1 / a) would lead to division by zero error.
        match func_call.func_type() {
            ExprType::Case | ExprType::And | ExprType::Or => {
                return;
            }
            _ => {}
        };

        if func_call.is_pure() {
            self.counter
                .entry(func_call.clone())
                .and_modify(|counter| *counter += 1)
                .or_insert(1);
        }

        func_call
            .inputs()
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default()
    }
}

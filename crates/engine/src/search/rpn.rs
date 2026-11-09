use super::SearchToken;

pub fn to_rpn(tokens: &[SearchToken]) -> Vec<SearchToken> {
    // Shunting-yard without parentheses. Precedence: NOT(3, right), AND(2, left), OR(1, left)
    fn precedence(tok: &SearchToken) -> (u8, bool) {
        match tok {
            SearchToken::Not => (3, true),
            SearchToken::And => (2, false),
            SearchToken::Or => (1, false),
            SearchToken::Term { .. } | SearchToken::QuotedTerm { .. } => (0, false),
        }
    }

    let mut output: Vec<SearchToken> = Vec::new();
    let mut ops: Vec<SearchToken> = Vec::new();

    for tok in tokens {
        match tok {
            SearchToken::Term { .. } | SearchToken::QuotedTerm { .. } => output.push(tok.clone()),
            SearchToken::And | SearchToken::Or | SearchToken::Not => {
                let (p_cur, right_assoc) = precedence(tok);
                while let Some(top) = ops.last() {
                    let (p_top, _) = precedence(top);
                    let should_pop = if right_assoc {
                        p_cur < p_top
                    } else {
                        p_cur <= p_top
                    };
                    if should_pop {
                        output.push(ops.pop().unwrap());
                    } else {
                        break;
                    }
                }
                ops.push(tok.clone());
            }
        }
    }
    while let Some(op) = ops.pop() {
        output.push(op);
    }
    output
}

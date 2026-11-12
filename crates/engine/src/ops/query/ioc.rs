/// Appends matched `[tag]` tokens that are not already present in the memo.
pub fn append_ioc_tags(memo: &mut String, tags: &[String]) {
    for tag in tags {
        if !memo.contains(tag) {
            if !memo.is_empty() && !memo.ends_with(' ') {
                memo.push(' ');
            }
            memo.push_str(tag);
        }
    }
}

/// Numeric buckets use their advertised edges for both membership and labels.
/// Reversing the edge calculation with division can round an exact boundary
/// into the preceding bucket (for example 0.58 in 50 bins over 0 .. 1).
pub(super) struct FloatBins {
    edges: Vec<f64>,
    constant: bool,
}

impl FloatBins {
    pub(super) fn new(min: f64, max: f64, buckets: usize) -> Result<Self, String> {
        if min > max {
            return Err("Invalid range: min is greater than max.".into());
        }
        let span = max - min;
        if !span.is_finite() {
            return Err("Invalid range: span is too wide.".into());
        }
        let edges = (0..=buckets)
            .map(|index| {
                if index == buckets {
                    max
                } else {
                    min + span * (index as f64 / buckets as f64)
                }
            })
            .collect();
        Ok(Self {
            edges,
            constant: min == max,
        })
    }

    pub(super) fn edge(&self, index: usize) -> f64 {
        self.edges[index]
    }

    pub(super) fn end_inclusive(&self, index: usize) -> bool {
        if self.constant {
            index == 0
        } else {
            index + 2 == self.edges.len()
        }
    }

    pub(super) fn index(&self, value: f64) -> Option<usize> {
        if !value.is_finite() || value < self.edges[0] || value > *self.edges.last()? {
            return None;
        }
        if self.constant {
            return Some(0);
        }
        // Choose the final edge at or below the point. Duplicate rounded
        // edges represent empty bins; the final maximum belongs to the last.
        Some(
            self.edges
                .partition_point(|edge| *edge <= value)
                .saturating_sub(1)
                .min(self.edges.len() - 2),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::FloatBins;

    #[test]
    fn rounded_edges_and_constant_ranges_keep_every_point_in_its_labeled_bin() {
        for (min, max) in [
            (0.0, 1.0),
            (-1.0, 1.0),
            (0.1, 0.1),
            (1.0, 1.0_f64.next_up()),
        ] {
            for count in [1, 3, 50, 200] {
                let bins = FloatBins::new(min, max, count).unwrap();
                for index in 0..=count {
                    let point = bins.edge(index);
                    let bucket = bins.index(point).unwrap();
                    for candidate in 0..count {
                        let inside = point >= bins.edge(candidate)
                            && (point < bins.edge(candidate + 1)
                                || bins.end_inclusive(candidate)
                                    && point == bins.edge(candidate + 1));
                        assert_eq!(inside, bucket == candidate);
                    }
                }
                assert_eq!(bins.index(min.next_down()), None);
                assert_eq!(bins.index(max.next_up()), None);
                assert_eq!(bins.index(f64::NAN), None);
            }
        }
    }
}

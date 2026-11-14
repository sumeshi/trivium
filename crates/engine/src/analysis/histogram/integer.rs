/// Equal-width integer bins. Unsigned distances cover the entire i128 domain;
/// labels and membership use the same rounded-up boundaries.
pub(super) struct IntegerBins {
    min: i128,
    max: i128,
    span: u128,
    buckets: usize,
}

impl IntegerBins {
    pub(super) fn new(min: i128, max: i128, buckets: usize) -> Self {
        debug_assert!(min <= max && buckets > 0);
        Self {
            min,
            max,
            span: max.abs_diff(min),
            buckets,
        }
    }

    fn offset(&self, index: usize) -> u128 {
        let buckets = self.buckets as u128;
        let index = index as u128;
        // Divide before multiplying. Both products and their sum fit even
        // when span is u128::MAX (i128::MIN .. i128::MAX).
        self.span / buckets * index + (self.span % buckets * index).div_ceil(buckets)
    }

    pub(super) fn edge(&self, index: usize) -> i128 {
        debug_assert!(index <= self.buckets);
        self.min
            .checked_add_unsigned(self.offset(index))
            .expect("integer bin edge lies between min and max")
    }

    pub(super) fn end_inclusive(&self, index: usize) -> bool {
        if self.span == 0 {
            index == 0
        } else {
            index + 1 == self.buckets
        }
    }

    pub(super) fn index(&self, value: i128) -> Option<usize> {
        if value < self.min || value > self.max {
            return None;
        }
        if self.span == 0 {
            return Some(0);
        }
        let offset = value.abs_diff(self.min);
        let index = if let Some(scaled) = offset.checked_mul(self.buckets as u128) {
            (scaled / self.span) as usize
        } else {
            // Very wide integer ranges need no wider integer or float:
            // locate the last edge at/below the value instead.
            let mut low = 0;
            let mut high = self.buckets;
            while low < high {
                let middle = low + (high - low).div_ceil(2);
                if self.offset(middle) <= offset {
                    low = middle;
                } else {
                    high = middle - 1;
                }
            }
            low
        };
        Some(index.min(self.buckets - 1))
    }
}

#[cfg(test)]
mod tests {
    use super::IntegerBins;

    #[test]
    fn every_integer_lands_in_its_advertised_bounds() {
        for min in -8..=8 {
            for max in min..=8 {
                for count in 1..=20 {
                    let bins = IntegerBins::new(min, max, count);
                    for value in min..=max {
                        let index = bins.index(value).unwrap();
                        for bucket in 0..count {
                            let inside = value >= bins.edge(bucket)
                                && (value < bins.edge(bucket + 1)
                                    || bins.end_inclusive(bucket)
                                        && value == bins.edge(bucket + 1));
                            assert_eq!(inside, bucket == index);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn full_i128_span_keeps_exact_edges_and_indices() {
        let bins = IntegerBins::new(i128::MIN, i128::MAX, 2);
        assert_eq!(
            (bins.edge(0), bins.edge(1), bins.edge(2)),
            (i128::MIN, 0, i128::MAX)
        );
        for value in [i128::MIN, i128::MIN + 1, -1] {
            assert_eq!(bins.index(value), Some(0));
        }
        for value in [0, 1, i128::MAX - 1, i128::MAX] {
            assert_eq!(bins.index(value), Some(1));
        }
        let bins = IntegerBins::new(i128::MIN, i128::MAX, 200);
        assert_eq!(bins.edge(0), i128::MIN);
        assert_eq!(bins.edge(200), i128::MAX);
        for index in 0..200 {
            assert!(bins.edge(index) < bins.edge(index + 1));
            assert_eq!(bins.index(bins.edge(index)), Some(index));
        }
    }
}

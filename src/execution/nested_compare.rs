use crate::types::{Scalar, canonical_float_bits};
use std::cmp::Ordering;

/// Containers compare by field/element position, never by their JSON spelling.
/// Outer SQL nulls are handled by the caller; inner nulls are structural values.
pub(super) fn compare(left: &Scalar, right: &Scalar) -> Option<Ordering> {
    match (left, right) {
        (Scalar::Null, Scalar::Null) => Some(Ordering::Equal),
        (Scalar::Null, _) => Some(Ordering::Less),
        (_, Scalar::Null) => Some(Ordering::Greater),
        (Scalar::Float(left), Scalar::Float(right)) => Some(
            f64::from_bits(canonical_float_bits(*left))
                .total_cmp(&f64::from_bits(canonical_float_bits(*right))),
        ),
        (Scalar::Struct(left), Scalar::Struct(right)) => sequence(
            left.iter().map(|(_, value)| value),
            right.iter().map(|(_, value)| value),
        ),
        (Scalar::List(left), Scalar::List(right)) => sequence(left.iter(), right.iter()),
        _ => super::scalar::cmp(left, right),
    }
}

fn sequence<'a>(
    mut left: impl Iterator<Item = &'a Scalar>,
    mut right: impl Iterator<Item = &'a Scalar>,
) -> Option<Ordering> {
    loop {
        match (left.next(), right.next()) {
            (Some(left), Some(right)) => {
                let ordering = compare(left, right)?;
                if ordering != Ordering::Equal {
                    return Some(ordering);
                }
            }
            (None, None) => return Some(Ordering::Equal),
            (None, Some(_)) => return Some(Ordering::Less),
            (Some(_), None) => return Some(Ordering::Greater),
        }
    }
}

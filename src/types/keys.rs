use super::{Scalar, ScalarKey};

pub(crate) fn canonical_float_bits(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

impl ScalarKey {
    pub(crate) fn from_scalar(value: &Scalar) -> Self {
        match value {
            Scalar::Null => Self::Null,
            Scalar::Int(value) => Self::Int(*value),
            Scalar::Decimal(value) => Self::Decimal(*value),
            Scalar::Float(value) => Self::Float(canonical_float_bits(*value)),
            Scalar::Bool(value) => Self::Bool(*value),
            Scalar::Str(value) => Self::Str(value.clone()),
            Scalar::Struct(fields) => Self::Struct(
                fields
                    .iter()
                    .map(|(_, value)| Self::from_scalar(value))
                    .collect(),
            ),
            Scalar::List(values) => Self::List(values.iter().map(Self::from_scalar).collect()),
        }
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::Str(text) => text.capacity(),
            Self::Struct(values) | Self::List(values) => {
                values.capacity() * std::mem::size_of::<Self>()
                    + values.iter().map(Self::heap_bytes).sum::<usize>()
            }
            _ => 0,
        }
    }
}

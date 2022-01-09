use crate::broker::log::segment::Segment;

pub struct _Reader {
    segment: Box<Segment>,
    segments: Vec<Segment>,
    index: usize,
    offset: u64,
}

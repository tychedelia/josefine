use crate::broker::log::segment::Segment;

pub struct Reader {
    segment: Box<Segment>,
    segments: Vec<Box<Segment>>,
    index: usize,
    offset: u64,
}
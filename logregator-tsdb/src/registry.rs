pub type Label = (String, String);
pub type LabelSet = HashSet<Label>;
pub struct Stream {
    id: usize,
    label_set: LabelSet
}

pub type StreamRegistry = HashMap<usize /* stream_id */, LabelSet>;
pub type LabelRegistry = HashMap<Label, Vec<usize /* stream_id */>>;
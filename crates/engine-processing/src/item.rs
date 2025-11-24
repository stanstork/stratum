#[derive(Clone)]
pub struct ItemId {
    run_id: String,
    item_id: String,
    part_id: String,
}

impl ItemId {
    pub fn new(run_id: String, item_id: String, part_id: String) -> Self {
        Self {
            run_id,
            item_id,
            part_id,
        }
    }

    pub fn run_id(&self) -> String {
        self.run_id.clone()
    }

    pub fn item_id(&self) -> String {
        self.item_id.clone()
    }

    pub fn part_id(&self) -> String {
        self.part_id.clone()
    }
}

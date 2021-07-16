use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Topic {
    pub id: Uuid,
    pub name: String,
}
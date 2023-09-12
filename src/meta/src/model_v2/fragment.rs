//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.2

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "fragment")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub fragment_id: i32,
    pub table_id: i32,
    pub fragment_type_mask: i32,
    pub distribution_type: String,
    pub stream_node: Json,
    pub vnode_mapping: Option<Json>,
    pub state_table_ids: Option<Vec<i32>>,
    pub upstream_fragment_id: Option<Vec<i32>>,
    pub dispatcher_type: Option<String>,
    pub dist_key_indices: Option<Vec<i32>>,
    pub output_indices: Option<Vec<i32>>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::actor::Entity")]
    Actor,
    #[sea_orm(
        belongs_to = "super::table::Entity",
        from = "Column::TableId",
        to = "super::table::Column::TableId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Table,
}

impl Related<super::actor::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Actor.def()
    }
}

impl Related<super::table::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Table.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

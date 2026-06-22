use bevy::prelude::*;

pub struct QuadTree {
    bounds: Rect,
    depth: u8,
    max_depth: u8,
    children: Option<Box<[QuadTree; 4]>>,
    shard_id: Option<u32>,  // défini uniquement sur les feuilles
}

impl QuadTree {
    /// Retourne le shard_id de la feuille contenant `pos`.
    pub fn shard_for(&self, pos: Vec2) -> Option<u32> {

    }

    /// Retourne les shard_ids distincts dans un rayon `margin` autour de `pos`.
    /// Utilisé pour détecter l'approche d'une frontière inter-shard.
    pub fn shards_near(&self, pos: Vec2, margin: f32) -> Vec<u32> {

    }
}
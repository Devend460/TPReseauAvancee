// spatial_server/src/spatial.rs
use bevy::prelude::*;

pub struct QuadTree {
    pub bounds: Rect,
    pub depth: u8,
    pub max_depth: u8,
    pub children: Option<Box<[QuadTree; 4]>>,
    pub shard_id: Option<u32>,
}

impl QuadTree {
    pub fn new(bounds: Rect, depth: u8, max_depth: u8, shard_id: Option<u32>) -> Self {
        Self {
            bounds,
            depth,
            max_depth,
            children: None,
            shard_id,
        }
    }

    /**
    Retourne le shard_id de la feuille contenant `pos`
    */
    pub fn shard_for(&self, pos: Vec2) -> Option<u32> {
        if !self.bounds.contains(pos) {
            return None;
        }
        if let Some(ref sub_trees) = self.children {
            for child in sub_trees.iter() {
                if child.bounds.contains(pos) {
                    return child.shard_for(pos);
                }
            }
        }
        self.shard_id // Si c'est une feuille, retourne son shard attribué
    }

    /**
    Retourne les shard_ids distincts dans un rayon `margin` autour de `pos`
    Utilisé pour détecter l'approche d'une frontière inter-shard (Partie 3)
    */
    pub fn shards_near(&self, pos: Vec2, margin: f32) -> Vec<u32> {
        let mut found_shards = Vec::new();
        self.collect_shards_near(pos, margin, &mut found_shards);
        found_shards.sort();
        found_shards.dedup();
        found_shards
    }

    fn collect_shards_near(&self, pos: Vec2, margin: f32, acc: &mut Vec<u32>) {
        // Crée une boîte englobante (AoI) autour du joueur pour le recoupement des frontières
        let player_rect = Rect::from_center_size(pos, Vec2::splat(margin * 2.0));

        // Si la boîte de marge n'intersecte pas ce quadrant, on s'arrête
        if self.bounds.intersect(player_rect).is_empty() {
            return;
        }

        if let Some(ref sub_trees) = self.children {
            for child in sub_trees.iter() {
                child.collect_shards_near(pos, margin, acc);
            }
        } else if let Some(id) = self.shard_id {
            acc.push(id);
        }
    }

    pub fn subdivide_statically(&mut self, shard_assignments: [u32; 4]) {
        if self.depth >= self.max_depth { return; }

        let center = self.bounds.center();
        let min = self.bounds.min;
        let max = self.bounds.max;

        let r_nw = Rect::new(min.x, center.y, center.x, max.y);
        let r_ne = Rect::new(center.x, center.y, max.x, max.y);
        let r_sw = Rect::new(min.x, min.y, center.x, center.y);
        let r_se = Rect::new(center.x, min.y, max.x, center.y);

        let next_d = self.depth + 1;
        self.children = Some(Box::new([
            QuadTree::new(r_nw, next_d, self.max_depth, Some(shard_assignments[0])),
            QuadTree::new(r_ne, next_d, self.max_depth, Some(shard_assignments[1])),
            QuadTree::new(r_sw, next_d, self.max_depth, Some(shard_assignments[2])),
            QuadTree::new(r_se, next_d, self.max_depth, Some(shard_assignments[3])),
        ]));
        self.shard_id = None;
    }
}
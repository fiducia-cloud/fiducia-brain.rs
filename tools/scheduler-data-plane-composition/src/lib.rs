//! Executable safety model for composing `fiducia-brain` placement publication with
//! `fiducia-node` staging, activation, restart, and write-authority fencing.
//!
//! Raft commit is an explicit input to this model. The crate checks what must hold after a
//! generation is published or committed; it does not claim to re-prove the Raft log itself.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

pub type Term = u64;
pub type Generation = u64;
pub type ShardId = u16;
pub type NodeId = u16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrainRole {
    Leader,
    Follower,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementSnapshot {
    pub term: Term,
    pub generation: Generation,
    assignments: BTreeMap<ShardId, BTreeSet<NodeId>>,
}

impl PlacementSnapshot {
    pub fn new(
        term: Term,
        generation: Generation,
        assignments: BTreeMap<ShardId, Vec<NodeId>>,
    ) -> Result<Self, ModelError> {
        if term == 0 {
            return Err(ModelError::InvalidTerm);
        }
        if generation == 0 {
            return Err(ModelError::InvalidGeneration);
        }
        if assignments.is_empty() {
            return Err(ModelError::EmptyPlacement);
        }

        let mut canonical = BTreeMap::new();
        for (shard, replicas) in assignments {
            if replicas.is_empty() {
                return Err(ModelError::EmptyReplicaSet(shard));
            }
            let mut unique = BTreeSet::new();
            for node in replicas {
                if !unique.insert(node) {
                    return Err(ModelError::DuplicateReplica { shard, node });
                }
            }
            canonical.insert(shard, unique);
        }

        Ok(Self {
            term,
            generation,
            assignments: canonical,
        })
    }

    #[must_use]
    pub fn replicas(&self, shard: ShardId) -> Option<&BTreeSet<NodeId>> {
        self.assignments.get(&shard)
    }

    #[must_use]
    pub fn shards_for(&self, node: NodeId) -> BTreeSet<ShardId> {
        self.assignments
            .iter()
            .filter_map(|(shard, replicas)| replicas.contains(&node).then_some(*shard))
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteFence {
    pub term: Term,
    pub generation: Generation,
}

/// Scheduler-side publication and commit boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrainPublisher {
    role: BrainRole,
    term: Term,
    last_published_generation: Generation,
    committed_generation: Generation,
    published: BTreeMap<Generation, PlacementSnapshot>,
}

impl BrainPublisher {
    pub fn new(
        role: BrainRole,
        term: Term,
        committed_generation: Generation,
    ) -> Result<Self, ModelError> {
        if term == 0 {
            return Err(ModelError::InvalidTerm);
        }
        Ok(Self {
            role,
            term,
            last_published_generation: committed_generation,
            committed_generation,
            published: BTreeMap::new(),
        })
    }

    #[must_use]
    pub const fn role(&self) -> BrainRole {
        self.role
    }

    #[must_use]
    pub const fn term(&self) -> Term {
        self.term
    }

    #[must_use]
    pub const fn committed_generation(&self) -> Generation {
        self.committed_generation
    }

    pub fn become_leader(&mut self, new_term: Term) -> Result<(), ModelError> {
        if new_term <= self.term {
            return Err(ModelError::StaleTerm {
                observed: self.term,
                provided: new_term,
            });
        }
        self.term = new_term;
        self.role = BrainRole::Leader;
        self.published.clear();
        self.last_published_generation = self.committed_generation;
        Ok(())
    }

    pub fn step_down(&mut self, observed_term: Term) -> Result<(), ModelError> {
        if observed_term < self.term {
            return Err(ModelError::StaleTerm {
                observed: self.term,
                provided: observed_term,
            });
        }
        self.term = observed_term;
        self.role = BrainRole::Follower;
        self.published.clear();
        self.last_published_generation = self.committed_generation;
        Ok(())
    }

    pub fn publish_next(
        &mut self,
        assignments: BTreeMap<ShardId, Vec<NodeId>>,
    ) -> Result<PlacementSnapshot, ModelError> {
        self.require_leader()?;
        let generation = self
            .last_published_generation
            .checked_add(1)
            .ok_or(ModelError::GenerationOverflow)?;
        let snapshot = PlacementSnapshot::new(self.term, generation, assignments)?;
        self.last_published_generation = generation;
        self.published.insert(generation, snapshot.clone());
        Ok(snapshot)
    }

    /// Records the result of the separately verified Raft commit boundary.
    pub fn commit(&mut self, snapshot: &PlacementSnapshot) -> Result<(), ModelError> {
        self.require_leader()?;
        if snapshot.term != self.term {
            return Err(ModelError::StaleTerm {
                observed: self.term,
                provided: snapshot.term,
            });
        }
        if snapshot.generation == self.committed_generation {
            return Ok(());
        }
        let expected = self
            .committed_generation
            .checked_add(1)
            .ok_or(ModelError::GenerationOverflow)?;
        if snapshot.generation != expected {
            return Err(ModelError::NonSequentialCommit {
                expected,
                provided: snapshot.generation,
            });
        }
        match self.published.get(&snapshot.generation) {
            Some(published) if published == snapshot => {}
            Some(_) => {
                return Err(ModelError::ConflictingGeneration(snapshot.generation));
            }
            None => {
                return Err(ModelError::UnknownPublishedGeneration(snapshot.generation));
            }
        }
        self.committed_generation = snapshot.generation;
        Ok(())
    }

    pub fn write_fence(&self, generation: Generation) -> Result<WriteFence, ModelError> {
        self.require_leader()?;
        if generation != self.committed_generation || generation == 0 {
            return Err(ModelError::UncommittedGeneration {
                committed: self.committed_generation,
                requested: generation,
            });
        }
        Ok(WriteFence {
            term: self.term,
            generation,
        })
    }

    fn require_leader(&self) -> Result<(), ModelError> {
        if self.role == BrainRole::Leader {
            Ok(())
        } else {
            Err(ModelError::NotLeader)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ActivePlacement {
    generation: Generation,
    assigned_shards: BTreeSet<ShardId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StagedPlacement {
    snapshot: PlacementSnapshot,
    assigned_shards: BTreeSet<ShardId>,
    acknowledged_shards: BTreeSet<ShardId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageOutcome {
    Staged,
    DuplicateStaged,
    DuplicateActive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckOutcome {
    Recorded,
    Replay,
}

/// Node-side durable activation model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeReplica {
    node_id: NodeId,
    observed_term: Term,
    active: Option<ActivePlacement>,
    staged: Option<StagedPlacement>,
    write_fence: Option<WriteFence>,
}

impl NodeReplica {
    #[must_use]
    pub const fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            observed_term: 0,
            active: None,
            staged: None,
            write_fence: None,
        }
    }

    #[must_use]
    pub const fn observed_term(&self) -> Term {
        self.observed_term
    }

    #[must_use]
    pub fn active_generation(&self) -> Option<Generation> {
        self.active.as_ref().map(|placement| placement.generation)
    }

    #[must_use]
    pub fn active_shards(&self) -> BTreeSet<ShardId> {
        self.active
            .as_ref()
            .map_or_else(BTreeSet::new, |placement| placement.assigned_shards.clone())
    }

    #[must_use]
    pub fn staged_generation(&self) -> Option<Generation> {
        self.staged
            .as_ref()
            .map(|placement| placement.snapshot.generation)
    }

    #[must_use]
    pub fn acknowledged_shards(&self) -> BTreeSet<ShardId> {
        self.staged
            .as_ref()
            .map_or_else(BTreeSet::new, |placement| {
                placement.acknowledged_shards.clone()
            })
    }

    #[must_use]
    pub const fn current_write_fence(&self) -> Option<WriteFence> {
        self.write_fence
    }

    pub fn observe_term(&mut self, term: Term) -> Result<(), ModelError> {
        if term == 0 {
            return Err(ModelError::InvalidTerm);
        }
        if term < self.observed_term {
            return Err(ModelError::StaleTerm {
                observed: self.observed_term,
                provided: term,
            });
        }
        if term > self.observed_term {
            self.observed_term = term;
            self.write_fence = None;
            if self
                .staged
                .as_ref()
                .is_some_and(|placement| placement.snapshot.term < term)
            {
                self.staged = None;
            }
        }
        Ok(())
    }

    pub fn stage(&mut self, snapshot: PlacementSnapshot) -> Result<StageOutcome, ModelError> {
        if snapshot.term < self.observed_term {
            return Err(ModelError::StaleTerm {
                observed: self.observed_term,
                provided: snapshot.term,
            });
        }
        self.observe_term(snapshot.term)?;
        let assigned_shards = snapshot.shards_for(self.node_id);

        if let Some(active) = self.active.as_ref() {
            if snapshot.generation < active.generation {
                return Err(ModelError::StaleGeneration {
                    active: active.generation,
                    provided: snapshot.generation,
                });
            }
            if snapshot.generation == active.generation {
                if assigned_shards == active.assigned_shards {
                    return Ok(StageOutcome::DuplicateActive);
                }
                return Err(ModelError::ConflictingGeneration(snapshot.generation));
            }
        }

        if let Some(staged) = self.staged.as_ref() {
            if snapshot.generation < staged.snapshot.generation {
                return Err(ModelError::StaleGeneration {
                    active: staged.snapshot.generation,
                    provided: snapshot.generation,
                });
            }
            if snapshot.generation == staged.snapshot.generation {
                if snapshot == staged.snapshot {
                    return Ok(StageOutcome::DuplicateStaged);
                }
                return Err(ModelError::ConflictingGeneration(snapshot.generation));
            }
        }

        self.write_fence = None;
        self.staged = Some(StagedPlacement {
            snapshot,
            assigned_shards,
            acknowledged_shards: BTreeSet::new(),
        });
        Ok(StageOutcome::Staged)
    }

    pub fn acknowledge_shard(
        &mut self,
        generation: Generation,
        shard: ShardId,
    ) -> Result<AckOutcome, ModelError> {
        let staged = self.staged.as_mut().ok_or(ModelError::NoStagedPlacement)?;
        if generation != staged.snapshot.generation {
            return Err(ModelError::AckGenerationMismatch {
                staged: staged.snapshot.generation,
                provided: generation,
            });
        }
        if !staged.assigned_shards.contains(&shard) {
            return Err(ModelError::ShardNotAssigned(shard));
        }
        if staged.acknowledged_shards.insert(shard) {
            Ok(AckOutcome::Recorded)
        } else {
            Ok(AckOutcome::Replay)
        }
    }

    pub fn activate(&mut self, generation: Generation) -> Result<(), ModelError> {
        let staged = self.staged.as_ref().ok_or(ModelError::NoStagedPlacement)?;
        if generation != staged.snapshot.generation {
            return Err(ModelError::AckGenerationMismatch {
                staged: staged.snapshot.generation,
                provided: generation,
            });
        }
        if staged.snapshot.term != self.observed_term {
            return Err(ModelError::StaleTerm {
                observed: self.observed_term,
                provided: staged.snapshot.term,
            });
        }
        let missing: Vec<_> = staged
            .assigned_shards
            .difference(&staged.acknowledged_shards)
            .copied()
            .collect();
        if !missing.is_empty() {
            return Err(ModelError::PartialActivation(missing));
        }

        let active = ActivePlacement {
            generation,
            assigned_shards: staged.assigned_shards.clone(),
        };
        self.active = Some(active);
        self.staged = None;
        self.write_fence = None;
        Ok(())
    }

    pub fn install_write_fence(&mut self, fence: WriteFence) -> Result<(), ModelError> {
        if fence.term != self.observed_term {
            return Err(ModelError::StaleTerm {
                observed: self.observed_term,
                provided: fence.term,
            });
        }
        let active = self.active.as_ref().ok_or(ModelError::NoActivePlacement)?;
        if active.generation != fence.generation {
            return Err(ModelError::AuthorityMismatch {
                active: active.generation,
                requested: fence.generation,
            });
        }
        self.write_fence = Some(fence);
        Ok(())
    }

    #[must_use]
    pub fn accepts_write(&self, fence: WriteFence, shard: ShardId) -> bool {
        self.write_fence == Some(fence)
            && self.active.as_ref().is_some_and(|active| {
                active.generation == fence.generation && active.assigned_shards.contains(&shard)
            })
    }

    /// Simulates a process restart with durable active/staged generation metadata.
    ///
    /// Data-copy acknowledgements and write authority are intentionally volatile. Losing either
    /// can reduce availability, but cannot activate a partial generation or preserve stale writes.
    pub fn restart(&mut self) {
        self.write_fence = None;
        if let Some(staged) = self.staged.as_mut() {
            staged.acknowledged_shards.clear();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelError {
    InvalidTerm,
    InvalidGeneration,
    NotLeader,
    EmptyPlacement,
    EmptyReplicaSet(ShardId),
    DuplicateReplica {
        shard: ShardId,
        node: NodeId,
    },
    GenerationOverflow,
    UnknownPublishedGeneration(Generation),
    ConflictingGeneration(Generation),
    NonSequentialCommit {
        expected: Generation,
        provided: Generation,
    },
    UncommittedGeneration {
        committed: Generation,
        requested: Generation,
    },
    StaleTerm {
        observed: Term,
        provided: Term,
    },
    StaleGeneration {
        active: Generation,
        provided: Generation,
    },
    NoStagedPlacement,
    AckGenerationMismatch {
        staged: Generation,
        provided: Generation,
    },
    ShardNotAssigned(ShardId),
    PartialActivation(Vec<ShardId>),
    NoActivePlacement,
    AuthorityMismatch {
        active: Generation,
        requested: Generation,
    },
}

impl Display for ModelError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTerm => write!(formatter, "term must be non-zero"),
            Self::InvalidGeneration => write!(formatter, "generation must be non-zero"),
            Self::NotLeader => write!(formatter, "only the leader may publish or authorize"),
            Self::EmptyPlacement => write!(formatter, "placement must contain at least one shard"),
            Self::EmptyReplicaSet(shard) => {
                write!(formatter, "shard {shard} has no replicas")
            }
            Self::DuplicateReplica { shard, node } => {
                write!(formatter, "shard {shard} repeats node {node}")
            }
            Self::GenerationOverflow => write!(formatter, "placement generation overflow"),
            Self::UnknownPublishedGeneration(generation) => {
                write!(
                    formatter,
                    "generation {generation} was not published in this term"
                )
            }
            Self::ConflictingGeneration(generation) => {
                write!(
                    formatter,
                    "generation {generation} has conflicting contents"
                )
            }
            Self::NonSequentialCommit { expected, provided } => write!(
                formatter,
                "commit must be sequential: expected {expected}, provided {provided}"
            ),
            Self::UncommittedGeneration {
                committed,
                requested,
            } => write!(
                formatter,
                "write authority requires committed generation {committed}, requested {requested}"
            ),
            Self::StaleTerm { observed, provided } => write!(
                formatter,
                "stale term: observed {observed}, provided {provided}"
            ),
            Self::StaleGeneration { active, provided } => write!(
                formatter,
                "stale generation: active/staged {active}, provided {provided}"
            ),
            Self::NoStagedPlacement => write!(formatter, "no placement is staged"),
            Self::AckGenerationMismatch { staged, provided } => write!(
                formatter,
                "ack generation mismatch: staged {staged}, provided {provided}"
            ),
            Self::ShardNotAssigned(shard) => {
                write!(formatter, "shard {shard} is not assigned to this node")
            }
            Self::PartialActivation(missing) => write!(
                formatter,
                "cannot activate before {} shard acknowledgement(s)",
                missing.len()
            ),
            Self::NoActivePlacement => write!(formatter, "no placement is active"),
            Self::AuthorityMismatch { active, requested } => write!(
                formatter,
                "write authority generation mismatch: active {active}, requested {requested}"
            ),
        }
    }
}

impl Error for ModelError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignments(entries: &[(ShardId, &[NodeId])]) -> BTreeMap<ShardId, Vec<NodeId>> {
        entries
            .iter()
            .map(|(shard, nodes)| (*shard, nodes.to_vec()))
            .collect()
    }

    fn activate_all(node: &mut NodeReplica, snapshot: PlacementSnapshot) {
        let generation = snapshot.generation;
        let assigned = snapshot.shards_for(node.node_id);
        node.stage(snapshot).unwrap();
        for shard in assigned {
            node.acknowledge_shard(generation, shard).unwrap();
        }
        node.activate(generation).unwrap();
    }

    #[test]
    fn follower_cannot_publish_or_issue_write_authority() {
        let mut brain = BrainPublisher::new(BrainRole::Follower, 1, 3).unwrap();
        assert_eq!(
            brain.publish_next(assignments(&[(1, &[1, 2, 3])])),
            Err(ModelError::NotLeader)
        );
        assert_eq!(brain.write_fence(3), Err(ModelError::NotLeader));
    }

    #[test]
    fn publication_and_commit_generations_are_strictly_sequential() {
        let mut brain = BrainPublisher::new(BrainRole::Leader, 1, 0).unwrap();
        let first = brain.publish_next(assignments(&[(1, &[1, 2, 3])])).unwrap();
        let second = brain.publish_next(assignments(&[(1, &[2, 3, 4])])).unwrap();
        assert_eq!(first.generation, 1);
        assert_eq!(second.generation, 2);
        assert!(matches!(
            brain.commit(&second),
            Err(ModelError::NonSequentialCommit {
                expected: 1,
                provided: 2
            })
        ));
        brain.commit(&first).unwrap();
        brain.commit(&second).unwrap();
        assert_eq!(brain.write_fence(2).unwrap().generation, 2);
    }

    #[test]
    fn malformed_replica_sets_fail_before_publication() {
        assert!(matches!(
            PlacementSnapshot::new(1, 1, assignments(&[(7, &[])])),
            Err(ModelError::EmptyReplicaSet(7))
        ));
        assert!(matches!(
            PlacementSnapshot::new(1, 1, assignments(&[(7, &[2, 2])])),
            Err(ModelError::DuplicateReplica { shard: 7, node: 2 })
        ));
    }

    #[test]
    fn duplicate_delivery_is_idempotent_but_same_generation_conflict_is_rejected() {
        let snapshot = PlacementSnapshot::new(1, 1, assignments(&[(1, &[1]), (2, &[1])])).unwrap();
        let mut node = NodeReplica::new(1);
        assert_eq!(node.stage(snapshot.clone()).unwrap(), StageOutcome::Staged);
        assert_eq!(
            node.stage(snapshot.clone()).unwrap(),
            StageOutcome::DuplicateStaged
        );

        let conflicting =
            PlacementSnapshot::new(1, 1, assignments(&[(1, &[1]), (2, &[2])])).unwrap();
        assert_eq!(
            node.stage(conflicting),
            Err(ModelError::ConflictingGeneration(1))
        );
    }

    #[test]
    fn partial_generation_cannot_activate_or_serve_writes() {
        let snapshot = PlacementSnapshot::new(1, 1, assignments(&[(1, &[1]), (2, &[1])])).unwrap();
        let mut node = NodeReplica::new(1);
        node.stage(snapshot).unwrap();
        node.acknowledge_shard(1, 1).unwrap();

        assert_eq!(
            node.activate(1),
            Err(ModelError::PartialActivation(vec![2]))
        );
        assert!(!node.accepts_write(
            WriteFence {
                term: 1,
                generation: 1
            },
            1
        ));
    }

    #[test]
    fn active_generation_requires_an_exact_committed_write_fence() {
        let mut brain = BrainPublisher::new(BrainRole::Leader, 1, 0).unwrap();
        let snapshot = brain
            .publish_next(assignments(&[(1, &[1]), (2, &[1, 2])]))
            .unwrap();
        assert!(matches!(
            brain.write_fence(1),
            Err(ModelError::UncommittedGeneration { .. })
        ));
        brain.commit(&snapshot).unwrap();

        let mut node = NodeReplica::new(1);
        activate_all(&mut node, snapshot);
        let fence = brain.write_fence(1).unwrap();
        node.install_write_fence(fence).unwrap();

        assert!(node.accepts_write(fence, 1));
        assert!(node.accepts_write(fence, 2));
        assert!(!node.accepts_write(
            WriteFence {
                term: 1,
                generation: 2
            },
            1
        ));
        assert!(!node.accepts_write(fence, 9));
    }

    #[test]
    fn newer_generation_staging_revokes_old_write_authority_until_activation() {
        let mut brain = BrainPublisher::new(BrainRole::Leader, 1, 0).unwrap();
        let first = brain
            .publish_next(assignments(&[(1, &[1]), (2, &[1])]))
            .unwrap();
        brain.commit(&first).unwrap();
        let first_fence = brain.write_fence(1).unwrap();

        let mut node = NodeReplica::new(1);
        activate_all(&mut node, first);
        node.install_write_fence(first_fence).unwrap();
        assert!(node.accepts_write(first_fence, 1));

        let second = brain
            .publish_next(assignments(&[(1, &[1]), (2, &[1]), (3, &[1])]))
            .unwrap();
        node.stage(second).unwrap();
        assert_eq!(node.active_generation(), Some(1));
        assert_eq!(node.current_write_fence(), None);
        assert!(!node.accepts_write(first_fence, 1));
    }

    #[test]
    fn leader_failover_fences_old_term_before_reauthorizing_same_generation() {
        let mut original = BrainPublisher::new(BrainRole::Leader, 1, 0).unwrap();
        let snapshot = original
            .publish_next(assignments(&[(1, &[1, 2, 3])]))
            .unwrap();
        original.commit(&snapshot).unwrap();
        let old_fence = original.write_fence(1).unwrap();

        let mut node = NodeReplica::new(1);
        activate_all(&mut node, snapshot);
        node.install_write_fence(old_fence).unwrap();
        assert!(node.accepts_write(old_fence, 1));

        let mut replacement = BrainPublisher::new(BrainRole::Follower, 1, 1).unwrap();
        replacement.become_leader(2).unwrap();
        node.observe_term(2).unwrap();
        assert!(!node.accepts_write(old_fence, 1));

        let new_fence = replacement.write_fence(1).unwrap();
        node.install_write_fence(new_fence).unwrap();
        assert!(node.accepts_write(new_fence, 1));
        assert!(!node.accepts_write(old_fence, 1));
    }

    #[test]
    fn restart_preserves_generation_but_loses_volatile_ack_and_write_authority() {
        let first = PlacementSnapshot::new(1, 1, assignments(&[(1, &[1]), (2, &[1])])).unwrap();
        let second =
            PlacementSnapshot::new(1, 2, assignments(&[(1, &[1]), (2, &[1]), (3, &[1])])).unwrap();
        let mut node = NodeReplica::new(1);
        activate_all(&mut node, first);
        node.install_write_fence(WriteFence {
            term: 1,
            generation: 1,
        })
        .unwrap();
        node.stage(second).unwrap();
        node.acknowledge_shard(2, 1).unwrap();
        node.restart();

        assert_eq!(node.active_generation(), Some(1));
        assert_eq!(node.staged_generation(), Some(2));
        assert!(node.acknowledged_shards().is_empty());
        assert_eq!(node.current_write_fence(), None);
        assert!(matches!(
            node.activate(2),
            Err(ModelError::PartialActivation(_))
        ));
    }

    #[test]
    fn newer_staged_generation_rejects_delayed_snapshot_and_ack() {
        let second = PlacementSnapshot::new(1, 2, assignments(&[(1, &[1]), (2, &[1])])).unwrap();
        let third = PlacementSnapshot::new(1, 3, assignments(&[(1, &[1]), (3, &[1])])).unwrap();
        let mut node = NodeReplica::new(1);
        node.stage(second.clone()).unwrap();
        node.stage(third).unwrap();

        assert!(matches!(
            node.stage(second),
            Err(ModelError::StaleGeneration {
                active: 3,
                provided: 2
            })
        ));
        assert_eq!(
            node.acknowledge_shard(2, 1),
            Err(ModelError::AckGenerationMismatch {
                staged: 3,
                provided: 2
            })
        );
    }

    #[test]
    fn removal_generation_deactivates_old_shards_before_new_authority() {
        let first = PlacementSnapshot::new(1, 1, assignments(&[(5, &[1, 2])])).unwrap();
        let removal = PlacementSnapshot::new(1, 2, assignments(&[(5, &[2, 3])])).unwrap();
        let mut node = NodeReplica::new(1);
        activate_all(&mut node, first);
        node.stage(removal).unwrap();
        node.activate(2).unwrap();
        node.install_write_fence(WriteFence {
            term: 1,
            generation: 2,
        })
        .unwrap();

        assert!(node.active_shards().is_empty());
        assert!(!node.accepts_write(
            WriteFence {
                term: 1,
                generation: 2
            },
            5
        ));
    }

    #[test]
    fn acknowledgement_order_does_not_change_activation_result() {
        let snapshot =
            PlacementSnapshot::new(4, 9, assignments(&[(1, &[7]), (2, &[7]), (3, &[7])])).unwrap();
        let orders = [
            [1, 2, 3],
            [1, 3, 2],
            [2, 1, 3],
            [2, 3, 1],
            [3, 1, 2],
            [3, 2, 1],
        ];

        for order in orders {
            let mut node = NodeReplica::new(7);
            node.stage(snapshot.clone()).unwrap();
            for shard in order {
                node.acknowledge_shard(9, shard).unwrap();
            }
            node.activate(9).unwrap();
            assert_eq!(node.active_shards(), BTreeSet::from([1, 2, 3]));
        }
    }

    #[test]
    fn same_generation_snapshot_can_be_reauthorized_in_a_higher_term() {
        let snapshot = PlacementSnapshot::new(1, 1, assignments(&[(1, &[1])])).unwrap();
        let mut node = NodeReplica::new(1);
        activate_all(&mut node, snapshot.clone());
        node.install_write_fence(WriteFence {
            term: 1,
            generation: 1,
        })
        .unwrap();

        let higher_term = PlacementSnapshot::new(2, 1, assignments(&[(1, &[1])])).unwrap();
        assert_eq!(
            node.stage(higher_term).unwrap(),
            StageOutcome::DuplicateActive
        );
        assert_eq!(node.current_write_fence(), None);
        node.install_write_fence(WriteFence {
            term: 2,
            generation: 1,
        })
        .unwrap();
        assert!(node.accepts_write(
            WriteFence {
                term: 2,
                generation: 1
            },
            1
        ));
    }
}

//! Control-plane voter topology (A2-5).
//!
//! Decides which cluster nodes are openraft **voters** (participate in
//! election + quorum) versus **learners** (receive the log but don't vote).
//! Policy (finalized 2026-07-14):
//!
//! - **≤5 nodes**: every node is a voter. Small clusters are simplest this way
//!   and tolerance is adequate (3→tolerate 1, 5→tolerate 2).
//! - **>5 nodes**: a fixed set of 5 voters; the rest are learners, so the
//!   consensus group stays small and elections stay fast (the etcd /
//!   CockroachDB system-range model).
//! - Voters are auto-selected as the first N by sorted node id (deterministic —
//!   every node computes the same set), or taken verbatim from an explicit
//!   `cluster.voters` list.
//!
//! Validation is fail-fast: the voter count must be odd (an even count adds no
//! fault tolerance while enlarging the split-brain surface), and an explicit
//! voter list must be a subset of cluster membership (a typo'd voter id would
//! silently cost a quorum vote and drop tolerance to zero).

/// Largest voter set we auto-select for a big cluster. Odd by construction.
const MAX_AUTO_VOTERS: usize = 5;

/// Error from resolving/validating the voter topology.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TopologyError {
    #[error("cluster has no members")]
    Empty,
    #[error("explicit voter {0:?} is not a cluster member")]
    VoterNotMember(String),
    #[error("voter count {0} must be odd (3 or 5); an even count adds no fault tolerance")]
    EvenVoterCount(usize),
    #[error("explicit voter list is empty")]
    NoVoters,
}

/// The resolved split of cluster members into voters and learners.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoterTopology {
    /// Nodes that vote (openraft voters). Sorted, deduplicated.
    pub voters: Vec<String>,
    /// Nodes that only receive the log (openraft learners). Sorted.
    pub learners: Vec<String>,
}

impl VoterTopology {
    /// Resolve the topology for a cluster.
    ///
    /// `members` is the full node-id set (this node + peers). `explicit` is an
    /// optional operator-provided voter list (`cluster.voters`); when `None`, the
    /// adaptive policy auto-selects.
    pub fn resolve(members: &[String], explicit: Option<&[String]>) -> Result<Self, TopologyError> {
        let mut all: Vec<String> = members.to_vec();
        all.sort();
        all.dedup();
        if all.is_empty() {
            return Err(TopologyError::Empty);
        }

        let voters: Vec<String> = match explicit {
            Some(list) => {
                let mut v: Vec<String> = list.to_vec();
                v.sort();
                v.dedup();
                if v.is_empty() {
                    return Err(TopologyError::NoVoters);
                }
                // Every explicit voter must be a real member.
                for voter in &v {
                    if !all.contains(voter) {
                        return Err(TopologyError::VoterNotMember(voter.clone()));
                    }
                }
                v
            }
            None => {
                // Adaptive: ≤5 → all; >5 → first 5 by sorted id.
                let n = all.len().min(MAX_AUTO_VOTERS);
                // If all.len() is even and ≤5, drop the last to keep it odd.
                let n = if all.len() <= MAX_AUTO_VOTERS && all.len().is_multiple_of(2) {
                    all.len() - 1
                } else {
                    n
                };
                all.iter().take(n).cloned().collect()
            }
        };

        if voters.len().is_multiple_of(2) {
            return Err(TopologyError::EvenVoterCount(voters.len()));
        }

        let learners: Vec<String> = all
            .iter()
            .filter(|m| !voters.contains(m))
            .cloned()
            .collect();

        Ok(VoterTopology { voters, learners })
    }

    /// Number of voter failures tolerated: `(n-1)/2` for n voters.
    pub fn fault_tolerance(&self) -> usize {
        self.voters.len().saturating_sub(1) / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn three_nodes_all_vote() {
        let t = VoterTopology::resolve(&m(&["c", "a", "b"]), None).unwrap();
        assert_eq!(t.voters, m(&["a", "b", "c"])); // sorted
        assert!(t.learners.is_empty());
        assert_eq!(t.fault_tolerance(), 1);
    }

    #[test]
    fn five_nodes_all_vote() {
        let t = VoterTopology::resolve(&m(&["n1", "n2", "n3", "n4", "n5"]), None).unwrap();
        assert_eq!(t.voters.len(), 5);
        assert!(t.learners.is_empty());
        assert_eq!(t.fault_tolerance(), 2);
    }

    #[test]
    fn four_nodes_drops_to_three_voters_to_stay_odd() {
        let t = VoterTopology::resolve(&m(&["a", "b", "c", "d"]), None).unwrap();
        assert_eq!(t.voters, m(&["a", "b", "c"]));
        assert_eq!(t.learners, m(&["d"]));
    }

    #[test]
    fn seven_nodes_fixes_five_voters_rest_learners() {
        let t =
            VoterTopology::resolve(&m(&["n1", "n2", "n3", "n4", "n5", "n6", "n7"]), None).unwrap();
        assert_eq!(t.voters, m(&["n1", "n2", "n3", "n4", "n5"]));
        assert_eq!(t.learners, m(&["n6", "n7"]));
        assert_eq!(t.fault_tolerance(), 2);
    }

    #[test]
    fn explicit_voters_honored() {
        let t = VoterTopology::resolve(&m(&["a", "b", "c", "d", "e"]), Some(&m(&["a", "b", "c"])))
            .unwrap();
        assert_eq!(t.voters, m(&["a", "b", "c"]));
        assert_eq!(t.learners, m(&["d", "e"]));
    }

    #[test]
    fn explicit_even_voter_count_rejected() {
        let e =
            VoterTopology::resolve(&m(&["a", "b", "c", "d"]), Some(&m(&["a", "b"]))).unwrap_err();
        assert_eq!(e, TopologyError::EvenVoterCount(2));
    }

    #[test]
    fn explicit_non_member_rejected() {
        let e =
            VoterTopology::resolve(&m(&["a", "b", "c"]), Some(&m(&["a", "x", "c"]))).unwrap_err();
        assert_eq!(e, TopologyError::VoterNotMember("x".to_string()));
    }

    #[test]
    fn empty_members_rejected() {
        assert_eq!(
            VoterTopology::resolve(&[], None).unwrap_err(),
            TopologyError::Empty
        );
    }

    #[test]
    fn single_node_is_sole_voter() {
        let t = VoterTopology::resolve(&m(&["solo"]), None).unwrap();
        assert_eq!(t.voters, m(&["solo"]));
        assert!(t.learners.is_empty());
        assert_eq!(t.fault_tolerance(), 0);
    }
}

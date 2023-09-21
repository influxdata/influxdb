/// A [`Topic`] contains a user-provided topic ID in the range 0 to 63
/// inclusive, encoded into an internal bitmap form.
///
/// A topic sets the Nth bit (from the LSB) to a 1, where N is the user-provided
/// topic ID.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Topic(u64);

impl std::fmt::Debug for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Topic")
            .field(&format_args!("{:0>64b}", self.0))
            .finish()
    }
}

impl Topic {
    /// Encode a topic ID (ranging from 0 to 63 inclusive) into a [`Topic`]
    /// bitmap.
    pub(crate) fn encode<T>(v: T) -> Self
    where
        T: Into<u64>,
    {
        let v = v.into();

        // Validate the topic ID can be mapped to a single bit in a u64
        assert!(v <= (u64::BITS as u64 - 1), "topic ID must be less than 64");

        // Map the topic ID into a bitset.
        Self(1 << v)
    }

    /// Construct a [`Topic`] from an encoded topic u64 containing single set
    /// bit.
    ///
    /// # Panics
    ///
    /// Panics if there's no topic bit set in `v`.
    pub(crate) fn from_encoded(v: u64) -> Self {
        assert_eq!(v.count_ones(), 1, "encoded topic must contain 1 set bit");
        assert_ne!(v, 0, "topic ID must be non-zero");
        Self(v)
    }

    /// Map a topic bitmap into an application-provided topic ID.
    pub(crate) fn as_id(&self) -> u64 {
        u64::BITS as u64 - 1 - self.0.leading_zeros() as u64
    }
}

impl From<Topic> for u64 {
    fn from(value: Topic) -> Self {
        value.0
    }
}

/// A set of [`Topic`] interests stored as a bitmap over 64 bits.
///
/// Each [`Topic`] contains exactly 1 set bit in a u64, and a [`TopicSet`]
/// contains a set of [`Topic`] bits.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct TopicSet(u64);

impl std::fmt::Debug for TopicSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TopicSet")
            .field(&format_args!("{:0>64b}", self.0))
            .finish()
    }
}

// Default to being interested in all topics (all bits set).
impl Default for TopicSet {
    fn default() -> Self {
        Self(u64::MAX)
    }
}

impl From<TopicSet> for u64 {
    fn from(v: TopicSet) -> Self {
        v.0
    }
}

impl From<u64> for TopicSet {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl TopicSet {
    /// Initialise a [`TopicSet`] that has no registered interests.
    pub(crate) fn empty() -> Self {
        Self(0)
    }

    /// Mark this [`TopicSet`] as interested in receiving messages from the
    /// specified [`Topic`].
    ///
    /// This method is idempotent.
    pub(crate) fn set_interested(&mut self, v: Topic) {
        debug_assert_eq!(v.0.count_ones(), 1);

        self.0 |= v.0;
    }

    /// Check if this [`TopicSet`] is interested in receiving messages from the
    /// specified [`Topic`].
    pub(crate) fn is_interested(&self, v: Topic) -> bool {
        debug_assert_eq!(v.0.count_ones(), 1);

        (self.0 & v.0) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interests() {
        let mut set = TopicSet::empty();

        let topic_a = Topic::encode(0_u64);
        let topic_b = Topic::encode(42_u64);

        assert!(!set.is_interested(topic_a));
        assert!(!set.is_interested(topic_b));

        set.set_interested(topic_b);
        set.set_interested(topic_b); // idempotent

        assert!(!set.is_interested(topic_a));
        assert!(set.is_interested(topic_b));

        set.set_interested(topic_a);

        assert!(set.is_interested(topic_a));
        assert!(set.is_interested(topic_b));
    }

    #[test]
    fn test_topic_round_trip() {
        for i in 0..64 {
            let topic = Topic::from_encoded(u64::from(Topic::encode(i)));
            assert_eq!(i, topic.as_id());
        }
    }

    #[test]
    #[should_panic(expected = "topic ID must be less than 64")]
    fn test_topic_64() {
        Topic::encode(64_u64);
    }
}

use futures::StreamExt;
use reaction::Reaction;
use tokio::{runtime::Handle, task::JoinSet};
use trigger::Trigger;

pub mod reaction;
pub mod trigger;

/// React to [triggers](Trigger) by a [reaction](Reaction).
#[derive(Debug)]
pub struct Reactor {
    _task: JoinSet<()>,
}

impl Reactor {
    /// Create new reactor given the triggers and a single reaction.
    ///
    /// The handle is used to run this in a background task.
    pub fn new(
        triggers: impl IntoIterator<Item = Trigger>,
        reaction: impl Reaction,
        handle: &Handle,
    ) -> Self {
        let mut task = JoinSet::new();
        let mut triggers = futures::stream::select_all(triggers);
        task.spawn_on(
            async move {
                let reaction = reaction;

                while let Some(()) = triggers.next().await {
                    // an observer should make sense of the result
                    reaction.exec().await.ok();
                }
            },
            handle,
        );

        Self { _task: task }
    }
}

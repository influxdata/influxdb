use std::{borrow::Cow, time::Duration};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetentionPeriod {
    Indefinite,
    Duration(Duration),
}

impl RetentionPeriod {
    /// Format the retention period the way the v1 /query API reports retention durations,
    /// e.g. `168h0m0s`.
    pub fn format_v1(&self) -> Cow<'static, str> {
        match self {
            RetentionPeriod::Indefinite => Cow::Borrowed("0s"),
            RetentionPeriod::Duration(duration) => {
                let total_secs = duration.as_secs();
                if total_secs == 0 {
                    return Cow::Borrowed("0s");
                }

                let hours = total_secs / 3_600;
                let minutes = (total_secs % 3_600) / 60;
                let seconds = total_secs % 60;

                if hours > 0 {
                    Cow::Owned(format!("{hours}h{minutes}m{seconds}s"))
                } else if minutes > 0 {
                    Cow::Owned(format!("{minutes}m{seconds}s"))
                } else {
                    Cow::Owned(format!("{seconds}s"))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;

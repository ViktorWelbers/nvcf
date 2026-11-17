// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Request counts behind the summary Pylon logs when a shutdown drain ends.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Running totals of the requests forwarded through Pylon's tunnels. Health
/// probes are not counted.
#[derive(Debug, Default)]
pub(crate) struct DrainCounters {
    started: AtomicU64,
    finished: AtomicU64,
}

/// Counts its request as finished when dropped.
#[derive(Debug)]
pub(crate) struct DrainRequestGuard<'a>(&'a DrainCounters);

impl Drop for DrainRequestGuard<'_> {
    fn drop(&mut self) {
        self.0.finished.fetch_add(1, Ordering::Release);
    }
}

/// Request totals at one point in time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DrainSnapshot {
    started: u64,
    finished: u64,
}

impl DrainCounters {
    pub(crate) fn begin_request(&self) -> DrainRequestGuard<'_> {
        self.started.fetch_add(1, Ordering::AcqRel);
        DrainRequestGuard(self)
    }

    pub(crate) fn snapshot(&self) -> DrainSnapshot {
        // Every request finishes after it starts, so reading `finished` first
        // never shows more finished requests than started ones.
        let finished = self.finished.load(Ordering::Acquire);
        let started = self.started.load(Ordering::Acquire);
        DrainSnapshot { started, finished }
    }
}

impl DrainSnapshot {
    pub(crate) fn in_flight(&self) -> u64 {
        self.started.saturating_sub(self.finished)
    }
}

/// What one shutdown drain did, from its start to its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainSummary {
    /// Requests running when the drain started.
    pub in_flight_at_start: u64,
    /// Requests that arrived during the drain, in the accept window.
    pub accepted: u64,
    /// Requests that ended during the drain, with any status.
    pub completed: u64,
    /// Requests still running when the drain ended. Pylon's exit cuts them off.
    pub cut_off: u64,
    pub duration: Duration,
    pub drain_timeout: Duration,
}

impl DrainSummary {
    pub(crate) fn between(
        start: DrainSnapshot,
        end: DrainSnapshot,
        duration: Duration,
        drain_timeout: Duration,
    ) -> Self {
        Self {
            in_flight_at_start: start.in_flight(),
            accepted: end.started.saturating_sub(start.started),
            completed: end.finished.saturating_sub(start.finished),
            cut_off: end.in_flight(),
            duration,
            drain_timeout,
        }
    }

    /// `completed` when no request was left running, `timed_out` otherwise.
    pub fn outcome(&self) -> &'static str {
        if self.cut_off == 0 {
            "completed"
        } else {
            "timed_out"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_counts_requests_across_the_drain() {
        let counters = DrainCounters::default();
        let finished_before = counters.begin_request();
        let running_at_start = counters.begin_request();
        let finishes_during_drain = counters.begin_request();
        drop(finished_before);

        let start = counters.snapshot();
        let accepted = counters.begin_request();
        drop(finishes_during_drain);
        drop(accepted);
        let end = counters.snapshot();

        let summary =
            DrainSummary::between(start, end, Duration::from_secs(3), Duration::from_secs(35));
        assert_eq!(summary.in_flight_at_start, 2);
        assert_eq!(summary.accepted, 1);
        assert_eq!(summary.completed, 2);
        assert_eq!(summary.cut_off, 1);
        assert_eq!(summary.outcome(), "timed_out");
        drop(running_at_start);
    }

    #[test]
    fn drain_with_no_request_left_running_completed() {
        let counters = DrainCounters::default();
        let request = counters.begin_request();
        let start = counters.snapshot();
        drop(request);

        let summary = DrainSummary::between(
            start,
            counters.snapshot(),
            Duration::from_millis(2100),
            Duration::from_secs(90),
        );
        assert_eq!(summary.in_flight_at_start, 1);
        assert_eq!(summary.accepted, 0);
        assert_eq!(summary.completed, 1);
        assert_eq!(summary.cut_off, 0);
        assert_eq!(summary.outcome(), "completed");
    }

    #[test]
    fn idle_drain_completed_with_zero_counts() {
        let counters = DrainCounters::default();
        let snapshot = counters.snapshot();
        let summary =
            DrainSummary::between(snapshot, snapshot, Duration::ZERO, Duration::from_secs(90));
        assert_eq!(
            (
                summary.in_flight_at_start,
                summary.accepted,
                summary.completed,
                summary.cut_off
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(summary.outcome(), "completed");
    }
}

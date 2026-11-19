//! Compiles weekly schedules into a step function over the minutes of a week.
//!
//! A [`CompiledSchedule`] answers "what is the floor now?" with one binary
//! search and "when does it change next?" with a second one. Evaluation runs on
//! local wall-clock time in the configured time zone; daylight-saving gaps and
//! folds are resolved explicitly so that the reported transition instants are
//! exact.

use std::fmt;

use jiff::civil::DateTime;
use jiff::tz::{AmbiguousOffset, TimeZone};
use jiff::{Timestamp, ToSpan};

use crate::crd::{Schedule, TimeBasedAutoscalerSpec};

/// Minutes in a day.
const DAY: u16 = 24 * 60;
/// Minutes in a week.
pub const WEEK: u16 = 7 * DAY;

/// Why a spec cannot be compiled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    /// A time is not `HH:MM` within the allowed range.
    InvalidTime {
        /// Schedule name.
        schedule: String,
        /// Offending value.
        value: String,
    },
    /// Start and end are equal.
    EmptyWindow {
        /// Schedule name.
        schedule: String,
    },
    /// A floor is negative.
    NegativeFloor {
        /// Schedule name, or `spec` for the default floor.
        schedule: String,
    },
    /// Two schedules share a name.
    DuplicateName {
        /// Schedule name.
        schedule: String,
    },
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTime { schedule, value } => {
                write!(f, "schedule {schedule:?}: invalid time {value:?}, expected HH:MM")
            }
            Self::EmptyWindow { schedule } => write!(f, "schedule {schedule:?}: start and end must differ"),
            Self::NegativeFloor { schedule } => write!(f, "{schedule}: minReplicas must not be negative"),
            Self::DuplicateName { schedule } => write!(f, "schedule {schedule:?}: duplicate name"),
        }
    }
}

impl std::error::Error for ScheduleError {}

/// A half-open window `[start, end)` in minutes of the week, `end <= WEEK`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    start: u16,
    end: u16,
    schedule: u16,
}

/// The floor as a step function over the minutes of a week, borrowing the
/// schedules it was compiled from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledSchedule<'a> {
    /// Segment start minutes, ascending, the first is `0`.
    starts: Vec<u16>,
    /// Floor of each segment; adjacent segments always differ.
    floors: Vec<i32>,
    /// `starts[first_change..]` are the minutes at which the floor changes:
    /// minute `0` is skipped when the week wraps without a change, and all of
    /// them when the floor is constant.
    first_change: usize,
    /// Windows used to report the active schedules.
    windows: Vec<Window>,
    /// Schedules indexed by [`Window::schedule`].
    schedules: &'a [Schedule],
}

/// Result of evaluating a schedule at an instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evaluation {
    /// Floor in effect.
    pub floor: i32,
    /// Next instant at which the floor changes; `None` when it is constant.
    pub next_change: Option<Timestamp>,
    /// Minute of the week the evaluation was made at.
    pub minute: u16,
}

/// Parses `HH:MM` into minutes after midnight; `24:00` only when `allow_end`.
fn parse_time(value: &str, allow_end: bool) -> Option<u16> {
    let bytes = value.as_bytes();
    if bytes.len() != 5 || bytes[2] != b':' {
        return None;
    }
    let digit = |i: usize| bytes[i].is_ascii_digit().then(|| u16::from(bytes[i] - b'0'));
    let hours = digit(0)? * 10 + digit(1)?;
    let minutes = digit(3)? * 10 + digit(4)?;
    let total = hours * 60 + minutes;
    (minutes < 60 && (total < DAY || (allow_end && total == DAY))).then_some(total)
}

impl<'a> CompiledSchedule<'a> {
    /// Validates and compiles a spec.
    ///
    /// # Errors
    ///
    /// Returns a [`ScheduleError`] describing the first invalid field.
    pub fn compile(spec: &'a TimeBasedAutoscalerSpec) -> Result<Self, ScheduleError> {
        if spec.min_replicas < 0 {
            return Err(ScheduleError::NegativeFloor { schedule: "spec".into() });
        }
        // At most one window per day plus one split at the end of the week.
        let mut windows = Vec::with_capacity(spec.schedules.len() * 8);
        for (index, schedule) in spec.schedules.iter().enumerate() {
            if spec.schedules[..index].iter().any(|other| other.name == schedule.name) {
                return Err(ScheduleError::DuplicateName { schedule: schedule.name.clone() });
            }
            // Bounded by the CRD (maxItems 64); saturate defensively.
            let index = u16::try_from(index).unwrap_or(u16::MAX);
            push_windows(schedule, index, &mut windows)?;
        }
        Ok(Self::sweep(spec.min_replicas, windows, &spec.schedules))
    }

    /// Builds the step function with a sweep over window boundaries.
    fn sweep(default: i32, windows: Vec<Window>, schedules: &'a [Schedule]) -> Self {
        let mut events: Vec<(u16, i32, i32)> = Vec::with_capacity(windows.len() * 2);
        for window in &windows {
            let floor = schedules[usize::from(window.schedule)].min_replicas;
            events.push((window.start, 1, floor));
            events.push((window.end, -1, floor));
        }
        events.sort_unstable_by_key(|&(minute, ..)| minute);

        // Open windows per floor; a handful of entries, so a vector beats a map.
        let mut active: Vec<(i32, i32)> = Vec::with_capacity(schedules.len());
        let mut starts = Vec::with_capacity(events.len() + 1);
        let mut floors: Vec<i32> = Vec::with_capacity(events.len() + 1);
        let mut i = 0;
        let mut minute = 0;
        loop {
            while i < events.len() && events[i].0 == minute {
                let (_, delta, floor) = events[i];
                if let Some(entry) = active.iter_mut().find(|(open, _)| *open == floor) {
                    entry.1 += delta;
                } else {
                    active.push((floor, delta));
                }
                i += 1;
            }
            active.retain(|&(_, count)| count != 0);
            let value = active.iter().fold(default, |max, &(floor, _)| max.max(floor));
            if floors.last() != Some(&value) {
                starts.push(minute);
                floors.push(value);
            }
            match events.get(i) {
                Some(&(next, ..)) if next < WEEK => minute = next,
                _ => break,
            }
        }

        let first_change = if floors.len() == 1 { starts.len() } else { usize::from(floors.first() == floors.last()) };
        Self { starts, floors, first_change, windows, schedules }
    }

    /// Floor at a minute of the week.
    #[must_use]
    pub fn floor_at(&self, minute: u16) -> i32 {
        let segment = self.starts.partition_point(|&start| start <= minute) - 1;
        self.floors[segment]
    }

    /// Next minute (possibly beyond [`WEEK`], wrapping) at which the floor changes.
    #[must_use]
    pub fn next_change_after(&self, minute: u16) -> Option<u32> {
        let changes = &self.starts[self.first_change..];
        let first = *changes.first()?;
        let index = changes.partition_point(|&change| change <= minute);
        Some(changes.get(index).map_or_else(|| u32::from(first) + u32::from(WEEK), |&change| u32::from(change)))
    }

    /// Names of the schedules active at a minute of the week, in spec order.
    #[must_use]
    pub fn active_at(&self, minute: u16) -> Vec<String> {
        let mut active: Vec<u16> =
            self.windows.iter().filter(|w| w.start <= minute && minute < w.end).map(|w| w.schedule).collect();
        active.sort_unstable();
        active.dedup();
        active.into_iter().map(|i| self.schedules[usize::from(i)].name.clone()).collect()
    }

    /// Evaluates the floor at `now` in `tz`.
    #[must_use]
    pub fn evaluate(&self, now: Timestamp, tz: &TimeZone) -> Evaluation {
        let local = tz.to_datetime(now);
        let minute = minute_of_week(local);
        let next_change = self.next_change_after(minute).map(|next| {
            let delta = i64::from(next) - i64::from(minute);
            let start_of_minute = local.with().second(0).subsec_nanosecond(0).build().unwrap_or(local);
            let target = start_of_minute.saturating_add(delta.minutes());
            resolve_local(tz, target, now)
        });
        Evaluation { floor: self.floor_at(minute), next_change, minute }
    }
}

fn push_windows(schedule: &Schedule, index: u16, windows: &mut Vec<Window>) -> Result<(), ScheduleError> {
    if schedule.min_replicas < 0 {
        return Err(ScheduleError::NegativeFloor { schedule: format!("schedule {:?}", schedule.name) });
    }
    let invalid = |value: &str| ScheduleError::InvalidTime { schedule: schedule.name.clone(), value: value.into() };
    let start = parse_time(&schedule.start, false).ok_or_else(|| invalid(&schedule.start))?;
    let end = parse_time(&schedule.end, true).ok_or_else(|| invalid(&schedule.end))?;
    if start == end {
        return Err(ScheduleError::EmptyWindow { schedule: schedule.name.clone() });
    }
    let length = if end > start { end - start } else { DAY - start + end };
    let days = schedule.days.iter().fold(0_u8, |mask, day| mask | 1 << day.index());
    for day in (0..7).filter(|day| days & 1 << day != 0) {
        let begin = day * DAY + start;
        let finish = begin + length;
        if finish <= WEEK {
            windows.push(Window { start: begin, end: finish, schedule: index });
        } else {
            windows.push(Window { start: begin, end: WEEK, schedule: index });
            windows.push(Window { start: 0, end: finish - WEEK, schedule: index });
        }
    }
    Ok(())
}

/// Minute of the week, Monday 00:00 being `0`.
#[must_use]
pub fn minute_of_week(local: DateTime) -> u16 {
    let day = u16::from(local.weekday().to_monday_zero_offset().unsigned_abs());
    let hour = u16::from(local.hour().unsigned_abs());
    let minute = u16::from(local.minute().unsigned_abs());
    day * DAY + hour * 60 + minute
}

/// Converts a local wall-clock time to the first instant after `now` at which
/// the local clock shows (or skips past) it.
fn resolve_local(tz: &TimeZone, local: DateTime, now: Timestamp) -> Timestamp {
    let ambiguous = tz.to_ambiguous_timestamp(local);
    match ambiguous.offset() {
        AmbiguousOffset::Unambiguous { offset } => offset.to_timestamp(local).unwrap_or(now),
        // The wall-clock time is skipped: the change happens when the clock jumps.
        AmbiguousOffset::Gap { after, .. } => after
            .to_timestamp(local)
            .ok()
            .and_then(|before_jump| tz.following(before_jump).next())
            .map_or(now, |transition| transition.timestamp()),
        // The wall-clock time occurs twice: pick the first occurrence after now.
        AmbiguousOffset::Fold { before, after } => {
            let first = before.to_timestamp(local).unwrap_or(now);
            if first > now { first } else { after.to_timestamp(local).unwrap_or(now) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{ScaleTargetRef, Weekday};

    const WEEKDAYS: [Weekday; 5] =
        [Weekday::Monday, Weekday::Tuesday, Weekday::Wednesday, Weekday::Thursday, Weekday::Friday];

    fn window(name: &str, days: &[Weekday], start: &str, end: &str, min_replicas: i32) -> Schedule {
        Schedule { name: name.into(), days: days.to_vec(), start: start.into(), end: end.into(), min_replicas }
    }

    fn spec(min_replicas: i32, schedules: Vec<Schedule>) -> TimeBasedAutoscalerSpec {
        TimeBasedAutoscalerSpec {
            scale_target_ref: ScaleTargetRef {
                api_version: "apps/v1".into(),
                kind: "Deployment".into(),
                name: "web".into(),
            },
            min_replicas,
            schedules,
        }
    }

    /// Binds `$name` to a schedule compiled from a spec that lives alongside it.
    macro_rules! compile {
        ($name:ident = $min:expr, $schedules:expr) => {
            let owned = spec($min, $schedules);
            let $name = CompiledSchedule::compile(&owned).unwrap();
        };
    }

    fn ts(value: &str) -> Timestamp {
        value.parse().unwrap()
    }

    const fn at(day: u16, hour: u16, minute: u16) -> u16 {
        day * DAY + hour * 60 + minute
    }

    #[test]
    fn parses_times() {
        assert_eq!(parse_time("00:00", false), Some(0));
        assert_eq!(parse_time("23:59", false), Some(1439));
        assert_eq!(parse_time("24:00", true), Some(DAY));
        for bad in ["24:00", "7:00", "07-00", "07:60", "ab:cd", "25:00", "07:0x", "007:00"] {
            assert_eq!(parse_time(bad, false), None, "{bad}");
        }
        assert_eq!(parse_time("24:01", true), None);
    }

    #[test]
    fn rejects_invalid_specs() {
        let error = |min, schedules| CompiledSchedule::compile(&spec(min, schedules)).unwrap_err().to_string();
        assert_eq!(error(-1, vec![]), "spec: minReplicas must not be negative");
        assert_eq!(
            error(0, vec![window("a", &WEEKDAYS, "08:00", "09:00", -2)]),
            r#"schedule "a": minReplicas must not be negative"#
        );
        assert_eq!(
            error(0, vec![window("a", &WEEKDAYS, "8:00", "09:00", 1)]),
            r#"schedule "a": invalid time "8:00", expected HH:MM"#
        );
        assert_eq!(
            error(0, vec![window("a", &WEEKDAYS, "08:00", "24:30", 1)]),
            r#"schedule "a": invalid time "24:30", expected HH:MM"#
        );
        assert_eq!(
            error(0, vec![window("a", &WEEKDAYS, "08:00", "08:00", 1)]),
            r#"schedule "a": start and end must differ"#
        );
        assert_eq!(
            error(0, vec![window("a", &WEEKDAYS, "08:00", "09:00", 1), window("a", &WEEKDAYS, "10:00", "11:00", 1)]),
            r#"schedule "a": duplicate name"#
        );
    }

    #[test]
    fn constant_floor_never_changes() {
        compile!(compiled = 2, vec![]);
        assert_eq!(compiled.floor_at(0), 2);
        assert_eq!(compiled.next_change_after(at(3, 12, 0)), None);
        let evaluation = compiled.evaluate(ts("2026-01-05T12:00:00Z"), &TimeZone::UTC);
        assert_eq!(evaluation, Evaluation { floor: 2, next_change: None, minute: at(0, 12, 0) });

        // A default above every window also yields a constant floor.
        compile!(compiled = 5, vec![window("low", &WEEKDAYS, "08:00", "18:00", 3)]);
        assert_eq!(compiled.next_change_after(0), None);
        assert_eq!(compiled.floor_at(at(0, 9, 0)), 5);
    }

    #[test]
    fn overlapping_windows_take_the_highest_floor() {
        compile!(
            compiled = 2,
            vec![
                window("business", &WEEKDAYS, "08:00", "18:00", 10),
                window("lunch", &Weekday::all(), "11:30", "13:30", 15),
            ]
        );
        let expectations = [
            (at(0, 7, 59), 2),
            (at(0, 8, 0), 10),
            (at(0, 12, 0), 15),
            (at(0, 13, 30), 10),
            (at(0, 18, 0), 2),
            (at(5, 12, 0), 15),
            (at(6, 23, 59), 2),
        ];
        for (minute, floor) in expectations {
            assert_eq!(compiled.floor_at(minute), floor, "minute {minute}");
        }
        assert_eq!(compiled.active_at(at(0, 12, 0)), ["business", "lunch"]);
        assert_eq!(compiled.active_at(at(5, 12, 0)), ["lunch"]);
        assert_eq!(compiled.active_at(at(6, 20, 0)), Vec::<String>::new());
        assert_eq!(compiled.next_change_after(at(0, 7, 0)), Some(u32::from(at(0, 8, 0))));
        assert_eq!(compiled.next_change_after(at(0, 8, 0)), Some(u32::from(at(0, 11, 30))));
    }

    #[test]
    fn overnight_windows_wrap_the_week() {
        compile!(compiled = 0, vec![window("night", &[Weekday::Sunday, Weekday::Sunday], "22:00", "06:00", 4)]);
        assert_eq!(compiled.floor_at(at(6, 21, 59)), 0);
        assert_eq!(compiled.floor_at(at(6, 23, 0)), 4);
        assert_eq!(compiled.floor_at(at(0, 5, 59)), 4);
        assert_eq!(compiled.floor_at(at(0, 6, 0)), 0);
        assert_eq!(compiled.active_at(at(0, 1, 0)), ["night"]);
        assert_eq!(compiled.next_change_after(at(6, 23, 0)), Some(u32::from(WEEK) + u32::from(at(0, 6, 0))));
        assert_eq!(compiled.next_change_after(at(0, 7, 0)), Some(u32::from(at(6, 22, 0))));
    }

    #[test]
    fn monday_midnight_is_only_a_change_when_the_floor_differs() {
        compile!(compiled = 0, vec![window("morning", &[Weekday::Monday], "08:00", "09:00", 5)]);
        assert_eq!(compiled.next_change_after(at(6, 23, 0)), Some(u32::from(WEEK) + u32::from(at(0, 8, 0))));

        compile!(compiled = 0, vec![window("late", &[Weekday::Sunday], "22:00", "24:00", 5)]);
        assert_eq!(compiled.floor_at(at(6, 23, 59)), 5);
        assert_eq!(compiled.floor_at(0), 0);
        assert_eq!(compiled.next_change_after(at(6, 23, 0)), Some(u32::from(WEEK)));
    }

    #[test]
    fn evaluates_in_the_time_zone() {
        let berlin = TimeZone::get("Europe/Berlin").unwrap();
        compile!(compiled = 0, vec![window("business", &WEEKDAYS, "08:00", "18:00", 10)]);
        // 07:59:30 local on Monday 2026-01-05 (CET, UTC+1).
        let evaluation = compiled.evaluate(ts("2026-01-05T06:59:30Z"), &berlin);
        assert_eq!(evaluation.floor, 0);
        assert_eq!(evaluation.minute, at(0, 7, 59));
        assert_eq!(evaluation.next_change, Some(ts("2026-01-05T07:00:00Z")));
        let evaluation = compiled.evaluate(ts("2026-01-05T07:00:00Z"), &berlin);
        assert_eq!((evaluation.floor, evaluation.next_change), (10, Some(ts("2026-01-05T17:00:00Z"))));
        // Friday evening: the next change is Monday morning.
        let evaluation = compiled.evaluate(ts("2026-01-09T17:00:00Z"), &berlin);
        assert_eq!((evaluation.floor, evaluation.next_change), (0, Some(ts("2026-01-12T07:00:00Z"))));
    }

    #[test]
    fn spring_forward_gap_changes_when_the_clock_jumps() {
        let berlin = TimeZone::get("Europe/Berlin").unwrap();
        compile!(compiled = 0, vec![window("early", &[Weekday::Sunday], "02:30", "05:00", 5)]);
        // 01:30 CET on 2026-03-29; 02:30 local does not exist, clocks jump to 03:00 CEST at 01:00Z.
        let evaluation = compiled.evaluate(ts("2026-03-29T00:30:00Z"), &berlin);
        assert_eq!((evaluation.floor, evaluation.next_change), (0, Some(ts("2026-03-29T01:00:00Z"))));
        let evaluation = compiled.evaluate(ts("2026-03-29T01:00:00Z"), &berlin);
        assert_eq!((evaluation.floor, evaluation.next_change), (5, Some(ts("2026-03-29T03:00:00Z"))));
    }

    #[test]
    fn fall_back_fold_uses_the_next_occurrence() {
        let berlin = TimeZone::get("Europe/Berlin").unwrap();
        compile!(compiled = 0, vec![window("early", &[Weekday::Sunday], "02:30", "04:00", 5)]);
        // 2026-10-25: 02:00-03:00 occurs twice, first in CEST (00:00Z-01:00Z) then in CET (01:00Z-02:00Z).
        let evaluation = compiled.evaluate(ts("2026-10-25T00:15:00Z"), &berlin);
        assert_eq!((evaluation.floor, evaluation.next_change), (0, Some(ts("2026-10-25T00:30:00Z"))));
        let evaluation = compiled.evaluate(ts("2026-10-25T00:45:00Z"), &berlin);
        assert_eq!((evaluation.floor, evaluation.next_change), (5, Some(ts("2026-10-25T03:00:00Z"))));
        let evaluation = compiled.evaluate(ts("2026-10-25T01:15:00Z"), &berlin);
        assert_eq!((evaluation.floor, evaluation.next_change), (0, Some(ts("2026-10-25T01:30:00Z"))));
    }

    #[test]
    fn minute_of_week_starts_on_monday() {
        assert_eq!(minute_of_week("2026-01-04T23:59".parse().unwrap()), WEEK - 1);
        assert_eq!(minute_of_week("2026-01-05T00:00".parse().unwrap()), 0);
    }
}

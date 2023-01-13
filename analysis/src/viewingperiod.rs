use std::{str::FromStr, fmt::Debug};

use chrono::{DateTime, Duration, Utc, TimeZone};

pub enum Status {
    Match,
    NoMatch,
    NoData,
    NoSound,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = match self {
            Self::Match => "MATCH",
            Self::NoMatch => "NO_MATCH",
            Self::NoSound => "NO_SOUND",
            Self::NoData => "NO_DATA"
        };
        write!(f, "{}", repr)
    }
}

#[derive(Debug)]
pub struct StatusParseErr {
    status_str: String
}

impl std::fmt::Display for StatusParseErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not a status: {}", self.status_str)
    }
}

impl std::error::Error for StatusParseErr {}

impl FromStr for Status {
    type Err = StatusParseErr;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "MATCH" => Ok(Self::Match),
            "NO_MATCH" => Ok(Self::NoMatch),
            "NO_DATA" => Ok(Self::NoData),
            "NO_SOUND" => Ok(Self::NoSound),
            other => Err(StatusParseErr { status_str: other.to_owned() })
        }
    }
}

pub struct ViewingPeriod {
    status: Status,
    user_id: u32,
    query_time: DateTime<Utc>,
    time_in_file: DateTime<Utc>,
    duration: Duration,

    stream_id: Option<String>,
    entry_id: Option<String>,
    ber: f32,
    valid: bool
}

impl Default for ViewingPeriod {
    fn default() -> Self {
        ViewingPeriod {
            status: Status::NoMatch,
            user_id: 0,
            query_time: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
            time_in_file: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
            duration: Duration::seconds(0),
            stream_id: Some(String::new()),
            entry_id: Some(String::new()),
            ber: 0.0,
            valid: false
        }
    }
}

impl ViewingPeriod {
    pub fn end_time(&self) -> DateTime<Utc> {
        return self.query_time + self.duration;
    }

    pub fn offset(&self) -> Duration {
        self.query_time - self.time_in_file
    }
}

#[inline]
fn millisec_to_sec(ms: i64) -> f64 {
    return ((ms / 1000) as f64) + ((ms % 1000) as f64);
}

#[inline]
fn print_duration(duration: Duration) -> String {
    return format!("{:.3}", millisec_to_sec(duration.num_milliseconds()));
}

impl std::fmt::Display for ViewingPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "user_id: {}, ", self.user_id)?;
        write!(f, "status: {}, ", self.status)?;
        write!(f, "stream_id: {}", match &self.stream_id {
            Some(stream) => stream,
            None => ""
        })?;
        write!(f, "entry_id: {}", match &self.entry_id {
            Some(entry) => entry,
            None => ""
        })?;
        write!(f, "offset_s: {}, ", print_duration(self.offset()))?;
        write!(f, "startTime: {}, ",
            self.query_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))?;
        write!(f, "endTime: {}, ",
            self.end_time().to_rfc3339_opts(chrono::SecondsFormat::Millis, true))?;
        write!(f, "duration: {}, ", print_duration(self.duration))?;
        write!(f, "ber: {:.2}, ", self.ber)?;
        write!(f, "valid: {}", self.valid)?;
        return Ok(());
    }
}
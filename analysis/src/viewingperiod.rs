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
        match s.to_uppercase().as_str() {
            "0" | "MATCH" => Ok(Self::Match),
            "1" | "NO_MATCH" | "NOMATCH" => Ok(Self::NoMatch),
            "2" | "NO_DATA" | "NODATA" => Ok(Self::NoData),
            "3" | "NO_SOUND" | "NOSOUND" => Ok(Self::NoSound),
            _ => Err(StatusParseErr { status_str: s.to_owned() })
        }
    }
}

impl std::convert::TryFrom<u32> for Status {
    type Error = StatusParseErr;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Match),
            1 => Ok(Self::NoMatch),
            2 => Ok(Self::NoData),
            3 => Ok(Self::NoSound),
            _ => Err(StatusParseErr { status_str: value.to_string() })
        }
    }
}

pub struct ViewingPeriod {
    pub(crate) provider: Option<String>,
    pub(crate) status: Status,
    pub(crate) user_id: String,
    pub(crate) query_time: DateTime<Utc>,
    pub(crate) time_in_file: DateTime<Utc>,
    pub(crate) duration: Duration,

    pub(crate) stream_id: Option<String>,
    pub(crate) entry_id: Option<String>,
    pub(crate) ber: f32,
    pub(crate) valid: bool
}

impl Default for ViewingPeriod {
    fn default() -> Self {
        ViewingPeriod {
            provider: None,
            status: Status::NoMatch,
            user_id: "NO_USER".to_string(),
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
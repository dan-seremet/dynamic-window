use std::str::FromStr;
use std::{path::Path, io::BufReader};
use std::io::{Read, BufRead};
use std::fs;

use chrono::{DateTime, TimeZone, Duration, Utc};

use crate::viewingperiod::{ViewingPeriod, Status};

/// Holds the names of the columns in a TSV or CSV file
type Header<'a> = Vec<&'a str>;

fn separator(path: impl AsRef<Path>) -> Option<char> {
    match path.as_ref().extension() {
        None => None,
        Some(os_str) => match os_str.to_str() {
            None => None,
            Some("csv") => Some(','),
            Some("tsv") => Some('\t'),
            Some(_) => None
        }
    }
}

fn read(path: impl AsRef<Path>) -> Vec<ViewingPeriod> {
    let sep = separator(&path)
        .expect("unsupported file extension");

    let file = fs::File::open(&path)
        .expect("failed to open file");
    let reader = BufReader::new(file);
    let mut lines_iter = reader.lines();
    let header_line = lines_iter.next()
        .expect("expected table to have at least header")
        .expect("failed to read header from file");
    let header: Header = header_line.split(sep).collect();

    return lines_iter
        .filter_map(|line|
            line.map_err(|err| println!("failed to read period line: {}", err)).ok())
        .map(|line| line_to_period(&line, &header, sep))
        .collect();
}

fn set_status(period: &mut ViewingPeriod, value: &str) {
    match Status::from_str(value) {
        Ok(status) => period.status = status,
        Err(err) => println!("failed to parse status '{}'", value)
    }
}

fn parse_datetime_str(value: &str) -> DateTime<chrono::Utc> {
    return chrono::Utc.datetime_from_str(value, "%F %T%.3f")
        .expect("failed to parse datetime");
}

fn parse_timestamp(value: &str) -> DateTime<chrono::Utc> {
    let millis = value.parse::<i64>()
        .expect("could not parse timestamp as integer");
    let naive = chrono::NaiveDateTime::from_timestamp_millis(millis)
        .expect("could not convert timestamp to datetime");
    return chrono::Utc.from_utc_datetime(&naive);
}

fn duration_from_millis(value: &str) -> Duration {
    let int_value = value.parse::<i64>()
        .expect("failed to parse millis from duration");
    return Duration::milliseconds(int_value);
}

fn duration_from_seconds(value: &str) -> Duration {
    let float_value = value.parse::<f64>()
        .expect("failed to parse seconds from duration");
    let milliseconds = (float_value * 1000.0).floor() as i64;
    return Duration::milliseconds(milliseconds);
}

fn line_to_period(line: &str, header: &Header, separator: char) -> ViewingPeriod {
    let removable_chars: &[_] = &['\'', '"', ' ', ','];
    let mut vp = ViewingPeriod::default();

    let mut offset: Option<Duration> = None;
    let mut end_time: Option<DateTime<Utc>> = None;

    for (key, &raw_value) in line.split(separator).zip(header.iter()) {

        let value = raw_value.trim().trim_matches(removable_chars);
        match key {
            "status" | "Status" => set_status(&mut vp, value),
            "userID" | "rss_id" | "DEVICE_ID" => vp.user_id = value.to_string(),
            "timeInFile" => vp.time_in_file = parse_timestamp(value),
            "tStartMsec" | "tStart" => vp.query_time = parse_timestamp(value),
            "startTime" | "start_ts" | "START" => vp.query_time = parse_datetime_str(value),
            "durationMsec" => vp.duration = duration_from_millis(value),
            "duration" => vp.duration = duration_from_seconds(value),
            "stream_id" | "Stream_id" | "stream_name" | "name" | "STREAM_LABEL" => vp.stream_id = Some(value.to_string()),
            "module_ref" => vp.provider = Some(value.to_string()),
            "period_id" | "id" => vp.entry_id = Some(value.to_string()),
            "bitErrorRate" | "ber" => vp.ber = value.parse::<f32>()
                .expect("failed to parse ber"),
            "valid" => vp.valid =  match value {
                "VALID" | "true" | "1" => true,
                _ => false
            },

            "offset" => offset = Some(duration_from_millis(value)),
            "offset_s" | "OFFSET" => offset = Some(duration_from_seconds(value)),
            "endTime" | "stop_ts" | "END" => end_time = Some(parse_datetime_str(value)),
            _ => println!("unrecognised field {}", key)
        };

        if let Some(offset_val) = offset {
            vp.time_in_file = vp.query_time - offset_val
        }
        if let Some(end_time_val) = end_time {
            vp.duration = end_time_val - vp.query_time
        }
        if let Some(stream) = &vp.stream_id {
            if !stream.is_empty() && stream != "0" && stream != "NO_DATA" && stream != "NO_MATCH" && stream != "NO_SOUND" {
                vp.status = Status::Match;
            }
        }
    }

    return vp;
}

#[cfg(test)]
mod test {
    use chrono::{Utc, Timelike};
    use super::*;

    #[test]
    fn test_parse_timestamp() {
        let millis: i64 = 1_673_531_400_000;
        let datetime = Utc.with_ymd_and_hms(2023, 1, 12, 13, 50, 0).unwrap();

        assert_eq!(
            parse_timestamp(millis.to_string().as_str()),
            datetime
        );
    }

    #[test]
    fn test_parse_time() {
        let string = "2023-01-12 13:50:00.123";
        let datetime = Utc.with_ymd_and_hms(2023, 1, 12, 13, 50, 0)
            .unwrap()
            .with_nanosecond(123_000_000)
            .unwrap();

        assert_eq!(
            parse_datetime_str(string),
            datetime
        );
    }
}
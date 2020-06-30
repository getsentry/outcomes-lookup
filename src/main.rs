use std::fmt::Write;

use argh::FromArgs;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use clickhouse_rs::Pool;
use uuid::Uuid;

type Error = Box<dyn std::error::Error>;

const CLICKHOUSE_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

/// Looks up outcomes from the outcomes dataset.
#[derive(Debug, FromArgs)]
struct Cli {
    /// the DSN for clickhouse to connect to.
    #[argh(option, default = "\"tcp://127.0.0.1:9000\".to_string()")]
    pub dsn: String,
    /// the project ID to scope the search down to.
    #[argh(option, short = 'p')]
    pub project_id: Option<u64>,
    /// start time to narrow down search.
    #[argh(option)]
    pub from: Option<DateTime<Utc>>,
    /// end time to narrow down search.
    #[argh(option)]
    pub to: Option<DateTime<Utc>>,
    /// the event ID to look up.
    #[argh(positional)]
    pub event_id: Uuid,
}

/// Possible outcomes
#[derive(Debug)]
pub enum Outcome {
    Accepted,
    Filtered,
    RateLimited,
    Invalid,
    Abuse,
    Unknown(u8),
}

struct OptFormat<T>(Option<T>);

impl<T: std::fmt::Debug> std::fmt::Display for OptFormat<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.0 {
            Some(ref val) => write!(f, "{:#?}", val),
            None => write!(f, "-"),
        }
    }
}

impl From<u8> for Outcome {
    fn from(value: u8) -> Self {
        match value {
            0 => Outcome::Accepted,
            1 => Outcome::Filtered,
            2 => Outcome::RateLimited,
            3 => Outcome::Invalid,
            4 => Outcome::Abuse,
            _ => Outcome::Unknown(value),
        }
    }
}

/// Given a project id makes a fast scan for the org id.
async fn find_org_id(pool: &Pool, project_id: u64) -> Result<Option<u64>, Error> {
    let mut client = pool.get_handle().await?;

    let block = client
        .query(format!(
            "select * from outcomes_raw_local where project_id = {} limit 1",
            project_id
        ))
        .fetch_all()
        .await?;

    Ok(if let Some(row) = block.rows().next() {
        let org_id: u64 = row.get("org_id")?;
        Some(org_id)
    } else {
        None
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli: Cli = argh::from_env();

    let pool = Pool::new(cli.dsn);

    let mut client = pool.get_handle().await?;
    let mut query = "select * from outcomes_raw_local prewhere ".to_string();

    if let Some(project_id) = cli.project_id {
        let org_id = find_org_id(&pool, project_id)
            .await?
            .ok_or("could not find org_id")?;

        write!(
            &mut query,
            "project_id = {} and org_id = {}",
            project_id, org_id
        )
        .unwrap();
    }

    if let Some(from) = cli.from {
        write!(
            &mut query,
            " and timestamp >= '{}'",
            from.format(CLICKHOUSE_FORMAT)
        )
        .unwrap();
    }

    if let Some(to) = cli.to {
        write!(
            &mut query,
            " and timestamp < '{}'",
            to.format(CLICKHOUSE_FORMAT)
        )
        .unwrap();
    }

    write!(&mut query, " where event_id = '{}'", cli.event_id).unwrap();

    let block = client.query(query).fetch_all().await?;

    let mut found = false;
    for row in block.rows() {
        let event_id: Option<Uuid> = row.get("event_id")?;
        let key_id: Option<u64> = row.get("key_id")?;
        let timestamp: DateTime<Tz> = row.get("timestamp")?;
        let outcome_raw: u8 = row.get("outcome")?;
        let reason: Option<String> = row.get("reason")?;
        let outcome: Outcome = outcome_raw.into();
        println!("event_id: {}", OptFormat(event_id));
        println!("key_id: {}", OptFormat(key_id));
        println!("timestamp: {}", timestamp);
        println!("outcome: {:?}", outcome);
        println!("reason: {}", OptFormat(reason));
        found = true;
    }

    if !found {
        println!("no outcomes found");
    }
    Ok(())
}

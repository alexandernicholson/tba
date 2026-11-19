//! Command line interface.

use std::ffi::OsString;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp};
use kube::Client;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::controller::{self, LeaderElection, Settings};
use crate::crd::{CONTROLLER_NAME, crd_yaml};
use crate::error::Error;
use crate::metrics::Metrics;
use crate::reconcile::Clock;
use crate::server::{self, Health};

/// Time based autoscaler: enforces time-of-week replica floors on Kubernetes workloads.
#[derive(Debug, Parser)]
#[command(name = "tba", version, about, propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the controller.
    Run(RunArgs),
    /// Print the custom resource definition.
    Crd,
    /// Evaluate time based autoscaler manifests offline.
    Evaluate(EvaluateArgs),
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// One JSON object per line.
    Json,
    /// Human readable text.
    Text,
}

#[derive(Debug, Args)]
struct RunArgs {
    /// IANA time zone schedules are evaluated in.
    #[arg(long, env = "TBA_TIMEZONE", default_value = "UTC", value_parser = parse_tz)]
    timezone: TimeZone,
    /// Namespace to watch; all namespaces when unset.
    #[arg(long, env = "TBA_WATCH_NAMESPACE")]
    namespace: Option<String>,
    /// Maximum delay between two reconciliations of an object, e.g. `60s`.
    #[arg(long, env = "TBA_RESYNC_INTERVAL", default_value = "60s", value_parser = parse_duration)]
    resync_interval: Duration,
    /// Delay before retrying after a failed API request, e.g. `15s`.
    #[arg(long, env = "TBA_RETRY_INTERVAL", default_value = "15s", value_parser = parse_duration)]
    retry_interval: Duration,
    /// Address serving /healthz, /readyz and /metrics.
    #[arg(long, env = "TBA_HTTP_ADDR", default_value = "0.0.0.0:8080")]
    http_addr: SocketAddr,
    /// Elect a leader through a coordination.k8s.io Lease before reconciling.
    #[arg(long, env = "TBA_LEADER_ELECT", default_value_t = true, action = ArgAction::Set)]
    leader_elect: bool,
    /// Name of the leader election Lease.
    #[arg(long, env = "TBA_LEASE_NAME", default_value = CONTROLLER_NAME)]
    lease_name: String,
    /// Namespace of the leader election Lease; defaults to the client namespace.
    #[arg(long, env = "POD_NAMESPACE")]
    lease_namespace: Option<String>,
    /// Identity of this replica; defaults to the host name.
    #[arg(long, env = "POD_NAME")]
    identity: Option<String>,
    /// Log format; the level is set with `RUST_LOG`.
    #[arg(long, env = "TBA_LOG_FORMAT", value_enum, default_value = "json")]
    log_format: LogFormat,
}

#[derive(Debug, Args)]
struct EvaluateArgs {
    /// File with time based autoscaler documents or lists, `-` for stdin.
    #[arg(short, long)]
    filename: PathBuf,
    /// Instant to evaluate at (RFC 3339); defaults to now.
    #[arg(long)]
    at: Option<Timestamp>,
    /// IANA time zone schedules are evaluated in.
    #[arg(long, env = "TBA_TIMEZONE", default_value = "UTC", value_parser = parse_tz)]
    timezone: TimeZone,
}

fn parse_tz(name: &str) -> Result<TimeZone, String> {
    TimeZone::get(name).map_err(|error| format!("unknown time zone {name:?}: {error}"))
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    value
        .parse::<SignedDuration>()
        .ok()
        .and_then(|duration| Duration::try_from(duration).ok())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| format!("invalid duration {value:?}, expected a positive value such as 30s or 5m"))
}

/// Entry point of the `tba` binary.
pub fn main() -> ExitCode {
    run(std::env::args_os().collect(), &mut std::io::stdin().lock(), &mut std::io::stdout().lock())
}

/// Parses `args` and executes the command.
pub fn run(args: Vec<OsString>, input: &mut dyn Read, out: &mut dyn Write) -> ExitCode {
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => {
            let _ = error.print();
            return if error.use_stderr() { ExitCode::from(2) } else { ExitCode::SUCCESS };
        }
    };
    match execute(cli, input, out) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn execute(cli: Cli, input: &mut dyn Read, out: &mut dyn Write) -> Result<(), Error> {
    match cli.command {
        Command::Crd => out.write_all(crd_yaml().as_bytes()).map_err(Error::from),
        Command::Evaluate(args) => {
            let text = if args.filename.as_os_str() == "-" {
                let mut text = String::new();
                input.read_to_string(&mut text)?;
                text
            } else {
                std::fs::read_to_string(&args.filename)?
            };
            let now = args.at.unwrap_or_else(Timestamp::now);
            crate::evaluate::evaluate(&text, now, &args.timezone, &mut std::io::BufWriter::new(out)).map(drop)
        }
        Command::Run(args) => tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(serve(args)),
    }
}

/// Initialises logging to stderr. Later calls are ignored.
pub fn init_logging(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    let _ = match format {
        LogFormat::Json => builder.json().flatten_event(true).try_init(),
        LogFormat::Text => builder.try_init(),
    };
}

async fn serve(args: RunArgs) -> Result<(), Error> {
    init_logging(args.log_format);
    let terminate = signal(SignalKind::terminate())?;
    let interrupt = signal(SignalKind::interrupt())?;
    let client = Client::try_from(kube::Config::infer().await?)?;
    let metrics = Arc::new(Metrics::default());
    let health = Arc::new(Health::new(Arc::clone(&metrics)));
    let shutdown = CancellationToken::new();
    let (addr, server) = server::bind(args.http_addr, Arc::clone(&health), shutdown.clone()).await?;
    let server = tokio::spawn(server);
    tokio::spawn(on_signal(terminate, interrupt, shutdown.clone(), Arc::clone(&health)));

    let identity = args
        .identity
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| format!("{CONTROLLER_NAME}-{}", std::process::id()));
    let leader_election = args.leader_elect.then(|| LeaderElection {
        lease_name: args.lease_name,
        lease_namespace: args.lease_namespace.unwrap_or_else(|| client.default_namespace().to_owned()),
    });
    let (version, timezone) = (env!("CARGO_PKG_VERSION"), args.timezone.iana_name().unwrap_or("custom"));
    tracing::info!(version, timezone, %addr, identity, "starting");
    let settings = Settings {
        tz: args.timezone,
        namespace: args.namespace,
        resync: args.resync_interval,
        retry: args.retry_interval,
        leader_election,
        identity,
        clock: Clock::System,
    };
    let result = controller::run(client, settings, metrics, shutdown.clone()).await;
    shutdown.cancel();
    server.await?;
    tracing::info!("stopped");
    result
}

/// Cancels `shutdown` on SIGTERM or SIGINT.
async fn on_signal(mut terminate: Signal, mut interrupt: Signal, shutdown: CancellationToken, health: Arc<Health>) {
    tokio::select! {
        _ = terminate.recv() => {},
        _ = interrupt.recv() => {},
        () = shutdown.cancelled() => return,
    }
    tracing::info!("shutdown requested");
    health.shutting_down();
    shutdown.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = "kind: TimeBasedAutoscaler\nmetadata: {name: web}\nspec:\n  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}\n  minReplicas: 2\n";

    fn invoke(args: &[&str], stdin: &str) -> (ExitCode, String) {
        let mut out = Vec::new();
        let args = std::iter::once("tba").chain(args.iter().copied()).map(OsString::from).collect();
        let code = run(args, &mut stdin.as_bytes(), &mut out);
        (code, String::from_utf8(out).unwrap())
    }

    #[test]
    fn prints_the_crd() {
        let (code, out) = invoke(&["crd"], "");
        assert_eq!(code, ExitCode::SUCCESS);
        assert!(out.contains("name: timebasedautoscalers.tba.alexandernicholson.github.io"), "{out}");
    }

    #[test]
    fn evaluates_stdin_and_files() {
        let (code, out) =
            invoke(&["evaluate", "-f", "-", "--at", "2026-01-05T12:00:00Z", "--timezone", "UTC"], MANIFEST);
        assert_eq!(code, ExitCode::SUCCESS);
        assert_eq!(out, "NAMESPACE\tNAME\tFLOOR\tACTIVE\tNEXT\ndefault\tweb\t2\t-\t-\n");

        let path = std::env::temp_dir().join(format!("tba-cli-{}.yaml", std::process::id()));
        std::fs::write(&path, MANIFEST).unwrap();
        let (code, out) = invoke(&["evaluate", "--filename", path.to_str().unwrap()], "");
        std::fs::remove_file(&path).unwrap();
        assert_eq!(code, ExitCode::SUCCESS);
        assert!(out.ends_with("default\tweb\t2\t-\t-\n"));
    }

    #[test]
    fn reports_failures_with_exit_codes() {
        assert_eq!(invoke(&["evaluate", "-f", "/nonexistent/tba.yaml"], "").0, ExitCode::FAILURE);
        assert_eq!(invoke(&["bogus"], "").0, ExitCode::from(2));
        assert_eq!(invoke(&["--help"], "").0, ExitCode::SUCCESS);
        assert_eq!(invoke(&["evaluate", "-f", "-", "--timezone", "Mars/Olympus"], "").0, ExitCode::from(2));
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("90s"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("5m"), Ok(Duration::from_secs(300)));
        for invalid in ["0s", "-5s", "soon"] {
            assert!(parse_duration(invalid).unwrap_err().contains("expected a positive value"), "{invalid}");
        }
    }

    #[test]
    fn logging_initialises_once() {
        init_logging(LogFormat::Text);
        init_logging(LogFormat::Json);
    }

    #[test]
    fn run_defaults_follow_conventions() {
        let cli = Cli::try_parse_from(["tba", "run"]).unwrap();
        let expected_addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
        assert!(matches!(
            cli.command,
            Command::Run(RunArgs { http_addr, leader_elect: true, ref lease_name, resync_interval, .. })
                if http_addr == expected_addr && lease_name == CONTROLLER_NAME && resync_interval == Duration::from_secs(60)
        ));
    }
}

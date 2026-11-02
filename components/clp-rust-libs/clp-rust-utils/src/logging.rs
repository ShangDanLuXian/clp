use tracing_appender::non_blocking::NonBlockingBuilder;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::{self};

/// Opaque struct to hold the worker guards for the background log writers.
/// These guards must be held for the lifetime of the program to ensure logs are flushed.
pub struct LoggerGuards {
    _stdout_guard: WorkerGuard,
    _file_guard: Option<WorkerGuard>,
}

/// Initializes the global tracing subscriber the same way as
/// [`set_up_logging_with_default_level`], with [`tracing::Level::ERROR`] as the default level.
///
/// # Returns
///
/// The guards of the background log writers.
#[must_use]
pub fn set_up_logging(log_filename: &str) -> LoggerGuards {
    set_up_logging_with_default_level(log_filename, tracing::Level::ERROR)
}

/// Initializes the global tracing subscriber with JSON-formatted output to stdout.
///
/// Filter directives are read from the `RUST_LOG` environment variable. If it's unset or contains
/// no valid directives, events at `default_level` or above are logged.
///
/// If the `CLP_LOGS_DIR` environment variable is set, logs are also written to a
/// rolling file (`{log_filename}`) in that directory. The returned [`LoggerGuards`]
/// must be held for the lifetime of the program to ensure logs are flushed.
///
/// # Returns
///
/// The guards of the background log writers.
#[must_use]
pub fn set_up_logging_with_default_level(
    log_filename: &str,
    default_level: tracing::Level,
) -> LoggerGuards {
    let subscriber = tracing_subscriber::fmt()
        .event_format(
            tracing_subscriber::fmt::format()
                .with_level(true)
                .with_target(false)
                .with_file(true)
                .with_line_number(true)
                .json(),
        )
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::from_level(default_level).into())
                .from_env_lossy(),
        )
        .with_ansi(false);

    let (stdout_writer, stdout_guard) = NonBlockingBuilder::default()
        .lossy(false)
        .finish(std::io::stdout());

    if let Ok(logs_directory) = std::env::var("CLP_LOGS_DIR") {
        let logs_directory = std::path::Path::new(logs_directory.as_str());
        let file_appender =
            RollingFileAppender::new(Rotation::HOURLY, logs_directory, log_filename);
        let (file_writer, file_guard) = NonBlockingBuilder::default()
            .lossy(false)
            .finish(file_appender);

        subscriber
            .with_writer(stdout_writer.and(file_writer))
            .init();

        LoggerGuards {
            _stdout_guard: stdout_guard,
            _file_guard: Some(file_guard),
        }
    } else {
        subscriber.with_writer(stdout_writer).init();

        LoggerGuards {
            _stdout_guard: stdout_guard,
            _file_guard: None,
        }
    }
}

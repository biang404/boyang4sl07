mod cleaner;
mod config;
mod coordinator;
mod core;
mod kafka;
mod messages;
mod worker;

use clap::{Parser, Subcommand};
use config::RunConfig;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Coordinator(RunConfig),
    Worker(WorkerArgs),
    Cleaner(CleanerArgs),
}

#[derive(clap::Args, Debug, Clone)]
struct WorkerArgs {
    #[command(flatten)]
    run: RunConfig,
    #[arg(long)]
    worker_id: Option<String>,
}

#[derive(clap::Args, Debug, Clone)]
struct CleanerArgs {
    #[arg(long)]
    bootstrap_servers: String,
    #[arg(long)]
    job_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.cmd {
        Commands::Coordinator(run) => coordinator::run_coordinator(run).await,
        Commands::Worker(worker) => worker::run_worker(worker.run, worker.worker_id).await,
        Commands::Cleaner(cleaner) => {
            cleaner::run_cleaner(&cleaner.bootstrap_servers, &cleaner.job_id).await
        }
    }
}

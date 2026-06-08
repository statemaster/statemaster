/// smdbctl — StateMaster admin & automation CLI
///
/// Operates directly against the storage layer (local admin mode) rather
/// than going through the TCP wire protocol. This makes it useful as an
/// offline / recovery tool as well as a development helper.
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use smdb_core::prelude::MachineDefinition;
use smdb_engine::Engine;
use smdb_storage::RedbEngine;

#[derive(Parser, Debug)]
#[command(
    name = "smdbctl",
    about = "StateMaster admin & automation CLI",
    long_about = "Directly opens the StateMaster database for read/write admin operations.\n\
                  No running daemon is required."
)]
struct Args {
    /// Address hint (unused in local mode; reserved for future remote mode)
    #[arg(short, long, default_value = "localhost:7632")]
    addr: String,

    /// Auth token (reserved for future remote mode)
    #[arg(short, long, env = "SMDB_TOKEN")]
    token: Option<String>,

    /// Path to the data directory containing statemaster.redb
    #[arg(short, long, env = "SMDB_DATA_DIR", default_value = "data")]
    data_dir: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Show server/storage status
    Status,

    /// List all machine definitions
    Machines,

    /// Show a machine definition
    Machine {
        /// Machine name
        name: String,
    },

    /// Show current state of an entity in a machine
    Current {
        /// Entity identifier
        entity_id: String,
        /// Machine name
        machine: String,
    },

    /// Show transition history of an entity
    History {
        /// Entity identifier
        entity_id: String,
        /// Machine name
        machine: String,
        /// Maximum number of records to return
        #[arg(short, long)]
        limit: Option<u32>,
    },

    /// Define a machine from a JSON file
    Define {
        /// Path to a JSON file containing a MachineDefinition
        file: String,
    },

    /// Fire a transition on an entity
    Transition {
        /// Entity identifier
        entity_id: String,
        /// Machine name
        machine: String,
        /// Event name
        event: String,
        /// Actor (e.g. "user:alice" or "system")
        #[arg(long, default_value = "smdbctl")]
        actor: Option<String>,
        /// JSON context object, e.g. '{"order_id": 42}'
        #[arg(long)]
        ctx: Option<String>,
        /// Optimistic locking: only fire if entity is at this version
        #[arg(long)]
        expected_version: Option<u64>,
    },
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn fmt_datetime(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn print_separator() {
    println!("{}", "-".repeat(60));
}

fn print_machine(m: &MachineDefinition) {
    println!("Machine:      {}", m.name);
    println!("Version:      {}", m.version);
    println!("Created:      {}", fmt_datetime(&m.created_at));
    println!("Initial:      {}", m.initial_state);
    println!("States ({}):", m.states.len());
    for s in &m.states {
        let marker = if s == &m.initial_state { " *" } else { "  " };
        println!("  {}{}", marker, s);
    }
    println!("Transitions ({}):", m.transitions.len());
    for t in &m.transitions {
        let guards = if t.guards.is_empty() {
            String::new()
        } else {
            format!("  [guards: {}]", t.guards.join(", "))
        };
        println!(
            "  {:20} {:30} -> {}{}",
            t.event,
            format!("[{}]", t.from_states.join(", ")),
            t.to_state,
            guards
        );
    }
    if !m.effects.is_empty() {
        println!("Effects ({}):", m.effects.len());
        for e in &m.effects {
            println!("  on '{}' -> emit '{}'", e.on_event, e.effect);
        }
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = Args::parse();

    // Init basic tracing to stderr so stdout stays clean for command output
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let db_path = PathBuf::from(&args.data_dir).join("statemaster.redb");

    // For Status we can report even if the DB doesn't exist yet.
    if matches!(args.command, Command::Status) {
        println!("addr:     {} (local mode)", args.addr);
        println!(
            "data_dir: {}",
            std::fs::canonicalize(&args.data_dir)
                .unwrap_or_else(|_| PathBuf::from(&args.data_dir))
                .display()
        );
        println!(
            "db_file:  {}",
            db_path.display()
        );
        println!(
            "db_exists: {}",
            if db_path.exists() { "yes" } else { "no" }
        );
        return Ok(());
    }

    // All other commands need an open database.
    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("creating data dir '{}'", args.data_dir))?;

    let storage = Arc::new(
        RedbEngine::open(&db_path)
            .with_context(|| format!("opening database at '{}'", db_path.display()))?,
    );
    let engine = Engine::new(storage);

    match args.command {
        Command::Status => unreachable!("handled above"),

        Command::Machines => {
            let machines = engine.list_machines().context("listing machines")?;
            if machines.is_empty() {
                println!("No machines defined.");
                return Ok(());
            }
            println!("{:<30} {:>7}  {}", "Name", "Version", "Initial State");
            print_separator();
            for m in &machines {
                println!("{:<30} {:>7}  {}", m.name, m.version, m.initial_state);
            }
            println!("\n{} machine(s) total.", machines.len());
        }

        Command::Machine { name } => {
            let m = engine
                .get_machine(&name)
                .with_context(|| format!("getting machine '{}'", name))?;
            print_separator();
            print_machine(&m);
            print_separator();
        }

        Command::Current { entity_id, machine } => {
            let state = engine
                .current(&entity_id, &machine)
                .with_context(|| format!("getting current state for '{entity_id}' in '{machine}'"))?;
            println!("Entity:    {}", state.entity_id);
            println!("Machine:   {}", state.machine);
            println!("State:     {}", state.current_state);
            println!("Version:   {}", state.version);
            println!("Updated:   {}", fmt_datetime(&state.updated_at));
            println!("Created:   {}", fmt_datetime(&state.created_at));
        }

        Command::History {
            entity_id,
            machine,
            limit,
        } => {
            let records = engine
                .history(&entity_id, &machine, limit, None)
                .with_context(|| format!("getting history for '{entity_id}' in '{machine}'"))?;

            if records.is_empty() {
                println!("No history found for '{entity_id}' in '{machine}'.");
                return Ok(());
            }

            println!(
                "{:>8}  {:<22}  {:<20}  {:<20}  {:<20}  {}",
                "Seq", "Timestamp", "Event", "From", "To", "Actor"
            );
            print_separator();
            for r in &records {
                println!(
                    "{:>8}  {:<22}  {:<20}  {:<20}  {:<20}  {}",
                    r.sequence,
                    r.timestamp.format("%Y-%m-%d %H:%M:%S"),
                    r.event,
                    r.from_state,
                    r.to_state,
                    r.actor
                );
            }
            println!("\n{} record(s).", records.len());
        }

        Command::Define { file } => {
            let content = std::fs::read_to_string(&file)
                .with_context(|| format!("reading file '{}'", file))?;
            let definition: MachineDefinition = serde_json::from_str(&content)
                .with_context(|| format!("parsing MachineDefinition from '{}'", file))?;
            let name = definition.name.clone();
            engine
                .define_machine(definition)
                .with_context(|| format!("defining machine '{}'", name))?;
            println!("Machine '{}' defined successfully.", name);
        }

        Command::Transition {
            entity_id,
            machine,
            event,
            actor,
            ctx,
            expected_version,
        } => {
            let ctx_value: serde_json::Value = match ctx {
                Some(s) => serde_json::from_str(&s)
                    .with_context(|| format!("parsing ctx JSON: {}", s))?,
                None => serde_json::json!({}),
            };
            let actor_str = actor.unwrap_or_else(|| "smdbctl".to_string());

            let result = engine
                .transition(
                    &entity_id,
                    &machine,
                    &event,
                    &actor_str,
                    ctx_value,
                    expected_version,
                    None,
                )
                .with_context(|| {
                    format!("firing '{event}' on '{entity_id}' in '{machine}'")
                })?;

            println!("Transition applied successfully.");
            println!("  Entity:    {}", result.entity_id);
            println!("  Machine:   {}", result.machine);
            println!("  From:      {}", result.from_state);
            println!("  To:        {}", result.to_state);
            println!("  Version:   {}", result.version);
            println!("  Seq:       {}", result.sequence);
            println!("  Txn ID:    {}", result.transition_id);
            println!("  Timestamp: {}", fmt_datetime(&result.timestamp));
        }
    }

    Ok(())
}

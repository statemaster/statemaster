/// smash — StateMaster interactive shell (the psql analog)
///
/// Operates directly against the storage layer in local mode.
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use smdb_core::prelude::MachineDefinition;
use smdb_engine::Engine;
use smdb_storage::RedbEngine;

#[derive(Parser, Debug)]
#[command(
    name = "smash",
    about = "StateMaster interactive shell",
    long_about = "Interactive shell for StateMaster — similar to psql.\n\
                  Directly opens the database (no daemon required in local mode).\n\n\
                  Type \\help inside the shell for available commands."
)]
struct Args {
    /// Server address (reserved for future remote mode)
    #[arg(short, long, default_value = "localhost:7632")]
    addr: String,

    /// Auth token (reserved for future remote mode)
    #[arg(short, long, env = "SMDB_TOKEN")]
    token: Option<String>,

    /// Path to the data directory containing statemaster.redb
    #[arg(short = 'd', long, env = "SMDB_DATA_DIR", default_value = "data")]
    data_dir: String,

    /// Run a single command and exit (like psql -c)
    #[arg(short = 'c', long)]
    command: Option<String>,
}

// ---------------------------------------------------------------------------
// Parsed command representation
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Cmd {
    Transition {
        entity_id: String,
        machine: String,
        event: String,
        actor: String,
        ctx: serde_json::Value,
        expect_version: Option<u64>,
    },
    Current {
        entity_id: String,
        machine: String,
    },
    History {
        entity_id: String,
        machine: String,
        limit: Option<u32>,
    },
    Define {
        source: String,
    },
    ListMachines,
    DescribeMachine {
        name: String,
    },
    DescribeEntity {
        entity_id: String,
    },
    Help,
    Quit,
    Empty,
}

// ---------------------------------------------------------------------------
// Tokenizer: split on whitespace but respects single-quoted strings
// ---------------------------------------------------------------------------

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;

    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\'' if !in_single_quote => {
                in_single_quote = true;
            }
            '\'' if in_single_quote => {
                in_single_quote = false;
            }
            ' ' | '\t' if !in_single_quote => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => {
                current.push(c);
            }
        }
        i += 1;
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

// ---------------------------------------------------------------------------
// Command parser
// ---------------------------------------------------------------------------

fn parse_command(input: &str) -> Result<Cmd, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(Cmd::Empty);
    }

    // Backslash meta-commands
    if let Some(rest) = trimmed.strip_prefix('\\') {
        let parts: Vec<&str> = rest.splitn(2, ' ').collect();
        return match parts[0] {
            "machines" => Ok(Cmd::ListMachines),
            "machine" => {
                if parts.len() < 2 || parts[1].trim().is_empty() {
                    Err("usage: \\machine <name>".to_string())
                } else {
                    Ok(Cmd::DescribeMachine {
                        name: parts[1].trim().to_string(),
                    })
                }
            }
            "entity" => {
                if parts.len() < 2 || parts[1].trim().is_empty() {
                    Err("usage: \\entity <entity_id>".to_string())
                } else {
                    Ok(Cmd::DescribeEntity {
                        entity_id: parts[1].trim().to_string(),
                    })
                }
            }
            "help" | "h" | "?" => Ok(Cmd::Help),
            "quit" | "q" | "exit" => Ok(Cmd::Quit),
            other => Err(format!("unknown meta-command: \\{}", other)),
        };
    }

    let tokens = tokenize(trimmed);
    if tokens.is_empty() {
        return Ok(Cmd::Empty);
    }

    match tokens[0].to_lowercase().as_str() {
        "transition" | "tr" => parse_transition(&tokens),
        "current" | "cur" => parse_current(&tokens),
        "history" | "hist" => parse_history(&tokens),
        "define" | "def" => parse_define(&tokens),
        "quit" | "exit" | "q" => Ok(Cmd::Quit),
        "help" | "h" | "?" => Ok(Cmd::Help),
        other => Err(format!("unknown command: '{}'. Type \\help for commands.", other)),
    }
}

fn parse_transition(tokens: &[String]) -> Result<Cmd, String> {
    // transition <entity_id> <machine> <event> [--actor NAME] [--ctx JSON] [--expect-version N]
    if tokens.len() < 4 {
        return Err(
            "usage: transition <entity_id> <machine> <event> [--actor NAME] [--ctx '{}'] [--expect-version N]"
                .to_string(),
        );
    }
    let entity_id = tokens[1].clone();
    let machine = tokens[2].clone();
    let event = tokens[3].clone();

    let mut actor = "smash".to_string();
    let mut ctx = serde_json::json!({});
    let mut expect_version: Option<u64> = None;

    let mut i = 4;
    while i < tokens.len() {
        match tokens[i].as_str() {
            "--actor" => {
                i += 1;
                if i >= tokens.len() {
                    return Err("--actor requires a value".to_string());
                }
                actor = tokens[i].clone();
            }
            "--ctx" => {
                i += 1;
                if i >= tokens.len() {
                    return Err("--ctx requires a JSON value".to_string());
                }
                ctx = serde_json::from_str(&tokens[i])
                    .map_err(|e| format!("invalid JSON for --ctx: {}", e))?;
            }
            "--expect-version" | "--expected-version" => {
                i += 1;
                if i >= tokens.len() {
                    return Err("--expect-version requires a number".to_string());
                }
                expect_version = Some(
                    tokens[i]
                        .parse::<u64>()
                        .map_err(|_| format!("'{}' is not a valid version number", tokens[i]))?,
                );
            }
            other => {
                return Err(format!("unexpected argument: '{}'", other));
            }
        }
        i += 1;
    }

    Ok(Cmd::Transition {
        entity_id,
        machine,
        event,
        actor,
        ctx,
        expect_version,
    })
}

fn parse_current(tokens: &[String]) -> Result<Cmd, String> {
    if tokens.len() < 3 {
        return Err("usage: current <entity_id> <machine>".to_string());
    }
    Ok(Cmd::Current {
        entity_id: tokens[1].clone(),
        machine: tokens[2].clone(),
    })
}

fn parse_history(tokens: &[String]) -> Result<Cmd, String> {
    if tokens.len() < 3 {
        return Err("usage: history <entity_id> <machine> [--limit N]".to_string());
    }
    let entity_id = tokens[1].clone();
    let machine = tokens[2].clone();
    let mut limit: Option<u32> = None;

    let mut i = 3;
    while i < tokens.len() {
        match tokens[i].as_str() {
            "--limit" | "-l" | "-n" => {
                i += 1;
                if i >= tokens.len() {
                    return Err("--limit requires a number".to_string());
                }
                limit = Some(
                    tokens[i]
                        .parse::<u32>()
                        .map_err(|_| format!("'{}' is not a valid limit", tokens[i]))?,
                );
            }
            other => return Err(format!("unexpected argument: '{}'", other)),
        }
        i += 1;
    }

    Ok(Cmd::History {
        entity_id,
        machine,
        limit,
    })
}

fn parse_define(tokens: &[String]) -> Result<Cmd, String> {
    if tokens.len() < 2 {
        return Err("usage: define <json_string_or_@file>".to_string());
    }
    // Rejoin remaining tokens so quoted JSON with spaces works
    let source = tokens[1..].join(" ");
    Ok(Cmd::Define { source })
}

// ---------------------------------------------------------------------------
// Command executor
// ---------------------------------------------------------------------------

fn execute(cmd: Cmd, engine: &Engine) -> Result<bool> {
    match cmd {
        Cmd::Empty => {}

        Cmd::Quit => {
            println!("Goodbye.");
            return Ok(false);
        }

        Cmd::Help => {
            print_help();
        }

        Cmd::ListMachines => {
            let machines = engine.list_machines().context("listing machines")?;
            if machines.is_empty() {
                println!("No machines defined.");
            } else {
                println!("{:<30} {:>7}  {}", "Name", "Version", "Initial State");
                println!("{}", "-".repeat(60));
                for m in &machines {
                    println!("{:<30} {:>7}  {}", m.name, m.version, m.initial_state);
                }
                println!("\n{} machine(s)", machines.len());
            }
        }

        Cmd::DescribeMachine { name } => {
            match engine.get_machine(&name) {
                Ok(m) => print_machine(&m),
                Err(e) => println!("Error: {}", e),
            }
        }

        Cmd::DescribeEntity { entity_id } => {
            // List all machines and show state of this entity in each
            let machines = engine.list_machines().context("listing machines")?;
            if machines.is_empty() {
                println!("No machines defined.");
                return Ok(true);
            }
            println!("Entity: {}", entity_id);
            println!("{}", "-".repeat(60));
            let mut found_any = false;
            for m in &machines {
                match engine.current(&entity_id, &m.name) {
                    Ok(state) => {
                        found_any = true;
                        println!(
                            "  {:30}  state={:<20}  v={}",
                            m.name, state.current_state, state.version
                        );
                    }
                    Err(_) => {
                        // Entity not present in this machine — show initial state as default
                        println!("  {:30}  state={:<20}  (not started)", m.name, m.initial_state);
                    }
                }
            }
            if !found_any {
                println!("No state records found for entity '{}'.", entity_id);
            }
        }

        Cmd::Current { entity_id, machine } => {
            match engine.current(&entity_id, &machine) {
                Ok(state) => {
                    println!("Entity:    {}", state.entity_id);
                    println!("Machine:   {}", state.machine);
                    println!("State:     {}", state.current_state);
                    println!("Version:   {}", state.version);
                    println!("Updated:   {}", state.updated_at.format("%Y-%m-%d %H:%M:%S UTC"));
                }
                Err(e) => println!("Error: {}", e),
            }
        }

        Cmd::History {
            entity_id,
            machine,
            limit,
        } => {
            match engine.history(&entity_id, &machine, limit, None) {
                Ok(records) if records.is_empty() => {
                    println!("No history for '{}' in '{}'.", entity_id, machine);
                }
                Ok(records) => {
                    println!(
                        "{:>8}  {:<20}  {:<20}  {:<20}  {:<20}  {}",
                        "Seq", "Timestamp", "Event", "From", "To", "Actor"
                    );
                    println!("{}", "-".repeat(100));
                    for r in &records {
                        println!(
                            "{:>8}  {:<20}  {:<20}  {:<20}  {:<20}  {}",
                            r.sequence,
                            r.timestamp.format("%Y-%m-%d %H:%M:%S"),
                            r.event,
                            r.from_state,
                            r.to_state,
                            r.actor
                        );
                    }
                    println!("\n{} record(s)", records.len());
                }
                Err(e) => println!("Error: {}", e),
            }
        }

        Cmd::Define { source } => {
            // Source can be: a raw JSON string, or @filename to read from file
            let json_str = if source.starts_with('@') {
                let path = &source[1..];
                std::fs::read_to_string(path)
                    .with_context(|| format!("reading file '{}'", path))?
            } else {
                source.clone()
            };

            match serde_json::from_str::<MachineDefinition>(&json_str) {
                Ok(def) => {
                    let name = def.name.clone();
                    match engine.define_machine(def) {
                        Ok(()) => println!("Machine '{}' defined.", name),
                        Err(e) => println!("Error: {}", e),
                    }
                }
                Err(e) => {
                    println!("Invalid MachineDefinition JSON: {}", e);
                }
            }
        }

        Cmd::Transition {
            entity_id,
            machine,
            event,
            actor,
            ctx,
            expect_version,
        } => {
            match engine.transition(
                &entity_id,
                &machine,
                &event,
                &actor,
                ctx,
                expect_version,
                None,
            ) {
                Ok(r) => {
                    println!(
                        "OK  {} -> {}  (v{}, seq {})",
                        r.from_state, r.to_state, r.version, r.sequence
                    );
                }
                Err(e) => println!("Error: {}", e),
            }
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

fn print_machine(m: &MachineDefinition) {
    println!("Machine:      {}", m.name);
    println!("Version:      {}", m.version);
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

fn print_help() {
    println!(
        r#"
smash — StateMaster interactive shell

Data commands:
  transition <entity_id> <machine> <event>
             [--actor NAME] [--ctx '{{"k":"v"}}'] [--expect-version N]
  current    <entity_id> <machine>
  history    <entity_id> <machine> [--limit N]
  define     <json_string>    -- inline JSON MachineDefinition
  define     @<file>          -- load MachineDefinition from file

Meta commands:
  \machines            list all machine definitions
  \machine <name>      describe a specific machine
  \entity  <id>        show all machine states for an entity
  \help | \h | \?      show this help
  \quit | \q | \exit   exit smash

Shortcuts:
  tr  = transition
  cur = current
  hist = history
  def = define
"#
    );
}

// ---------------------------------------------------------------------------
// REPL core
// ---------------------------------------------------------------------------

fn run_repl(engine: &Engine) -> Result<()> {
    let mut rl: DefaultEditor = DefaultEditor::new().context("initializing readline")?;

    // Load history from ~/.smash_history if it exists
    let history_path = dirs_or_home().join(".smash_history");
    let _ = rl.load_history(&history_path);

    println!("smash — StateMaster shell (type \\help for commands, \\quit to exit)");

    loop {
        match rl.readline("smdb> ") {
            Ok(line) => {
                let _ = rl.add_history_entry(&line);
                match parse_command(&line) {
                    Ok(cmd) => {
                        match execute(cmd, engine) {
                            Ok(true) => {} // continue
                            Ok(false) => break, // quit
                            Err(e) => println!("Error: {:#}", e),
                        }
                    }
                    Err(msg) => println!("Parse error: {}", msg),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                // Ctrl-C clears the line; keep looping
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D
                println!("\nGoodbye.");
                break;
            }
            Err(e) => {
                println!("Readline error: {}", e);
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
    Ok(())
}

fn dirs_or_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = Args::parse();

    // Tracing to stderr so the REPL stdout stays clean
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let db_path = PathBuf::from(&args.data_dir).join("statemaster.redb");
    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("creating data dir '{}'", args.data_dir))?;

    let storage = Arc::new(
        RedbEngine::open(&db_path)
            .with_context(|| format!("opening database at '{}'", db_path.display()))?,
    );
    let engine = Engine::new(storage);

    if let Some(one_shot) = args.command {
        // -c mode: execute one command and exit
        match parse_command(&one_shot) {
            Ok(cmd) => {
                match execute(cmd, &engine) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Error: {:#}", e);
                        std::process::exit(1);
                    }
                }
            }
            Err(msg) => {
                eprintln!("Parse error: {}", msg);
                std::process::exit(1);
            }
        }
    } else {
        run_repl(&engine)?;
    }

    Ok(())
}

//! Interactive REPL for the `nex` CLI.
//!
//! Provides a readline-style loop with history and simple keyword completion.
//! Each line is parsed into a one-shot command and dispatched through the same
//! path as the non-interactive CLI, so behaviour stays identical.
//!
//! Meta-commands (prefixed with `\` or `:`) control the session itself:
//! - `\help` / `:help`   — show help
//! - `\quit` / `:quit`   — exit (also Ctrl-D)
//! - bare Cypher is assumed when no subcommand keyword is recognized.

use anyhow::{Context, Result};
use nexora_client::NexoraClient;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{CompletionType, Config, Context as RlContext, Editor, Helper};

use crate::{execute, Commands};

/// Keywords offered by tab-completion.
const KEYWORDS: &[&str] = &[
    "cypher", "sql", "ingest", "sq", "node", "edges", "vector", "health", "help", "quit", "exit",
    // Common Cypher keywords for bare-query completion.
    "MATCH", "RETURN", "WHERE", "CREATE", "DELETE", "SET", "MERGE", "WITH", "ORDER BY", "LIMIT",
];

/// rustyline helper providing prefix keyword completion.
struct ReplHelper;

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &RlContext<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        // Complete the last whitespace-delimited word.
        let start = line[..pos]
            .rfind(char::is_whitespace)
            .map(|i| i + 1)
            .unwrap_or(0);
        let word = &line[start..pos];
        if word.is_empty() {
            return Ok((start, Vec::new()));
        }
        let word_lower = word.to_lowercase();
        let candidates = KEYWORDS
            .iter()
            .filter(|kw| kw.to_lowercase().starts_with(&word_lower))
            .map(|kw| Pair {
                display: kw.to_string(),
                replacement: kw.to_string(),
            })
            .collect();
        Ok((start, candidates))
    }
}

impl Hinter for ReplHelper {
    type Hint = String;
}
impl Highlighter for ReplHelper {}
impl Validator for ReplHelper {}
impl Helper for ReplHelper {}

/// Run the interactive REPL until EOF or an explicit quit.
pub async fn run_repl(client: &NexoraClient) -> Result<()> {
    println!("nexora interactive shell — type \\help for commands, \\quit to exit");

    let config = Config::builder()
        .completion_type(CompletionType::List)
        .auto_add_history(true)
        .build();
    let mut editor: Editor<ReplHelper, rustyline::history::DefaultHistory> =
        Editor::with_config(config).context("failed to initialize line editor")?;
    editor.set_helper(Some(ReplHelper));

    // Best-effort history persistence in the user's home dir.
    let history_path = history_file_path();
    if let Some(ref path) = history_path {
        let _ = editor.load_history(path);
    }

    loop {
        match editor.readline("nex> ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match handle_line(client, trimmed).await {
                    LineOutcome::Continue => {}
                    LineOutcome::Quit => break,
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C: abandon the current line, keep the session.
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D: exit cleanly.
                break;
            }
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        }
    }

    if let Some(ref path) = history_path {
        let _ = editor.save_history(path);
    }
    println!("bye");
    Ok(())
}

enum LineOutcome {
    Continue,
    Quit,
}

/// Handle a single REPL line: meta-commands, or a dispatched CLI command.
async fn handle_line(client: &NexoraClient, line: &str) -> LineOutcome {
    // Meta-commands.
    if let Some(meta) = line.strip_prefix('\\').or_else(|| line.strip_prefix(':')) {
        return handle_meta(meta.trim());
    }
    if matches!(line, "quit" | "exit") {
        return LineOutcome::Quit;
    }
    if line == "help" {
        print_help();
        return LineOutcome::Continue;
    }

    // Parse into a command and dispatch. Errors are printed but never abort
    // the session.
    match parse_line(line) {
        Ok(command) => {
            if let Err(e) = execute(client, command).await {
                eprintln!("Error: {e:#}");
            }
        }
        Err(e) => eprintln!("parse error: {e}"),
    }
    LineOutcome::Continue
}

fn handle_meta(meta: &str) -> LineOutcome {
    match meta {
        "quit" | "q" | "exit" => LineOutcome::Quit,
        "help" | "h" | "?" => {
            print_help();
            LineOutcome::Continue
        }
        other => {
            eprintln!("unknown meta-command '\\{other}' (try \\help)");
            LineOutcome::Continue
        }
    }
}

/// Parse a REPL line into a [`Commands`]. A line whose first token is not a
/// recognized subcommand is treated as a bare Cypher query.
fn parse_line(line: &str) -> Result<Commands> {
    let (head, rest) = split_first_word(line);
    match head {
        "cypher" => Ok(Commands::Cypher {
            query: rest.to_string(),
        }),
        "sql" => Ok(Commands::Sql {
            query: rest.to_string(),
        }),
        "health" => Ok(Commands::Health {
            readiness: rest.trim() == "--readiness" || rest.trim() == "readiness",
        }),
        "node" => parse_node(rest),
        "edges" => parse_edges(rest),
        "sq" => parse_sq(rest),
        "vector" => parse_vector(rest),
        // Anything else: assume the whole line is a Cypher query.
        _ => Ok(Commands::Cypher {
            query: line.to_string(),
        }),
    }
}

fn parse_node(rest: &str) -> Result<Commands> {
    let (sub, args) = split_first_word(rest);
    let tokens: Vec<&str> = args.split_whitespace().collect();
    match sub {
        "get" => {
            let qid = tokens.first().context("node get: missing <qid>")?;
            let key = tokens.get(1).context("node get: missing <key>")?;
            Ok(Commands::Node(crate::NodeCommands::Get {
                qid: (*qid).to_string(),
                key: (*key).to_string(),
            }))
        }
        "set" => {
            let qid = tokens.first().context("node set: missing <qid>")?;
            let key = tokens.get(1).context("node set: missing <key>")?;
            // Value is the remainder after qid+key so JSON with spaces works.
            let value = args
                .splitn(3, char::is_whitespace)
                .nth(2)
                .context("node set: missing <value>")?
                .to_string();
            Ok(Commands::Node(crate::NodeCommands::Set {
                qid: (*qid).to_string(),
                key: (*key).to_string(),
                value,
            }))
        }
        other => anyhow::bail!("node: unknown subcommand '{other}' (get|set)"),
    }
}

fn parse_edges(rest: &str) -> Result<Commands> {
    let (sub, args) = split_first_word(rest);
    let tokens: Vec<&str> = args.split_whitespace().collect();
    match sub {
        "get" => {
            let qid = tokens.first().context("edges get: missing <qid>")?;
            Ok(Commands::Edges(crate::EdgesCommands::Get {
                qid: (*qid).to_string(),
            }))
        }
        "add" => {
            // edges add <source> <edge_type> <target> [direction]
            let source = tokens.first().context("edges add: missing <source>")?;
            let edge_type = tokens.get(1).context("edges add: missing <edge_type>")?;
            let target = tokens.get(2).context("edges add: missing <target>")?;
            let direction = tokens.get(3).unwrap_or(&"outgoing").to_string();
            Ok(Commands::Edges(crate::EdgesCommands::Add {
                source: (*source).to_string(),
                edge_type: (*edge_type).to_string(),
                target: (*target).to_string(),
                direction,
            }))
        }
        other => anyhow::bail!("edges: unknown subcommand '{other}' (get|add)"),
    }
}

fn parse_sq(rest: &str) -> Result<Commands> {
    let (sub, args) = split_first_word(rest);
    let tokens: Vec<&str> = args.split_whitespace().collect();
    match sub {
        "list" => Ok(Commands::Sq(crate::SqCommands::List)),
        "get" => {
            let name = tokens.first().context("sq get: missing <name>")?;
            Ok(Commands::Sq(crate::SqCommands::Get {
                name: (*name).to_string(),
            }))
        }
        "delete" => {
            let name = tokens.first().context("sq delete: missing <name>")?;
            Ok(Commands::Sq(crate::SqCommands::Delete {
                name: (*name).to_string(),
            }))
        }
        "create" => {
            let name = tokens.first().context("sq create: missing <name>")?;
            let pattern = args
                .split_once(char::is_whitespace)
                .map(|x| x.1)
                .context("sq create: missing <pattern-json>")?
                .to_string();
            Ok(Commands::Sq(crate::SqCommands::Create {
                name: (*name).to_string(),
                pattern,
            }))
        }
        other => anyhow::bail!("sq: unknown subcommand '{other}' (list|get|create|delete)"),
    }
}

fn parse_vector(rest: &str) -> Result<Commands> {
    let (sub, args) = split_first_word(rest);
    match sub {
        "search" => {
            // vector search <json-array> [k]
            let (vector, k_str) = split_last_word(args.trim());
            let (vector, k) = if k_str.parse::<usize>().is_ok() && !vector.is_empty() {
                (vector.trim().to_string(), k_str.parse().unwrap())
            } else {
                (args.trim().to_string(), 10usize)
            };
            Ok(Commands::Vector(crate::VectorCommands::Search {
                vector,
                k,
            }))
        }
        other => anyhow::bail!("vector: unknown subcommand '{other}' (search)"),
    }
}

/// Split a string into (first_word, remainder). Remainder is trimmed of the
/// leading separator whitespace.
fn split_first_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.find(char::is_whitespace) {
        Some(i) => (&s[..i], s[i..].trim_start()),
        None => (s, ""),
    }
}

/// Split a string into (everything_before_last_word, last_word).
fn split_last_word(s: &str) -> (&str, &str) {
    match s.rfind(char::is_whitespace) {
        Some(i) => (s[..i].trim_end(), s[i..].trim()),
        None => ("", s),
    }
}

fn print_help() {
    println!(
        r#"nexora interactive shell — commands:
  <cypher>                     run a Cypher query (bare input is treated as Cypher)
  cypher <query>               run a Cypher query explicitly
  sql <query>                  run a SQL query
  node get <qid> <key>         get a node property
  node set <qid> <key> <json>  set a node property
  edges get <qid>              list a node's edges
  edges add <src> <type> <dst> [dir]   add an edge
  sq list                      list standing queries
  sq get <name>                get a standing query
  sq create <name> <json>      create a standing query
  sq delete <name>             delete a standing query
  vector search <json> [k]     vector similarity search
  health [readiness]           health / readiness check

meta:
  \help  \quit                 (also 'help', 'quit', 'exit', Ctrl-D)
"#
    );
}

/// Location of the persisted REPL history file (`~/.nexora_history`).
fn history_file_path() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".nexora_history"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_first_word() {
        assert_eq!(
            split_first_word("cypher MATCH (n)"),
            ("cypher", "MATCH (n)")
        );
        assert_eq!(split_first_word("health"), ("health", ""));
        assert_eq!(split_first_word("  node  get x"), ("node", "get x"));
        assert_eq!(split_first_word(""), ("", ""));
    }

    #[test]
    fn test_split_last_word() {
        assert_eq!(split_last_word("[1,2,3] 5"), ("[1,2,3]", "5"));
        assert_eq!(split_last_word("[1,2,3]"), ("", "[1,2,3]"));
    }

    #[test]
    fn test_parse_bare_cypher() {
        let cmd = parse_line("MATCH (n) RETURN n").unwrap();
        match cmd {
            Commands::Cypher { query } => assert_eq!(query, "MATCH (n) RETURN n"),
            _ => panic!("expected Cypher"),
        }
    }

    #[test]
    fn test_parse_explicit_cypher() {
        let cmd = parse_line("cypher MATCH (n) RETURN n").unwrap();
        match cmd {
            Commands::Cypher { query } => assert_eq!(query, "MATCH (n) RETURN n"),
            _ => panic!("expected Cypher"),
        }
    }

    #[test]
    fn test_parse_sql() {
        let cmd = parse_line("sql SELECT * FROM nodes").unwrap();
        match cmd {
            Commands::Sql { query } => assert_eq!(query, "SELECT * FROM nodes"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn test_parse_node_get() {
        let cmd = parse_line("node get abc123 name").unwrap();
        match cmd {
            Commands::Node(crate::NodeCommands::Get { qid, key }) => {
                assert_eq!(qid, "abc123");
                assert_eq!(key, "name");
            }
            _ => panic!("expected Node::Get"),
        }
    }

    #[test]
    fn test_parse_node_set_json_with_spaces() {
        let cmd = parse_line(r#"node set abc123 profile {"a": 1, "b": 2}"#).unwrap();
        match cmd {
            Commands::Node(crate::NodeCommands::Set { qid, key, value }) => {
                assert_eq!(qid, "abc123");
                assert_eq!(key, "profile");
                assert_eq!(value, r#"{"a": 1, "b": 2}"#);
            }
            _ => panic!("expected Node::Set"),
        }
    }

    #[test]
    fn test_parse_node_get_missing_key_errors() {
        assert!(parse_line("node get abc123").is_err());
    }

    #[test]
    fn test_parse_edges_add_with_direction() {
        let cmd = parse_line("edges add src KNOWS dst incoming").unwrap();
        match cmd {
            Commands::Edges(crate::EdgesCommands::Add {
                source,
                edge_type,
                target,
                direction,
            }) => {
                assert_eq!(source, "src");
                assert_eq!(edge_type, "KNOWS");
                assert_eq!(target, "dst");
                assert_eq!(direction, "incoming");
            }
            _ => panic!("expected Edges::Add"),
        }
    }

    #[test]
    fn test_parse_edges_add_default_direction() {
        let cmd = parse_line("edges add src KNOWS dst").unwrap();
        match cmd {
            Commands::Edges(crate::EdgesCommands::Add { direction, .. }) => {
                assert_eq!(direction, "outgoing");
            }
            _ => panic!("expected Edges::Add"),
        }
    }

    #[test]
    fn test_parse_sq_list() {
        assert!(matches!(
            parse_line("sq list").unwrap(),
            Commands::Sq(crate::SqCommands::List)
        ));
    }

    #[test]
    fn test_parse_sq_create() {
        let cmd = parse_line(r#"sq create adults {"type":"age"}"#).unwrap();
        match cmd {
            Commands::Sq(crate::SqCommands::Create { name, pattern }) => {
                assert_eq!(name, "adults");
                assert_eq!(pattern, r#"{"type":"age"}"#);
            }
            _ => panic!("expected Sq::Create"),
        }
    }

    #[test]
    fn test_parse_vector_search_with_k() {
        let cmd = parse_line("vector search [1.0,2.0,3.0] 5").unwrap();
        match cmd {
            Commands::Vector(crate::VectorCommands::Search { vector, k }) => {
                assert_eq!(vector, "[1.0,2.0,3.0]");
                assert_eq!(k, 5);
            }
            _ => panic!("expected Vector::Search"),
        }
    }

    #[test]
    fn test_parse_vector_search_default_k() {
        let cmd = parse_line("vector search [1.0,2.0,3.0]").unwrap();
        match cmd {
            Commands::Vector(crate::VectorCommands::Search { vector, k }) => {
                assert_eq!(vector, "[1.0,2.0,3.0]");
                assert_eq!(k, 10);
            }
            _ => panic!("expected Vector::Search"),
        }
    }

    #[test]
    fn test_parse_health_readiness() {
        match parse_line("health readiness").unwrap() {
            Commands::Health { readiness } => assert!(readiness),
            _ => panic!("expected Health"),
        }
        match parse_line("health").unwrap() {
            Commands::Health { readiness } => assert!(!readiness),
            _ => panic!("expected Health"),
        }
    }
}

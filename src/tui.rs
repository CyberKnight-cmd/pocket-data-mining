use std::{
    io,
    sync::{atomic::Ordering, Arc, atomic::AtomicBool},
    time::{Duration, Instant},
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Modifier},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph},
    Terminal,
};

use crate::{
    buffer_pool::pool::BufferPool,
    progress::MiningProgress,
};

pub fn run_tui(
    progress: Arc<MiningProgress>,
    pool: Arc<BufferPool>,
    guard: Arc<crate::mining::core::MemoryGuard>,
    done: Arc<AtomicBool>,
) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let start_time = Instant::now();

    loop {
        if done.load(Ordering::Relaxed) {
            // Auto-close TUI when algorithm finishes so the terminal isn't blocked.
            break;
        }

        terminal.draw(|f| {
            let size = f.size();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3), // Header
                    Constraint::Min(0),    // Main content
                ])
                .split(size);

            // Header
            let header = Paragraph::new(Line::from(vec![
                Span::styled(" Air-HUIM Mining Dashboard ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            ]))
            .block(Block::default().borders(Borders::ALL));
            f.render_widget(header, chunks[0]);

            let main_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(33),
                    Constraint::Percentage(34),
                    Constraint::Percentage(33),
                ])
                .split(chunks[1]);

            // Left: Status
            let stage = progress.stage.lock().unwrap().clone();
            let active = progress.active_prefix.lock().unwrap().clone();
            let elapsed = start_time.elapsed().as_secs();
            let status_text = vec![
                Line::from(vec![Span::raw(format!("Stage: {}", stage))]),
                Line::from(vec![Span::raw(format!("Exploring: {}", active))]),
                Line::from(vec![Span::raw(format!("Uptime: {}s", elapsed))]),
            ];
            let status_block = Paragraph::new(status_text)
                .block(Block::default().title("Status").borders(Borders::ALL));
            f.render_widget(status_block, main_chunks[0]);

            // Center: whole-process budget (ledger) and the buffer pool's share of it
            const MB: f64 = 1024.0 * 1024.0;
            let budget = guard.budget().max(1) as f64;
            let ledger = guard.used() as f64;
            let pool_used = pool.used_bytes() as f64;
            let rss = crate::mining::core::memory_guard::current_rss_bytes() as f64;
            let ratio = (pool_used / budget).clamp(0.0, 1.0);
            let process_ram_mb = rss / MB;
            let global_ratio = (rss.max(ledger) / budget).clamp(0.0, 1.0);

            let m = &pool.metrics;
            let hits = m.hits.load(Ordering::Relaxed);
            let misses = m.misses.load(Ordering::Relaxed);
            let evictions = m.evictions.load(Ordering::Relaxed);
            
            let fast_reads = progress.fast_path_reads.load(Ordering::Relaxed);
            let fast_writes = progress.fast_path_writes.load(Ordering::Relaxed);

            let pool_text = vec![
                Line::from(vec![Span::styled("Buffer Pool (Disk):", Style::default().add_modifier(Modifier::BOLD))]),
                Line::from(format!("  Hits: {}", hits)),
                Line::from(format!("  Misses: {}", misses)),
                Line::from(format!("  Evictions: {}", evictions)),
                Line::from(""),
                Line::from(vec![Span::styled("Fast-Path (RAM):", Style::default().add_modifier(Modifier::BOLD))]),
                Line::from(format!("  Reads: {}", fast_reads)),
                Line::from(format!("  Writes: {}", fast_writes)),
            ];

            let pool_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Length(3), Constraint::Min(0)])
                .split(main_chunks[1]);

            let global_gauge = Gauge::default()
                .block(Block::default().title("Process Memory vs Budget (RSS | ledger)").borders(Borders::ALL))
                .gauge_style(Style::default().fg(Color::Magenta))
                .ratio(global_ratio)
                .label(format!("{:.1} MB | {:.1} MB / {:.1} MB", process_ram_mb, ledger / MB, budget / MB));
            f.render_widget(global_gauge, pool_layout[0]);

            let bp_gauge = Gauge::default()
                .block(Block::default().title("Buffer Pool (share of budget)").borders(Borders::ALL))
                .gauge_style(Style::default().fg(Color::Cyan))
                .ratio(ratio)
                .label(format!("{:.1} MB / {:.1} MB", pool_used / MB, budget / MB));
            f.render_widget(bp_gauge, pool_layout[1]);

            let metrics_block = Paragraph::new(pool_text)
                .block(Block::default().title("Data Layer Metrics").borders(Borders::ALL));
            f.render_widget(metrics_block, pool_layout[2]);

            // Right: Mining
            let huis = progress.huis_found.load(Ordering::Relaxed);
            let depth = progress.current_depth.load(Ordering::Relaxed);
            let mining_text = vec![
                Line::from(format!("HUIs Found: {}", huis)),
                Line::from(format!("Current DFS Depth: {}", depth)),
            ];
            let mining_block = Paragraph::new(mining_text)
                .block(Block::default().title("Mining Stats").borders(Borders::ALL));
            f.render_widget(mining_block, main_chunks[2]);
        })?;

        if crossterm::event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    // User requested mid-way exit
                    let _ = disable_raw_mode();
                    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
                    let _ = terminal.show_cursor();
                    println!("Mining aborted by user.");
                    std::process::exit(0);
                }
            }
        }
    }

    // Cleanup
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

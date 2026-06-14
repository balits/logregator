use owo_colors::OwoColorize;

pub fn info(label: &str, msg: &str) {
    println!("{}: {}: {}", "=> INFO".green(), label.bold(), msg.bold());
}

pub fn warn(msg: &str) {
    eprintln!("{}: {}", "=> WARN".yellow().bold(), msg.bold());
}

pub fn error(msg: &str) {
    eprintln!("{}: {}", "=> ERRO".red().bold(), msg.bold());
}

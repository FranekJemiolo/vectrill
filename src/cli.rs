//! CLI interface for vectrill

#[cfg(feature = "cli")]
pub mod app {
    use clap::Parser;

    #[derive(Parser)]
    #[command(name = "vectrill")]
    #[command(about = "High-performance Arrow-native streaming engine")]
    pub struct Cli {
        #[arg(short, long)]
        pub config: Option<String>,

        #[arg(short, long)]
        pub verbose: bool,
    }
}

#[cfg(not(feature = "cli"))]
pub mod app {
    pub struct Cli {
        pub config: Option<String>,
        pub verbose: bool,
    }
}

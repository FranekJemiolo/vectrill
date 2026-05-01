//! CLI interface for vectrill

#[cfg(feature = "cli")]
pub mod app {
    #[cfg(feature = "cli")]
    use clap::Parser;

    #[cfg(feature = "cli")]
    #[derive(Parser)]
    #[cfg(feature = "cli")]
    #[command(name = "vectrill")]
    #[cfg(feature = "cli")]
    #[command(about = "High-performance Arrow-native streaming engine")]
    pub struct Cli {
        #[cfg(feature = "cli")]
        #[arg(short, long)]
        pub config: Option<String>,

        #[cfg(feature = "cli")]
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

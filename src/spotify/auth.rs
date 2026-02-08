use anyhow::{Result, Context};
use rspotify::{AuthCodeSpotify, Credentials, OAuth, prelude::*, scopes};
use std::path::PathBuf;

pub const DEFAULT_REDIRECT_URI: &str = "http://localhost:8888/callback";

pub fn get_token_path() -> Result<PathBuf> {
    let mut path = crate::shared::paths::config_dir().context("Failed to get config directory")?;
    path.push("spotify_token.json");
    Ok(path)
}

pub fn create_spotify_client() -> Result<AuthCodeSpotify> {
    // These should ideally be configurable, but for now we need some defaults
    // or the user must provide them via env vars or config.
    // I'll check env vars first.
    let client_id = std::env::var("SPOTIFY_CLIENT_ID").ok();
    let client_secret = std::env::var("SPOTIFY_CLIENT_SECRET").ok();

    if client_id.is_none() || client_secret.is_none() {
        return Err(anyhow::anyhow!("SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET environment variables must be set for Spotify integration"));
    }

    let credentials = Credentials::new(&client_id.unwrap(), &client_secret.unwrap());
    let oauth = OAuth {
        redirect_uri: DEFAULT_REDIRECT_URI.to_string(),
        scopes: scopes!(
            "user-read-playback-state",
            "user-modify-playback-state",
            "user-read-currently-playing",
            "user-library-read",
            "user-library-modify",
            "playlist-read-private",
            "playlist-read-collaborative",
            "streaming"
        ),
        ..Default::default()
    };

    let spotify = AuthCodeSpotify::with_config(credentials, oauth, Default::default());

    let token_path = get_token_path()?;
    if token_path.exists() {
        // Load token from file
        let json = std::fs::read_to_string(token_path)?;
        *spotify.get_token().lock().unwrap() = Some(serde_json::from_str(&json)?);
    }

    Ok(spotify)
}

pub fn login(spotify: &AuthCodeSpotify) -> Result<()> {
    let url = spotify.get_authorize_url(false)?;
    println!("Please visit this URL to authorize rmpc: {}", url);

    // In a real app we'd start a local server to catch the callback,
    // but for now we can ask the user to paste the redirect URL.
    println!("Paste the redirect URL here:");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let code = spotify.parse_response_code(&input).context("Failed to parse response code")?;

    spotify.request_token(&code)?;

    let token_lock = spotify.get_token();
    let token = token_lock.lock().unwrap();
    if let Some(token) = token.as_ref() {
        let json = serde_json::to_string(token)?;
        std::fs::write(get_token_path()?, json)?;
        println!("Login successful and token saved.");
    }

    Ok(())
}

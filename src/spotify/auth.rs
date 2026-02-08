use anyhow::{Result, Context};
use rspotify::{AuthCodeSpotify, Credentials, OAuth, prelude::*, scopes};
use std::path::PathBuf;

pub const DEFAULT_REDIRECT_URI: &str = "http://localhost:8888/callback";

pub fn get_token_path() -> Result<PathBuf> {
    let mut path = crate::shared::paths::config_dir().context("Failed to get config directory")?;
    path.push("spotify_token.json");
    Ok(path)
}

pub fn get_creds_path() -> Result<PathBuf> {
    let mut path = crate::shared::paths::config_dir().context("Failed to get config directory")?;
    path.push("spotify_creds.json");
    Ok(path)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SpotifyCreds {
    client_id: String,
    client_secret: String,
}

pub fn create_spotify_client() -> Result<AuthCodeSpotify> {
    // These should ideally be configurable, but for now we need some defaults
    // or the user must provide them via env vars or config.
    // I'll check env vars first.
    let mut client_id = std::env::var("SPOTIFY_CLIENT_ID").ok();
    let mut client_secret = std::env::var("SPOTIFY_CLIENT_SECRET").ok();

    let creds_path = get_creds_path()?;
    if (client_id.is_none() || client_secret.is_none()) && creds_path.exists() {
        let json = std::fs::read_to_string(&creds_path)?;
        if let Ok(creds) = serde_json::from_str::<SpotifyCreds>(&json) {
            if client_id.is_none() { client_id = Some(creds.client_id); }
            if client_secret.is_none() { client_secret = Some(creds.client_secret); }
        }
    }

    if client_id.is_none() || client_secret.is_none() {
        println!("Spotify Client ID and Client Secret not found.");
        println!("You can create them at https://developer.spotify.com/dashboard");
        println!("Use http://localhost:8888/callback as the Redirect URI.");
        println!("Please enter your Client ID:");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let id = input.trim().to_string();

        println!("Please enter your Client Secret:");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let secret = input.trim().to_string();

        let creds = SpotifyCreds { client_id: id.clone(), client_secret: secret.clone() };
        let json = serde_json::to_string(&creds)?;
        std::fs::write(&creds_path, json)?;

        client_id = Some(id);
        client_secret = Some(secret);
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

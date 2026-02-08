use std::{io::Write, path::PathBuf, sync::Arc};

use anyhow::{Result, bail};
use itertools::Itertools;

use crate::{
    config::{
        cli::{AddRandom, Command, Provider, StickerCmd},
        cli_config::CliConfig,
    },
    ctx::Ctx,
    mpd::{
        QueuePosition,
        commands::{State, mpd_config::MpdConfig, volume::Bound},
        mpd_client::{AlbumArtOrder, Filter, Tag, ValueChange},
        version::Version,
    },
    core::player::{Player, Enqueue},
    shared::{
        args,
        ext::duration::DurationExt,
        lrc::{LrcIndex, get_lrc_path},
        macros::status_error,
        ytdlp::{self, YtDlp, YtDlpHost},
    },
};

impl Command {
    pub fn execute(
        mut self,
        config: &CliConfig,
    ) -> Result<Box<dyn FnOnce(&mut dyn Player) -> Result<()> + Send + 'static>> {
        match self {
            Command::Config { .. } => bail!("Cannot use config command here."),
            Command::Theme { .. } => bail!("Cannot use theme command here."),
            Command::Version => bail!("Cannot use version command here."),
            Command::DebugInfo => bail!("Cannot use debuginfo command here."),
            Command::Raw { .. } => bail!("Cannot use raw command here."),
            Command::Remote { .. } => bail!("Cannot use remote command here."),
            Command::AddRandom { tag, count } => Ok(Box::new(move |player| {
                match tag {
                    AddRandom::Song => {
                        player.add_random_songs(count, None)?;
                    }
                    AddRandom::Artist => {
                        player.add_random_tag(count, Tag::Artist)?;
                    }
                    AddRandom::Album => {
                        player.add_random_tag(count, Tag::Album)?;
                    }
                    AddRandom::AlbumArtist => {
                        player.add_random_tag(count, Tag::AlbumArtist)?;
                    }
                    AddRandom::Genre => {
                        player.add_random_tag(count, Tag::Genre)?;
                    }
                }
                Ok(())
            })),
            Command::Update { ref mut path, wait } | Command::Rescan { ref mut path, wait } => {
                let path = path.take();
                let is_update = matches!(self, Command::Update { .. });
                Ok(Box::new(move |player| {
                    let crate::mpd::commands::Update { job_id } = if is_update {
                        player.update(path.as_deref())?
                    } else {
                        player.rescan(path.as_deref())?
                    };

                    if wait {
                        loop {
                            player.wait_for_event()?; // Wait for next event
                            let status = player.get_status()?;
                            match status.updating_db {
                                Some(current_id) if current_id > job_id => {
                                    break;
                                }
                                Some(_id) => {}
                                None => break,
                            }
                        }
                    }
                    Ok(())
                }))
            }
            Command::LyricsIndex => {
                let lyrics_dir = config.lyrics_dir.clone();
                Ok(Box::new(|_| {
                    let Some(dir) = lyrics_dir else {
                        bail!("Lyrics dir is not configured");
                    };
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&LrcIndex::index(&PathBuf::from(dir)))?
                    );
                    Ok(())
                }))
            }
            Command::Queue => Ok(Box::new(|player| {
                let queue = player.get_queue()?;
                println!("{}", serde_json::ser::to_string(&queue)?);
                Ok(())
            })),
            Command::ListAll { files } => Ok(Box::new(move |player| {
                let mut all_songs = Vec::new();
                if files.is_empty() {
                    all_songs = player.list_all(None)?;
                } else {
                    for file in files {
                        all_songs.extend(player.list_all(Some(&file))?);
                    }
                };

                all_songs.iter().for_each(|song| println!("{}", song.file));
                Ok(())
            })),
            Command::Play { position: None } => Ok(Box::new(|player| Ok(player.play(None)?))),
            Command::Play { position: Some(pos) } => {
                Ok(Box::new(move |player| Ok(player.play_pos(pos)?)))
            }
            Command::Pause => Ok(Box::new(|player| Ok(player.pause()?))),
            Command::TogglePause => Ok(Box::new(|player| {
                let status = player.get_status()?;
                if matches!(status.state, State::Play | State::Pause) {
                    player.pause_toggle()?;
                } else {
                    player.play(None)?;
                }
                Ok(())
            })),
            Command::Unpause => Ok(Box::new(|player| Ok(player.unpause()?))),
            Command::Stop => Ok(Box::new(|player| Ok(player.stop()?))),
            Command::Volume { value: Some(value) } => {
                Ok(Box::new(move |player| Ok(player.volume(ValueChange::Set(value.parse()?))?)))
            }
            Command::Volume { value: None } => Ok(Box::new(|player| {
                println!("{}", player.get_status()?.volume.value());
                Ok(())
            })),
            Command::Next { keep_state } => Ok(Box::new(move |player| {
                let status = player.get_status()?;
                Ok(player.next_keep_state(keep_state, status.state)?)
            })),
            Command::Prev { rewind_to_start, keep_state } => Ok(Box::new(move |player| {
                let status = player.get_status()?;
                match rewind_to_start {
                    Some(value) => {
                        if status.elapsed.as_secs() >= value {
                            player.seek_current(ValueChange::Set(0))?;
                        } else {
                            player.prev_keep_state(keep_state, status.state)?;
                        }
                    }
                    None => {
                        player.prev_keep_state(keep_state, status.state)?;
                    }
                }
                Ok(())
            })),
            Command::Repeat { value } => {
                Ok(Box::new(move |player| Ok(player.set_repeat((value).into())?)))
            }
            Command::Random { value } => {
                Ok(Box::new(move |player| Ok(player.set_random((value).into())?)))
            }
            Command::Single { value } => {
                Ok(Box::new(move |player| Ok(player.set_single((value).into())?)))
            }
            Command::Consume { value } => {
                Ok(Box::new(move |player| Ok(player.set_consume((value).into())?)))
            }
            Command::ToggleRepeat => Ok(Box::new(move |player| {
                let status = player.get_status()?;
                Ok(player.set_repeat(!status.repeat)?)
            })),
            Command::ToggleRandom => Ok(Box::new(move |player| {
                let status = player.get_status()?;
                Ok(player.set_random(!status.random)?)
            })),
            Command::ToggleSingle { skip_oneshot } => Ok(Box::new(move |player| {
                let status = player.get_status()?;
                if skip_oneshot || player.version() < Version::new(0, 21, 0) {
                    player.set_single(status.single.cycle_skip_oneshot())?;
                } else {
                    player.set_single(status.single.cycle())?;
                }
                Ok(())
            })),
            Command::ToggleConsume { skip_oneshot } => Ok(Box::new(move |player| {
                let status = player.get_status()?;
                if skip_oneshot || player.version() < Version::new(0, 24, 0) {
                    player.set_consume(status.consume.cycle_skip_oneshot())?;
                } else {
                    player.set_consume(status.consume.cycle())?;
                }
                Ok(())
            })),
            Command::Seek { value } => {
                Ok(Box::new(move |player| Ok(player.seek_current(value.parse()?)?)))
            }
            Command::Clear => Ok(Box::new(|player| Ok(player.clear_queue()?))),
            Command::Add { files, skip_ext_check, position }
                if files.iter().any(|path| path.is_absolute()) =>
            {
                Ok(Box::new(move |client| {
                    let Some(MpdConfig { music_directory, .. }) = client.config() else {
                        status_error!("Cannot add absolute path without socket connection to MPD");
                        return Ok(());
                    };

                    let dir = music_directory.clone();

                    let mut files = files;

                    if !skip_ext_check {
                        let supported_extensions = client
                            .decoders()?
                            .into_iter()
                            .flat_map(|decoder| decoder.suffixes)
                            .collect_vec();

                        files = files
                            .into_iter()
                            .filter(|path| {
                                path.to_string_lossy() == "/"
                                    || path.extension().and_then(|ext| ext.to_str()).is_some_and(
                                        |ext| {
                                            supported_extensions
                                                .iter()
                                                .any(|supported_ext| supported_ext == ext)
                                        },
                                    )
                            })
                            .collect_vec();
                    }

                    if let Some(QueuePosition::Absolute(_) | QueuePosition::RelativeAdd(_)) =
                        position
                    {
                        files.reverse();
                    }
                    for file in files {
                        if file.starts_with(&dir) {
                            client.add(
                                file.to_string_lossy()
                                    .trim_start_matches(&dir)
                                    .trim_start_matches('/')
                                    .trim_end_matches('/'),
                                position,
                            )?;
                        } else {
                            client.add(&file.to_string_lossy(), position)?;
                        }
                    }

                    Ok(())
                }))
            }
            Command::Add { mut files, position, .. } => Ok(Box::new(move |client| {
                if let Some(QueuePosition::Absolute(_) | QueuePosition::RelativeAdd(_)) = position {
                    files.reverse();
                }
                for file in files {
                    client.add(&file.to_string_lossy(), position)?;
                }

                Ok(())
            })),
            Command::AddYt { url, position } => {
                let config = config.clone();
                Ok(Box::new(move |player| {
                    // This is only supported for MPD for now because it uses idle loop to wait for download
                    // To make it generic we should probably not rely on idle loop here if not MPD.
                    // But for now let's just make it call the player methods.

                    ytdlp::init_and_download(&config, &url, |path| {
                        if let Err(err) = player.add_downloaded_file_to_queue(
                            path,
                            config.cache_dir.as_deref(),
                            position,
                        ) {
                            eprintln!("Failed to add downloaded file to queue: {err}");
                        }
                        Ok(())
                    })?;

                    Ok(())
                }))
            }
            Command::SearchYt { query, provider, interactive, limit, position } => {
                let kind: YtDlpHost = provider.into();
                let chosen_url = if interactive {
                    ytdlp::search_pick_cli(kind, query.trim(), limit)?
                } else {
                    YtDlp::search(kind, query.trim(), 1)?
                        .into_iter()
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("No results found for query '{query}'"))
                        .map(|item| item.url)?
                };

                let config = config.clone();
                Ok(Box::new(move |player| {
                    ytdlp::init_and_download(&config, &chosen_url, |path| {
                        player.add_downloaded_file_to_queue(
                            path,
                            config.cache_dir.as_deref(),
                            position,
                        )?;
                        Ok(())
                    })?;

                    Ok(())
                }))
            }
            Command::Save { name } => Ok(Box::new(move |player| {
                player.save_queue_as_playlist(&name, None)?;
                Ok(())
            })),
            Command::Load { names } => Ok(Box::new(move |player| {
                for name in names {
                    player.enqueue_multiple(vec![Enqueue::Playlist { name }], None, None, false)?;
                }
                Ok(())
            })),
            Command::Decoders => Ok(Box::new(|player| {
                println!("{}", serde_json::ser::to_string(&player.decoders()?)?);
                Ok(())
            })),
            Command::Outputs => Ok(Box::new(|player| {
                println!("{}", serde_json::ser::to_string(&player.outputs()?)?);
                Ok(())
            })),
            Command::ToggleOutput { id } => {
                Ok(Box::new(move |player| Ok(player.toggle_output(id)?)))
            }
            Command::EnableOutput { id } => {
                Ok(Box::new(move |player| Ok(player.enable_output(id)?)))
            }
            Command::DisableOutput { id } => {
                Ok(Box::new(move |player| Ok(player.disable_output(id)?)))
            }
            Command::Status => Ok(Box::new(|player| {
                println!("{}", serde_json::ser::to_string(&player.get_status()?)?);
                Ok(())
            })),
            Command::Song { path: Some(paths) } if paths.len() == 1 => {
                Ok(Box::new(move |player| {
                    let path = &paths[0];
                    if let Some(song) = player.find_one(&[Filter::new(Tag::File, path.as_str())])? {
                        println!("{}", serde_json::ser::to_string(&song)?);
                        Ok(())
                    } else {
                        println!("Song with path '{path}' not found.");
                        std::process::exit(1);
                    }
                }))
            }
            Command::Song { path: Some(paths) } => Ok(Box::new(move |player| {
                let mut songs = Vec::new();
                for path in &paths {
                    if let Some(song) = player.find_one(&[Filter::new(Tag::File, path.as_str())])? {
                        songs.push(song);
                    } else {
                        println!("Song with path '{path}' not found.");
                        std::process::exit(1);
                    }
                }
                println!("{}", serde_json::ser::to_string(&songs)?);
                Ok(())
            })),
            Command::Song { path: None } => Ok(Box::new(|player| {
                let current_song = player.get_current_song()?;
                if let Some(song) = current_song {
                    println!("{}", serde_json::ser::to_string(&song)?);
                    Ok(())
                } else {
                    std::process::exit(1);
                }
            })),
            Command::Mount { name, path } => {
                Ok(Box::new(move |player| Ok(player.mount(&name, &path)?)))
            }
            Command::Unmount { name } => Ok(Box::new(move |player| Ok(player.unmount(&name)?))),
            Command::ListMounts => Ok(Box::new(|player| {
                println!("{}", serde_json::ser::to_string(&player.list_mounts()?)?);
                Ok(())
            })),
            Command::ListPartitions => Ok(Box::new(|player| {
                println!("{}", serde_json::ser::to_string(&player.list_partitions()?)?);
                Ok(())
            })),
            Command::AlbumArt { output } => Ok(Box::new(move |player| {
                let Some(song) = player.get_current_song()? else {
                    std::process::exit(3);
                };

                let album_art = player.find_album_art(&song.file, AlbumArtOrder::EmbeddedFirst)?;

                let Some(album_art) = album_art else {
                    std::process::exit(2);
                };

                if &output == "-" {
                    std::io::stdout().write_all(&album_art)?;
                    std::io::stdout().flush()?;
                    Ok(())
                } else {
                    std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(output)?
                        .write_all(&album_art)?;
                    Ok(())
                }
            })),
            Command::Sticker { cmd: StickerCmd::Set { uri, key, value } } => {
                Ok(Box::new(move |player| {
                    player.set_sticker(&uri, &key, &value)?;
                    Ok(())
                }))
            }
            Command::Sticker { cmd: StickerCmd::Get { uri, key } } => Ok(Box::new(move |player| {
                match player.sticker(&uri, &key)? {
                    Some(sticker) => {
                        println!("{}", serde_json::ser::to_string(&sticker)?);
                    }
                    None => {
                        std::process::exit(1);
                    }
                }
                Ok(())
            })),
            Command::Sticker { cmd: StickerCmd::Delete { uri, key } } => {
                Ok(Box::new(move |player| {
                    player.delete_sticker(&uri, &key)?;
                    Ok(())
                }))
            }
            Command::Sticker { cmd: StickerCmd::DeleteAll { uri } } => {
                Ok(Box::new(move |player| {
                    player.delete_all_stickers(&uri)?;
                    Ok(())
                }))
            }
            Command::Sticker { cmd: StickerCmd::List { uri } } => Ok(Box::new(move |player| {
                let stickers = player.list_stickers(&uri)?;
                println!("{}", serde_json::ser::to_string(&stickers)?);
                Ok(())
            })),
            Command::Sticker { cmd: StickerCmd::Find { uri, key } } => {
                Ok(Box::new(move |player| {
                    let stickers = player.find_stickers(&uri, &key, None)?;
                    println!("{}", serde_json::ser::to_string(&stickers)?);
                    Ok(())
                }))
            }
            Command::SendMessage { channel, content } => Ok(Box::new(move |player| {
                player.send_message(&channel, &content)?;
                Ok(())
            })),
            Command::SpotifyLogin => bail!("SpotifyLogin should be handled in main."),
        }
    }
}

impl From<Provider> for YtDlpHost {
    fn from(p: Provider) -> Self {
        match p {
            Provider::Youtube => YtDlpHost::Youtube,
            Provider::Soundcloud => YtDlpHost::Soundcloud,
            Provider::Nicovideo => YtDlpHost::NicoVideo,
        }
    }
}

pub fn run_external_blocking<'a, E>(
    command: &[String],
    command_args: &[String],
    envs: E,
) -> Result<()>
where
    E: IntoIterator<Item = (&'a str, &'a str)> + std::fmt::Debug,
{
    let [cmd, args @ ..] = command else {
        bail!("Invalid command: {command:?}");
    };

    let (mut used_count, cmd) = args::replace_arg_placeholder(cmd, command_args)?;
    let mut cmd = std::process::Command::new(cmd);

    for arg in args {
        if used_count > command_args.len() {
            bail!("Not enough arguments provided for command");
        }

        let (arg_used_count, arg) =
            args::replace_arg_placeholder(arg, &command_args[used_count..])?;
        used_count += arg_used_count;
        cmd.arg(arg);
    }

    if used_count < command_args.len() {
        bail!("Too many arguments provided for command");
    }

    for (key, val) in envs {
        cmd.env(key, val);
    }

    log::debug!(cmd:? = cmd.get_program(), args:? = cmd.get_args(); "Running external command");
    log::trace!(cmd:?, envs:? = cmd.get_envs(); "Running external command");

    let out = match cmd.output() {
        Ok(out) => out,
        Err(err) => {
            bail!("Unexpected error when executing external command: {err:?}");
        }
    };

    if !out.status.success() {
        bail!(
            "External command failed: exit code: '{}', stdout: '{}', stderr: '{}'",
            out.status.code().map_or_else(|| "-".to_string(), |v| v.to_string()),
            String::from_utf8_lossy(&out.stdout).trim(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }

    Ok(())
}

pub fn run_external<K: Into<String>, V: Into<String>>(
    command: Arc<Vec<String>>,
    command_args: Vec<String>,
    envs: Vec<(K, V)>,
) {
    let envs = envs.into_iter().map(|(k, v)| (k.into(), v.into())).collect_vec();

    std::thread::spawn(move || {
        if let Err(err) = run_external_blocking(
            command.as_slice(),
            &command_args,
            envs.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        ) {
            status_error!("{}", err);
        }
    });
}

pub fn create_env<'a>(
    ctx: &Ctx,
    selected_songs_paths: impl IntoIterator<Item = &'a str>,
) -> Vec<(String, String)> {
    let mut result = Vec::new();

    if let Some((_, current)) = ctx.find_current_song_in_queue() {
        result.push(("CURRENT_SONG".to_owned(), current.file.clone()));
        result.extend(
            current.metadata.iter().map(|(k, v)| (k.to_ascii_uppercase(), v.last().to_owned())),
        );
        let lrc_path = ctx
            .config
            .lyrics_dir
            .as_ref()
            .and_then(|dir| get_lrc_path(dir, &current.file).ok())
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        let lrc = ctx.find_lrc().ok().flatten();
        let duration = current.duration.map_or_else(String::new, |d| d.to_string());
        result.push(("DURATION".to_owned(), duration));
        result.push(("HAS_LRC".to_owned(), lrc.is_some().to_string()));
        result.push(("LRC_FILE".to_owned(), lrc_path));
        result.push(("FILE".to_owned(), current.file.clone()));
    }
    result.push(("PID".to_owned(), std::process::id().to_string()));

    let songs =
        selected_songs_paths.into_iter().enumerate().fold(String::new(), |mut acc, (idx, val)| {
            if idx > 0 {
                acc.push('\n');
            }
            acc.push_str(val);
            acc
        });

    if !songs.is_empty() {
        result.push(("SELECTED_SONGS".to_owned(), songs));
    }
    result.push(("VERSION".to_owned(), env!("CARGO_PKG_VERSION").to_string()));

    result.push(("STATE".to_owned(), ctx.status.state.to_string()));

    result
}

use std::collections::{HashSet, HashMap};
use std::time::Duration;
use anyhow::{Result, Context, bail};
use crate::mpd::commands::{Song, Status, Playlist, LsInfo, Decoder, Mounts, State, idle::IdleEvent, metadata_tag::MetadataTag};
use crate::mpd::QueuePosition;
use crate::mpd::mpd_client::{ValueChange, Tag, Filter, AlbumArtOrder, SingleOrRange, SaveMode};
use crate::mpd::commands::status::OnOffOneshot;
use crate::mpd::version::Version;
use crate::core::player::{Player, PlayerBreakHandle, Enqueue, PlayerDelete};
use std::sync::Arc;
use tokio::runtime::Runtime;
use librespot::core::{Session, SessionConfig};
use librespot::core::authentication::Credentials;
use librespot::core::SpotifyUri;
use librespot::playback::player::{Player as LibrespotPlayer, PlayerEvent};
use librespot::playback::config::PlayerConfig;
#[cfg(feature = "spotify-rodio")]
use librespot::playback::config::AudioFormat;
use librespot::playback::mixer::NoOpVolume;
use librespot::playback::audio_backend::{Sink, SinkResult};
use librespot::playback::decoder::AudioPacket;
use librespot::playback::convert::Converter;
use rspotify::{AuthCodeSpotify, prelude::*};
use tokio::sync::mpsc;

struct DummySink;
impl Sink for DummySink {
    fn start(&mut self) -> SinkResult<()> { Ok(()) }
    fn stop(&mut self) -> SinkResult<()> { Ok(()) }
    fn write(&mut self, _packet: AudioPacket, _converter: &mut Converter) -> SinkResult<()> { Ok(()) }
}

pub struct SpotifyClient {
    #[allow(dead_code)]
    runtime: Runtime,
    #[allow(dead_code)]
    session: Session,
    player: Arc<LibrespotPlayer>,
    event_rx: mpsc::UnboundedReceiver<PlayerEvent>,
    spotify: AuthCodeSpotify,
    queue: Vec<Song>,
    current_pos: Option<usize>,
    volume: u32,
    next_id: u32,
    state: State,
    elapsed: Duration,
    duration: Option<Duration>,
}

impl SpotifyClient {
    pub fn init() -> Result<Self> {
        let runtime = Runtime::new().context("Failed to create tokio runtime")?;

        let spotify = crate::spotify::auth::create_spotify_client()?;

        let token_lock = spotify.get_token();
        let token = token_lock.lock().unwrap();
        let token = token.as_ref().context("Spotify not logged in. Run 'rmpc spotifylogin' first.")?;

        let (session, player, event_rx) = runtime.block_on(async {
            let session_config = SessionConfig::default();
            let player_config = PlayerConfig::default();

            let credentials = Credentials::with_access_token(token.access_token.clone());

            let session = Session::new(session_config, None);
            session.connect(credentials, false).await.context("Failed to connect librespot")?;

            #[cfg(feature = "spotify-rodio")]
            let backend = || librespot::playback::audio_backend::rodio::mk_rodio(None, AudioFormat::default());
            #[cfg(not(feature = "spotify-rodio"))]
            let backend = || Box::new(DummySink) as Box<dyn Sink>;

            let player = LibrespotPlayer::new(player_config, session.clone(), Box::new(NoOpVolume), backend);
            let event_rx = player.get_player_event_channel();

            Ok::<_, anyhow::Error>((session, player, event_rx))
        })?;

        let _ = token;

        Ok(Self {
            runtime,
            session,
            player,
            event_rx,
            spotify,
            queue: Vec::new(),
            current_pos: None,
            volume: 50,
            next_id: 1,
            state: State::Stop,
            elapsed: Duration::default(),
            duration: None,
        })
    }
}

impl Player for SpotifyClient {
    fn name(&self) -> &str {
        "spotify"
    }

    fn play(&mut self, pos: Option<usize>) -> Result<()> {
        if let Some(pos) = pos {
            self.play_pos(pos)?;
        } else {
            self.player.play();
            self.state = State::Play;
        }
        Ok(())
    }

    fn pause(&mut self) -> Result<()> {
        self.player.pause();
        self.state = State::Pause;
        Ok(())
    }

    fn pause_toggle(&mut self) -> Result<()> {
        match self.state {
            State::Play => self.pause()?,
            State::Pause | State::Stop => self.unpause()?,
        }
        Ok(())
    }

    fn unpause(&mut self) -> Result<()> {
        self.player.play();
        self.state = State::Play;
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        self.player.stop();
        self.state = State::Stop;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(pos) = self.current_pos {
            if pos + 1 < self.queue.len() {
                self.play_pos(pos + 1)?;
            }
        }
        Ok(())
    }

    fn prev(&mut self) -> Result<()> {
        if let Some(pos) = self.current_pos {
            if pos > 0 {
                self.play_pos(pos - 1)?;
            }
        }
        Ok(())
    }

    fn seek_current(&mut self, value: ValueChange) -> Result<()> {
        let current_ms = self.elapsed.as_millis() as u32;
        let new_ms = match value {
            ValueChange::Set(v) => v * 1000,
            ValueChange::Increase(v) => current_ms + v * 1000,
            ValueChange::Decrease(v) => current_ms.saturating_sub(v * 1000),
        };
        self.player.seek(new_ms);
        Ok(())
    }

    fn get_status(&mut self) -> Result<Status> {
        self.poll_events();
        let mut status = Status::default();
        status.volume = crate::mpd::commands::Volume::new(self.volume);
        status.state = self.state;
        status.song = self.current_pos;
        status.songid = self.current_pos.and_then(|p| self.queue.get(p)).map(|s| s.id);
        status.elapsed = self.elapsed;
        status.duration = self.duration.unwrap_or_default();
        Ok(status)
    }

    fn get_current_song(&mut self) -> Result<Option<Song>> {
        Ok(self.current_pos.and_then(|p| self.queue.get(p).cloned()))
    }

    fn get_queue(&mut self) -> Result<Vec<Song>> {
        Ok(self.queue.clone())
    }

    fn add(&mut self, uri: &str, _position: Option<QueuePosition>) -> Result<()> {
        let track_id = rspotify::model::TrackId::from_uri(uri).context("Invalid Spotify URI")?;
        let track = self.spotify.track(track_id, None)?;

        let song = Song {
            id: self.next_id,
            file: uri.to_string(),
            duration: Some(track.duration.to_std().unwrap_or_default()),
            metadata: {
                let mut m = HashMap::new();
                m.insert("title".to_string(), MetadataTag::Single(track.name));
                m.insert("artist".to_string(), MetadataTag::Multiple(track.artists.into_iter().map(|a| a.name).collect::<Vec<_>>()));
                m.insert("album".to_string(), MetadataTag::Single(track.album.name));
                m
            },
            ..Default::default()
        };
        self.next_id += 1;
        self.queue.push(song);
        Ok(())
    }

    fn clear_queue(&mut self) -> Result<()> {
        self.queue.clear();
        self.current_pos = None;
        self.player.stop();
        Ok(())
    }

    fn volume(&mut self, volume: ValueChange) -> Result<()> {
        let new_volume = match volume {
            ValueChange::Set(v) => v,
            ValueChange::Increase(v) => self.volume.saturating_add(v),
            ValueChange::Decrease(v) => self.volume.saturating_sub(v),
        }.clamp(0, 100);

        self.volume = new_volume;
        let _librespot_vol = (new_volume as f64 / 100.0 * 65535.0) as u16;
        // self.player.set_volume(_librespot_vol); // librespot 0.4+ uses u16
        Ok(())
    }

    fn crossfade(&mut self, _seconds: u32) -> Result<()> {
        Ok(())
    }

    fn set_repeat(&mut self, _enabled: bool) -> Result<()> {
        todo!()
    }

    fn set_random(&mut self, _enabled: bool) -> Result<()> {
        todo!()
    }

    fn set_single(&mut self, _single: OnOffOneshot) -> Result<()> {
        Ok(())
    }

    fn set_consume(&mut self, _consume: OnOffOneshot) -> Result<()> {
        Ok(())
    }

    fn delete_multiple(&mut self, _items: Vec<PlayerDelete>) -> Result<()> {
        todo!()
    }

    fn create_playlist(&mut self, _name: &str, _items: Vec<String>) -> Result<()> {
        todo!()
    }

    fn add_to_playlist(&mut self, _playlist_name: &str, _uri: &str, _position: Option<usize>) -> Result<()> {
        todo!()
    }

    fn add_to_playlist_multiple(&mut self, _playlist_name: &str, _song_paths: Vec<String>) -> Result<()> {
        todo!()
    }

    fn get_songs_info(&mut self, _song_uris: Vec<String>) -> Result<Vec<Song>> {
        Ok(Vec::new())
    }

    fn fetch_song_stickers(&mut self, _song_uris: Vec<String>) -> Result<HashMap<String, HashMap<String, String>>> {
        Ok(HashMap::new())
    }

    fn set_sticker_multiple(&mut self, _key: &str, _value: String, _items: Vec<Enqueue>) -> Result<()> {
        Ok(())
    }

    fn delete_sticker_multiple(&mut self, _key: &str, _items: Vec<Enqueue>) -> Result<()> {
        Ok(())
    }

    fn find(&mut self, filter: &[Filter<'_>]) -> Result<Vec<Song>> {
        self.search(filter, false)
    }

    fn search(&mut self, filter: &[Filter<'_>], _ignore_diacritics: bool) -> Result<Vec<Song>> {
        let mut query = String::new();
        for f in filter {
            if !query.is_empty() { query.push(' '); }
            query.push_str(&f.value);
        }

        let result = self.spotify.search(&query, rspotify::model::SearchType::Track, None, None, Some(50), None)?;

        let mut songs = Vec::new();
        if let rspotify::model::SearchResult::Tracks(tracks) = result {
            for track in tracks.items {
                songs.push(Song {
                    id: 0, // Not in queue
                    file: track.id.as_ref().map(|id| id.uri()).unwrap_or_default(),
                    duration: Some(track.duration.to_std().unwrap_or_default()),
                    metadata: {
                        let mut m = HashMap::new();
                        m.insert("title".to_string(), MetadataTag::Single(track.name));
                        m.insert("artist".to_string(), MetadataTag::Multiple(track.artists.into_iter().map(|a| a.name).collect::<Vec<_>>()));
                        m.insert("album".to_string(), MetadataTag::Single(track.album.name));
                        m
                    },
                    ..Default::default()
                });
            }
        }

        Ok(songs)
    }

    fn list_tag(&mut self, _tag: Tag, _filter: Option<&[Filter<'_>]>) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn list_all(&mut self, _path: Option<&str>) -> Result<Vec<Song>> {
        Ok(Vec::new())
    }

    fn lsinfo(&mut self, _path: Option<&str>) -> Result<LsInfo> {
        Ok(LsInfo::default())
    }

    fn list_playlists(&mut self) -> Result<Vec<Playlist>> {
        let playlists = self.spotify.current_user_playlists();
        let mut result = Vec::new();
        for p in playlists {
            let p = p?;
            result.push(Playlist {
                name: p.name,
                ..Default::default()
            });
        }
        Ok(result)
    }

    fn list_playlist_info(&mut self, name: &str, _range: Option<SingleOrRange>) -> Result<Vec<Song>> {
        let playlists = self.spotify.current_user_playlists();
        let mut playlist_id = None;
        for p in playlists {
            let p = p?;
            if p.name == name {
                playlist_id = Some(p.id.clone());
                break;
            }
        }

        let id = playlist_id.context("Playlist not found")?;
        let items = self.spotify.playlist_items(id, None, None);

        let mut songs = Vec::new();
        for item in items {
            let item = item?;
            if let Some(rspotify::model::PlayableItem::Track(track)) = item.track {
                songs.push(Song {
                    id: 0,
                    file: track.id.as_ref().map(|id: &rspotify::model::TrackId| id.uri()).unwrap_or_default(),
                    duration: Some(track.duration.to_std().unwrap_or_default()),
                    metadata: {
                        let mut m = HashMap::new();
                        m.insert("title".to_string(), MetadataTag::Single(track.name));
                        m.insert("artist".to_string(), MetadataTag::Multiple(track.artists.into_iter().map(|a| a.name).collect::<Vec<_>>()));
                        m.insert("album".to_string(), MetadataTag::Single(track.album.name));
                        m
                    },
                    ..Default::default()
                });
            }
        }
        Ok(songs)
    }

    fn rename_playlist(&mut self, _current_name: &str, _new_name: &str) -> Result<()> {
        todo!()
    }

    fn move_in_playlist(&mut self, _playlist: &str, _range: &SingleOrRange, _new_idx: usize) -> Result<()> {
        todo!()
    }

    fn move_output(&mut self, _name: &str) -> Result<()> {
        Ok(())
    }

    fn switch_to_partition(&mut self, _name: &str) -> Result<()> {
        Ok(())
    }

    fn new_partition(&mut self, _name: &str) -> Result<()> {
        Ok(())
    }

    fn delete_partition(&mut self, _name: &str) -> Result<()> {
        Ok(())
    }

    fn switch_to_partition_with_autocreate(&mut self, _name: &str, _autocreate: bool) -> Result<()> {
        Ok(())
    }

    fn create_and_switch_to_partition(&mut self, _name: &str) -> Result<()> {
        Ok(())
    }

    fn update(&mut self, _path: Option<&str>) -> Result<crate::mpd::commands::Update> {
        Ok(crate::mpd::commands::Update { job_id: 0 })
    }

    fn rescan(&mut self, _path: Option<&str>) -> Result<crate::mpd::commands::Update> {
        Ok(crate::mpd::commands::Update { job_id: 0 })
    }

    fn next_keep_state(&mut self, _keep: bool, _state: State) -> Result<()> {
        todo!()
    }

    fn prev_keep_state(&mut self, _keep: bool, _state: State) -> Result<()> {
        todo!()
    }

    fn add_random_songs(&mut self, _count: usize, _filter: Option<&[Filter<'_>]>) -> Result<()> {
        Ok(())
    }

    fn add_random_tag(&mut self, _count: usize, _tag: Tag) -> Result<()> {
        Ok(())
    }

    fn decoders(&mut self) -> Result<Vec<Decoder>> {
        Ok(Vec::new())
    }

    fn outputs(&mut self) -> Result<crate::mpd::commands::outputs::Outputs> {
        Ok(crate::mpd::commands::outputs::Outputs(Vec::new()))
    }

    fn list_partitioned_outputs(&mut self, _current_partition: &str) -> Result<Vec<crate::shared::mpd_client_ext::PartitionedOutput>> {
        Ok(Vec::new())
    }

    fn toggle_output(&mut self, _id: u32) -> Result<()> {
        Ok(())
    }

    fn toggle_output_kind(&mut self, _name: &str, _id: u32, _kind: crate::shared::mpd_client_ext::PartitionedOutputKind) -> Result<()> {
        Ok(())
    }

    fn enable_output(&mut self, _id: u32) -> Result<()> {
        Ok(())
    }

    fn disable_output(&mut self, _id: u32) -> Result<()> {
        Ok(())
    }

    fn find_one(&mut self, _filter: &[Filter<'_>]) -> Result<Option<Song>> {
        Ok(None)
    }

    fn mount(&mut self, _name: &str, _path: &str) -> Result<()> {
        Ok(())
    }

    fn unmount(&mut self, _name: &str) -> Result<()> {
        Ok(())
    }

    fn list_mounts(&mut self) -> Result<Mounts> {
        Ok(Mounts::default())
    }

    fn list_partitions(&mut self) -> Result<Vec<String>> {
        Ok(vec!["default".to_string()])
    }

    fn find_album_art(&mut self, _path: &str, _order: AlbumArtOrder) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn sticker(&mut self, _uri: &str, _key: &str) -> Result<Option<crate::mpd::commands::stickers::Sticker>> {
        Ok(None)
    }

    fn set_sticker(&mut self, _uri: &str, _key: &str, _value: &str) -> Result<()> {
        Ok(())
    }

    fn delete_sticker(&mut self, _uri: &str, _key: &str) -> Result<()> {
        Ok(())
    }

    fn delete_all_stickers(&mut self, _uri: &str) -> Result<()> {
        Ok(())
    }

    fn list_stickers(&mut self, _uri: &str) -> Result<crate::mpd::commands::stickers::Stickers> {
        Ok(crate::mpd::commands::stickers::Stickers(HashMap::new()))
    }

    fn find_stickers(&mut self, _uri: &str, _key: &str, _filter: Option<crate::mpd::mpd_client::StickerFilter>) -> Result<crate::mpd::commands::stickers::StickersWithFile> {
        Ok(crate::mpd::commands::stickers::StickersWithFile(Vec::new()))
    }

    fn send_message(&mut self, _channel: &str, _content: &str) -> Result<()> {
        Ok(())
    }

    fn delete_id(&mut self, id: u32) -> Result<()> {
        if let Some(pos) = self.queue.iter().position(|s| s.id == id) {
            self.queue.remove(pos);
            if self.current_pos == Some(pos) {
                self.current_pos = None;
                self.player.stop();
            } else if let Some(p) = self.current_pos {
                if p > pos {
                    self.current_pos = Some(p - 1);
                }
            }
        }
        Ok(())
    }

    fn play_id(&mut self, id: u32) -> Result<()> {
        let (idx, song) = self.queue.iter().enumerate().find(|(_, s)| s.id == id).context("Song not found")?;
        let uri = SpotifyUri::from_uri(&song.file).context("Invalid Spotify URI")?;
        self.player.load(uri, true, 0);
        self.current_pos = Some(idx);
        Ok(())
    }

    fn move_id(&mut self, _id: u32, _to: QueuePosition) -> Result<()> {
        todo!()
    }

    fn move_in_queue(&mut self, _from: SingleOrRange, _to: QueuePosition) -> Result<()> {
        todo!()
    }

    fn swap_positions(&mut self, _swaps: Vec<(usize, usize)>) -> Result<()> {
        todo!()
    }

    fn delete_from_queue(&mut self, songs: SingleOrRange) -> Result<()> {
        if let Some(end) = songs.end {
            let end = end.min(self.queue.len());
            if songs.start < end {
                self.queue.drain(songs.start..end);
            }
        } else {
            if songs.start < self.queue.len() {
                self.queue.remove(songs.start);
            }
        }
        Ok(())
    }

    fn shuffle(&mut self, _range: Option<SingleOrRange>) -> Result<()> {
        todo!()
    }

    fn save_queue_as_playlist(&mut self, _name: &str, _mode: Option<SaveMode>) -> Result<()> {
        Ok(())
    }

    fn enqueue_multiple(&mut self, items: Vec<Enqueue>, autoplay_idx: Option<usize>, _position: Option<QueuePosition>, replace: bool) -> Result<()> {
        if replace {
            self.clear_queue()?;
        }
        let start_idx = self.queue.len();
        for item in items {
            match item {
                Enqueue::File { path } => { self.add(&path, None)?; }
                Enqueue::Playlist { name } => { self.add(&name, None)?; }
                Enqueue::Find { .. } => { }
            }
        }
        if let Some(idx) = autoplay_idx {
            self.play_pos(start_idx + idx)?;
        }
        Ok(())
    }

    fn play_pos(&mut self, pos: usize) -> Result<()> {
        let song = self.queue.get(pos).context("Song not found at position")?;
        let uri = SpotifyUri::from_uri(&song.file).context("Invalid Spotify URI")?;
        self.player.load(uri, true, 0);
        self.current_pos = Some(pos);
        Ok(())
    }

    fn add_downloaded_file_to_queue(&mut self, _path: std::path::PathBuf, _cache_dir: Option<&std::path::Path>, _position: Option<QueuePosition>) -> Result<()> {
        bail!("Spotify backend does not support adding downloaded files")
    }

    fn wait_for_event(&mut self) -> Result<Vec<IdleEvent>> {
        // Spotify event loop
        // Poll for events for up to 100ms
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_millis(100) {
            if self.poll_events() {
                return Ok(vec![IdleEvent::Player]); // Notify UI of change
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(Vec::new())
    }

    fn get_break_handle(&self) -> Result<Box<dyn PlayerBreakHandle>> {
        Ok(Box::new(SpotifyBreakHandle {}))
    }

    fn set_read_timeout(&mut self, _timeout: Option<Duration>) -> Result<()> {
        Ok(())
    }

    fn set_write_timeout(&mut self, _timeout: Option<Duration>) -> Result<()> {
        Ok(())
    }

    fn reconnect(&mut self) -> Result<()> {
        Ok(())
    }

    fn supported_commands(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn version(&self) -> Version {
        Version::new(0, 1, 0)
    }

    fn config(&self) -> Option<crate::mpd::commands::mpd_config::MpdConfig> {
        None
    }
}

impl SpotifyClient {
    fn poll_events(&mut self) -> bool {
        let mut changed = false;
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                PlayerEvent::Playing { position_ms, .. } => {
                    self.state = State::Play;
                    self.elapsed = Duration::from_millis(position_ms as u64);
                    changed = true;
                }
                PlayerEvent::Paused { position_ms, .. } => {
                    self.state = State::Pause;
                    self.elapsed = Duration::from_millis(position_ms as u64);
                    changed = true;
                }
                PlayerEvent::Stopped { .. } => {
                    self.state = State::Stop;
                    changed = true;
                }
                PlayerEvent::EndOfTrack { .. } => {
                    let _ = self.next();
                    changed = true;
                }
                _ => {}
            }
        }
        changed
    }

}

pub struct SpotifyBreakHandle {}
impl PlayerBreakHandle for SpotifyBreakHandle {
    fn break_idle(&self) -> Result<()> {
        Ok(())
    }
}

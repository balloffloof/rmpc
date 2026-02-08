use std::collections::{HashSet, HashMap};
use std::time::Duration;
use anyhow::Result;
use crate::mpd::commands::{Song, Status, Playlist, LsInfo, Decoder, Mounts, State, idle::IdleEvent};
use crate::mpd::QueuePosition;
use crate::mpd::mpd_client::{ValueChange, Tag, FilterKind, Filter, AlbumArtOrder};
use crate::mpd::commands::status::OnOffOneshot;
use crate::mpd::version::Version;
use crate::mpd::mpd_client::SingleOrRange;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum PlayerDelete {
    SongInPlaylist { playlist: Arc<str>, range: SingleOrRange },
    Playlist { name: String },
}

#[derive(Debug, Clone)]
pub enum Enqueue {
    File { path: String },
    Playlist { name: String },
    #[allow(dead_code)]
    Find { filter: Vec<(Tag, FilterKind, String)> },
}

pub trait Player: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &str;

    fn play(&mut self, pos: Option<usize>) -> Result<()>;
    fn pause(&mut self) -> Result<()>;
    fn pause_toggle(&mut self) -> Result<()>;
    fn unpause(&mut self) -> Result<()>;
    fn stop(&mut self) -> Result<()>;
    #[allow(dead_code)]
    fn next(&mut self) -> Result<()>;
    #[allow(dead_code)]
    fn prev(&mut self) -> Result<()>;
    fn seek_current(&mut self, value: ValueChange) -> Result<()>;
    fn get_status(&mut self) -> Result<Status>;
    fn get_current_song(&mut self) -> Result<Option<Song>>;
    fn get_queue(&mut self) -> Result<Vec<Song>>;
    fn add(&mut self, uri: &str, position: Option<QueuePosition>) -> Result<()>;
    fn clear_queue(&mut self) -> Result<()>;
    fn volume(&mut self, volume: ValueChange) -> Result<()>;
    fn crossfade(&mut self, seconds: u32) -> Result<()>;
    fn set_repeat(&mut self, enabled: bool) -> Result<()>;
    fn set_random(&mut self, enabled: bool) -> Result<()>;
    fn set_single(&mut self, single: OnOffOneshot) -> Result<()>;
    fn set_consume(&mut self, consume: OnOffOneshot) -> Result<()>;

    fn enqueue_multiple(&mut self, items: Vec<Enqueue>, autoplay_idx: Option<usize>, position: Option<QueuePosition>, replace: bool) -> Result<()>;
    fn delete_multiple(&mut self, items: Vec<PlayerDelete>) -> Result<()>;
    fn create_playlist(&mut self, name: &str, items: Vec<String>) -> Result<()>;
    fn add_to_playlist(&mut self, playlist_name: &str, uri: &str, position: Option<usize>) -> Result<()>;
    fn add_to_playlist_multiple(&mut self, playlist_name: &str, song_paths: Vec<String>) -> Result<()>;
    fn get_songs_info(&mut self, song_uris: Vec<String>) -> Result<Vec<Song>>;
    fn fetch_song_stickers(&mut self, song_uris: Vec<String>) -> Result<HashMap<String, HashMap<String, String>>>;
    fn set_sticker_multiple(&mut self, key: &str, value: String, items: Vec<Enqueue>) -> Result<()>;
    fn delete_sticker_multiple(&mut self, key: &str, items: Vec<Enqueue>) -> Result<()>;

    fn find(&mut self, filter: &[Filter<'_>]) -> Result<Vec<Song>>;
    fn search(&mut self, filter: &[Filter<'_>], ignore_diacritics: bool) -> Result<Vec<Song>>;
    fn list_tag(&mut self, tag: Tag, filter: Option<&[Filter<'_>]>) -> Result<Vec<String>>;
    fn list_all(&mut self, path: Option<&str>) -> Result<Vec<Song>>;
    fn lsinfo(&mut self, path: Option<&str>) -> Result<LsInfo>;
    fn list_playlists(&mut self) -> Result<Vec<Playlist>>;
    fn list_playlist_info(&mut self, name: &str, range: Option<SingleOrRange>) -> Result<Vec<Song>>;
    fn rename_playlist(&mut self, current_name: &str, new_name: &str) -> Result<()>;
    fn move_in_playlist(&mut self, playlist: &str, range: &SingleOrRange, new_idx: usize) -> Result<()>;
    #[allow(dead_code)]
    fn move_output(&mut self, name: &str) -> Result<()>;
    fn switch_to_partition(&mut self, name: &str) -> Result<()>;
    #[allow(dead_code)]
    fn new_partition(&mut self, name: &str) -> Result<()>;
    fn delete_partition(&mut self, name: &str) -> Result<()>;
    fn switch_to_partition_with_autocreate(&mut self, name: &str, autocreate: bool) -> Result<()>;
    fn create_and_switch_to_partition(&mut self, name: &str) -> Result<()>;
    fn update(&mut self, path: Option<&str>) -> Result<crate::mpd::commands::Update>;
    fn rescan(&mut self, path: Option<&str>) -> Result<crate::mpd::commands::Update>;
    fn next_keep_state(&mut self, keep: bool, state: State) -> Result<()>;
    fn prev_keep_state(&mut self, keep: bool, state: State) -> Result<()>;

    fn add_random_songs(&mut self, count: usize, filter: Option<&[Filter<'_>]>) -> Result<()>;
    fn add_random_tag(&mut self, count: usize, tag: Tag) -> Result<()>;
    fn decoders(&mut self) -> Result<Vec<Decoder>>;
    fn outputs(&mut self) -> Result<crate::mpd::commands::outputs::Outputs>;
    fn list_partitioned_outputs(&mut self, current_partition: &str) -> Result<Vec<crate::shared::mpd_client_ext::PartitionedOutput>>;
    fn toggle_output(&mut self, id: u32) -> Result<()>;
    fn toggle_output_kind(&mut self, name: &str, id: u32, kind: crate::shared::mpd_client_ext::PartitionedOutputKind) -> Result<()>;
    fn enable_output(&mut self, id: u32) -> Result<()>;
    fn disable_output(&mut self, id: u32) -> Result<()>;
    fn find_one(&mut self, filter: &[Filter<'_>]) -> Result<Option<Song>>;
    fn mount(&mut self, name: &str, path: &str) -> Result<()>;
    fn unmount(&mut self, name: &str) -> Result<()>;
    fn list_mounts(&mut self) -> Result<Mounts>;
    fn list_partitions(&mut self) -> Result<Vec<String>>;
    fn find_album_art(&mut self, path: &str, order: AlbumArtOrder) -> Result<Option<Vec<u8>>>;
    fn sticker(&mut self, uri: &str, key: &str) -> Result<Option<crate::mpd::commands::stickers::Sticker>>;
    fn set_sticker(&mut self, uri: &str, key: &str, value: &str) -> Result<()>;
    fn delete_sticker(&mut self, uri: &str, key: &str) -> Result<()>;
    fn delete_all_stickers(&mut self, uri: &str) -> Result<()>;
    fn list_stickers(&mut self, uri: &str) -> Result<crate::mpd::commands::stickers::Stickers>;
    fn find_stickers(&mut self, uri: &str, key: &str, filter: Option<crate::mpd::mpd_client::StickerFilter>) -> Result<crate::mpd::commands::stickers::StickersWithFile>;
    fn send_message(&mut self, channel: &str, content: &str) -> Result<()>;
    fn delete_id(&mut self, id: u32) -> Result<()>;
    fn play_id(&mut self, id: u32) -> Result<()>;
    fn move_id(&mut self, id: u32, to: QueuePosition) -> Result<()>;
    fn move_in_queue(&mut self, from: SingleOrRange, to: QueuePosition) -> Result<()>;
    fn swap_positions(&mut self, swaps: Vec<(usize, usize)>) -> Result<()>;
    fn delete_from_queue(&mut self, songs: SingleOrRange) -> Result<()>;
    fn shuffle(&mut self, range: Option<SingleOrRange>) -> Result<()>;
    fn save_queue_as_playlist(&mut self, name: &str, mode: Option<crate::mpd::mpd_client::SaveMode>) -> Result<()>;
    fn play_pos(&mut self, pos: usize) -> Result<()>;
    fn add_downloaded_file_to_queue(&mut self, path: std::path::PathBuf, cache_dir: Option<&std::path::Path>, position: Option<QueuePosition>) -> Result<()>;

    fn wait_for_event(&mut self) -> Result<Vec<IdleEvent>>;
    fn get_break_handle(&self) -> Result<Box<dyn PlayerBreakHandle>>;
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<()>;
    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<()>;
    fn reconnect(&mut self) -> Result<()>;

    fn supported_commands(&self) -> HashSet<String>;
    fn version(&self) -> Version;
    fn config(&self) -> Option<crate::mpd::commands::mpd_config::MpdConfig>;
}

pub trait PlayerBreakHandle: Send + Sync {
    fn break_idle(&self) -> Result<()>;
}

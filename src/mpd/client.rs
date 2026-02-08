#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
#[cfg(target_os = "linux")]
use std::os::unix::net::SocketAddr;
use std::{
    collections::{HashSet, HashMap},
    io::{BufRead, BufReader, Write},
    net::{Shutdown, TcpStream},
    os::unix::net::UnixStream,
};

use anyhow::Result;
use log::debug;

use crate::core::player::{Player, PlayerBreakHandle, Enqueue, PlayerDelete};
use crate::mpd::commands::IdleEvent;
use crate::mpd::commands::{Song, Status, Playlist, LsInfo, Decoder, Mounts, State};
use crate::mpd::mpd_client::{ValueChange, Filter, Tag, AlbumArtOrder, SingleOrRange};
use crate::mpd::commands::status::OnOffOneshot;
use crate::mpd::QueuePosition;

use super::{
    commands::mpd_config::MpdConfig,
    errors::MpdError,
    proto_client::SocketClient,
    version::Version,
};
use crate::{
    config::{MpdAddress, address::MpdPassword},
    mpd::{
        errors::{ErrorCode, MpdFailureResponse},
        mpd_client::{MpdClient, MpdCommand},
        proto_client::ProtoClient,
    },
    shared::{macros::status_warn, mpd_client_ext::MpdClientExt},
};

type MpdResult<T> = Result<T, MpdError>;

const MIN_SUPPORTED_VERSION: Version = Version { major: 0, minor: 23, patch: 5 };

pub struct Client<'name> {
    name: &'name str,
    rx: BufReader<TcpOrUnixStream>,
    pub stream: TcpOrUnixStream,
    addr: MpdAddress,
    password: Option<MpdPassword>,
    pub version: Version,
    pub config: Option<MpdConfig>,
    pub supported_commands: HashSet<String>,
    partition: Option<String>,
    autocreate_partition: bool,
}

impl std::fmt::Debug for Client<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Client {{ name: {:?}, addr: {:?} }}", self.name, self.addr)
    }
}

pub enum TcpOrUnixStream {
    Unix(UnixStream),
    Tcp(TcpStream),
}

impl TcpOrUnixStream {
    fn set_write_timeout(&mut self, duration: Option<std::time::Duration>) -> std::io::Result<()> {
        match self {
            TcpOrUnixStream::Unix(s) => {
                s.set_write_timeout(duration)?;
            }
            TcpOrUnixStream::Tcp(s) => {
                s.set_write_timeout(duration)?;
            }
        }
        Ok(())
    }

    fn set_read_timeout(&mut self, duration: Option<std::time::Duration>) -> std::io::Result<()> {
        match self {
            TcpOrUnixStream::Unix(s) => {
                s.set_read_timeout(duration)?;
            }
            TcpOrUnixStream::Tcp(s) => {
                s.set_read_timeout(duration)?;
            }
        }
        Ok(())
    }

    pub fn try_clone(&self) -> std::io::Result<Self> {
        Ok(match self {
            TcpOrUnixStream::Unix(s) => TcpOrUnixStream::Unix(s.try_clone()?),
            TcpOrUnixStream::Tcp(s) => TcpOrUnixStream::Tcp(s.try_clone()?),
        })
    }

    #[allow(dead_code)]
    pub fn shutdown_both(&mut self) -> std::io::Result<()> {
        match self {
            TcpOrUnixStream::Unix(s) => s.shutdown(Shutdown::Both),
            TcpOrUnixStream::Tcp(s) => s.shutdown(Shutdown::Both),
        }
    }
}

impl std::io::Read for TcpOrUnixStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            TcpOrUnixStream::Unix(s) => s.read(buf),
            TcpOrUnixStream::Tcp(s) => s.read(buf),
        }
    }
}

impl std::io::Write for TcpOrUnixStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            TcpOrUnixStream::Unix(s) => s.write(buf),
            TcpOrUnixStream::Tcp(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            TcpOrUnixStream::Unix(s) => s.flush(),
            TcpOrUnixStream::Tcp(s) => s.flush(),
        }
    }
}

#[allow(dead_code)]
impl<'name> Client<'name> {
    pub fn init(
        addr: MpdAddress,
        password: Option<MpdPassword>,
        name: &'name str,
        partition: Option<String>,
        autocreate_partition: bool,
    ) -> MpdResult<Client<'name>> {
        let mut stream = match addr {
            MpdAddress::IpAndPort(ref addr) => TcpOrUnixStream::Tcp(TcpStream::connect(addr)?),
            MpdAddress::SocketPath(ref addr) => TcpOrUnixStream::Unix(UnixStream::connect(addr)?),
            #[cfg(target_os = "linux")]
            MpdAddress::AbstractSocket(ref addr) => {
                let addr = SocketAddr::from_abstract_name(addr)?;
                TcpOrUnixStream::Unix(UnixStream::connect_addr(&addr)?)
            }
            #[cfg(not(target_os = "linux"))]
            MpdAddress::AbstractSocket(ref _addr) => {
                return Err(MpdError::Generic(
                    "Abstract socket only supported on Linux".to_string(),
                ));
            }
        };
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(None)?;
        let mut rx = BufReader::new(stream.try_clone()?);

        let mut buf = String::new();
        rx.read_line(&mut buf)?;
        if !buf.starts_with("OK") {
            return Err(MpdError::Generic(format!("Handshake validation failed. '{buf}'")));
        }
        let Some(version): Option<Version> =
            buf.strip_prefix("OK MPD ").and_then(|v| v.parse().ok())
        else {
            return Err(MpdError::Generic(format!(
                "Handshake validation failed. Cannot parse version from '{buf}'"
            )));
        };

        debug!(name, addr:?, version = version.to_string().as_str(), handshake = buf.trim(); "MPD client initialized");

        if version < MIN_SUPPORTED_VERSION {
            status_warn!(
                "MPD version '{version}' is lower than supported. Minimum supported protocol version is '{MIN_SUPPORTED_VERSION}'. Some features may work incorrectly."
            );
        }

        let mut client = Self {
            name,
            rx,
            stream,
            addr,
            password,
            version,
            partition,
            autocreate_partition,
            config: None,
            supported_commands: HashSet::new(),
        };

        if let Some(MpdPassword(ref password)) = client.password.clone() {
            debug!("Used password auth to MPD");
            client.password(password)?;
        }

        if let Some(partition) = client.partition.clone() {
            debug!(partition = partition.as_str(); "Using partition");
            match MpdClient::switch_to_partition(&mut client, &partition) {
                Ok(()) => {}
                Err(MpdError::Mpd(MpdFailureResponse { code: ErrorCode::NoExist, .. }))
                    if autocreate_partition =>
                {
                    MpdClient::new_partition(&mut client, &partition)?;
                    MpdClient::switch_to_partition(&mut client, &partition)?;
                }
                err @ Err(_) => err?,
            }
        }

        // 2^18 seems to be max limit supported by MPD and higher values dont
        // have any effect
        client.binary_limit(2u64.pow(18))?;
        client.supported_commands = client.commands()?.0.into_iter().collect();

        Ok(client)
    }

    pub fn reconnect(&mut self) -> MpdResult<&Client<'_>> {
        debug!(name = self.name, addr:? = self.addr; "trying to reconnect");
        let mut stream = match &self.addr {
            MpdAddress::IpAndPort(addr) => TcpOrUnixStream::Tcp(TcpStream::connect(addr)?),
            MpdAddress::SocketPath(addr) => TcpOrUnixStream::Unix(UnixStream::connect(addr)?),
            #[cfg(target_os = "linux")]
            MpdAddress::AbstractSocket(addr) => {
                let addr = SocketAddr::from_abstract_name(addr)?;
                TcpOrUnixStream::Unix(UnixStream::connect_addr(&addr)?)
            }
            #[cfg(not(target_os = "linux"))]
            MpdAddress::AbstractSocket(addr) => {
                return Err(MpdError::Generic(
                    "Abstract socket only supported on Linux".to_string(),
                ));
            }
        };
        stream.set_write_timeout(None)?;
        stream.set_read_timeout(None)?;
        let mut rx = BufReader::new(stream.try_clone()?);

        let mut buf = String::new();
        rx.read_line(&mut buf)?;
        if !buf.starts_with("OK") {
            return Err(MpdError::Generic(format!("Handshake validation failed. '{buf}'")));
        }

        let Some(version): Option<Version> =
            buf.strip_prefix("OK MPD ").and_then(|v| v.parse().ok())
        else {
            return Err(MpdError::Generic(format!(
                "Handshake validation failed. Cannot parse version from '{buf}'"
            )));
        };

        self.rx = rx;
        self.stream = stream;
        self.version = version;
        self.config = None;

        debug!(name = self.name, addr:? = self.addr, handshake = buf.trim(), version = version.to_string().as_str(); "MPD client initialized");

        if let Some(MpdPassword(password)) = &self.password.clone() {
            debug!("Used password auth to MPD");
            self.password(password)?;
        }

        if let Some(partition) = self.partition.clone() {
            debug!(partition = partition.as_str(); "Using partition");
            match MpdClient::switch_to_partition(self, &partition) {
                Ok(()) => {}
                Err(MpdError::Mpd(MpdFailureResponse { code: ErrorCode::NoExist, .. }))
                    if self.autocreate_partition =>
                {
                    MpdClient::new_partition(self, &partition)?;
                    MpdClient::switch_to_partition(self, &partition)?;
                }
                err @ Err(_) => err?,
            }
        }

        self.supported_commands = self.commands()?.0.into_iter().collect();

        self.binary_limit(1024 * 1024 * 5)?;

        Ok(self)
    }

    pub fn set_read_timeout(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    pub fn set_write_timeout(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<()> {
        self.stream.set_write_timeout(timeout)
    }

    fn clear_read_buf(&mut self) -> Result<()> {
        log::trace!("Reinitialized read buffer");
        self.rx = BufReader::new(self.stream.try_clone()?);
        Ok(())
    }
}

impl SocketClient for Client<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        Write::write_all(&mut self.stream, bytes)
    }

    fn read(&mut self) -> &mut impl BufRead {
        &mut self.rx
    }

    fn clear_read_buf(&mut self) -> Result<()> {
        self.clear_read_buf()
    }

    fn version(&self) -> Version {
        self.version
    }
}

impl Player for Client<'_> {
    fn name(&self) -> &str {
        self.name
    }

    fn play(&mut self, pos: Option<usize>) -> Result<()> {
        if let Some(pos) = pos {
            MpdClient::play_pos(self, pos).map_err(Into::into)
        } else {
            MpdClient::play(self).map_err(Into::into)
        }
    }

    fn pause(&mut self) -> Result<()> {
        MpdClient::pause(self).map_err(Into::into)
    }

    fn pause_toggle(&mut self) -> Result<()> {
        MpdClient::pause_toggle(self).map_err(Into::into)
    }

    fn unpause(&mut self) -> Result<()> {
        MpdClient::unpause(self).map_err(Into::into)
    }

    fn stop(&mut self) -> Result<()> {
        MpdClient::stop(self).map_err(Into::into)
    }

    fn list_playlist_info(&mut self, name: &str, range: Option<SingleOrRange>) -> Result<Vec<Song>> {
        MpdClient::list_playlist_info(self, name, range).map_err(Into::into)
    }

    fn rename_playlist(&mut self, current_name: &str, new_name: &str) -> Result<()> {
        MpdClient::rename_playlist(self, current_name, new_name).map_err(Into::into)
    }

    fn move_in_playlist(&mut self, playlist: &str, range: &SingleOrRange, new_idx: usize) -> Result<()> {
        MpdClient::move_in_playlist(self, playlist, range, new_idx).map_err(Into::into)
    }

    fn move_output(&mut self, name: &str) -> Result<()> {
        MpdClient::move_output(self, name).map_err(Into::into)
    }

    fn switch_to_partition(&mut self, name: &str) -> Result<()> {
        MpdClient::switch_to_partition(self, name).map_err(Into::into)
    }

    fn new_partition(&mut self, name: &str) -> Result<()> {
        MpdClient::new_partition(self, name).map_err(Into::into)
    }

    fn delete_partition(&mut self, name: &str) -> Result<()> {
        MpdClient::delete_partition(self, name).map_err(Into::into)
    }

    fn switch_to_partition_with_autocreate(&mut self, name: &str, autocreate: bool) -> Result<()> {
        match MpdClient::switch_to_partition(self, name) {
            Ok(()) => Ok(()),
            Err(MpdError::Mpd(MpdFailureResponse { code: ErrorCode::NoExist, .. })) if autocreate => {
                MpdClient::new_partition(self, name)?;
                MpdClient::switch_to_partition(self, name).map_err(Into::into)
            }
            Err(err) => Err(err.into()),
        }
    }

    fn create_and_switch_to_partition(&mut self, name: &str) -> Result<()> {
        self.send_start_cmd_list()?;
        self.send_new_partition(name)?;
        self.send_switch_to_partition(name)?;
        self.send_execute_cmd_list()?;
        self.read_ok()?;
        Ok(())
    }

    fn update(&mut self, path: Option<&str>) -> Result<crate::mpd::commands::Update> {
        MpdClient::update(self, path).map_err(Into::into)
    }

    fn rescan(&mut self, path: Option<&str>) -> Result<crate::mpd::commands::Update> {
        MpdClient::rescan(self, path).map_err(Into::into)
    }

    fn next_keep_state(&mut self, keep: bool, state: State) -> Result<()> {
        MpdClientExt::next_keep_state(self, keep, state).map_err(Into::into)
    }

    fn prev_keep_state(&mut self, keep: bool, state: State) -> Result<()> {
        MpdClientExt::prev_keep_state(self, keep, state).map_err(Into::into)
    }

    fn add_random_songs(&mut self, count: usize, filter: Option<&[Filter<'_>]>) -> Result<()> {
        MpdClient::add_random_songs(self, count, filter).map_err(Into::into)
    }

    fn add_random_tag(&mut self, count: usize, tag: Tag) -> Result<()> {
        MpdClient::add_random_tag(self, count, tag).map_err(Into::into)
    }

    fn decoders(&mut self) -> Result<Vec<Decoder>> {
        MpdClient::decoders(self).map(|v| v.0).map_err(Into::into)
    }

    fn outputs(&mut self) -> Result<crate::mpd::commands::outputs::Outputs> {
        MpdClient::outputs(self).map_err(Into::into)
    }

    fn toggle_output(&mut self, id: u32) -> Result<()> {
        MpdClient::toggle_output(self, id).map_err(Into::into)
    }

    fn toggle_output_kind(&mut self, name: &str, id: u32, kind: crate::shared::mpd_client_ext::PartitionedOutputKind) -> Result<()> {
        match kind {
            crate::shared::mpd_client_ext::PartitionedOutputKind::OtherPartition => {
                MpdClient::move_output(self, name)?;
                let new_outputs = MpdClient::outputs(self)?.0;
                if let Some(output) = new_outputs.iter().find(|output| output.name == name) {
                    MpdClient::enable_output(self, output.id)?;
                }
            }
            crate::shared::mpd_client_ext::PartitionedOutputKind::CurrentPartition => {
                MpdClient::toggle_output(self, id)?;
            }
        }
        Ok(())
    }

    fn enable_output(&mut self, id: u32) -> Result<()> {
        MpdClient::enable_output(self, id).map_err(Into::into)
    }

    fn disable_output(&mut self, id: u32) -> Result<()> {
        MpdClient::disable_output(self, id).map_err(Into::into)
    }

    fn find_one(&mut self, filter: &[Filter<'_>]) -> Result<Option<Song>> {
        MpdClient::find_one(self, filter).map_err(Into::into)
    }

    fn mount(&mut self, name: &str, path: &str) -> Result<()> {
        MpdClient::mount(self, name, path).map_err(Into::into)
    }

    fn unmount(&mut self, name: &str) -> Result<()> {
        MpdClient::unmount(self, name).map_err(Into::into)
    }

    fn list_mounts(&mut self) -> Result<Mounts> {
        MpdClient::list_mounts(self).map_err(Into::into)
    }

    fn list_partitions(&mut self) -> Result<Vec<String>> {
        MpdClient::list_partitions(self).map(|v| v.0).map_err(Into::into)
    }

    fn find_album_art(&mut self, path: &str, order: AlbumArtOrder) -> Result<Option<Vec<u8>>> {
        MpdClient::find_album_art(self, path, order).map_err(Into::into)
    }

    fn sticker(&mut self, uri: &str, key: &str) -> Result<Option<crate::mpd::commands::stickers::Sticker>> {
        MpdClient::sticker(self, uri, key).map_err(Into::into)
    }

    fn set_sticker(&mut self, uri: &str, key: &str, value: &str) -> Result<()> {
        MpdClient::set_sticker(self, uri, key, value).map_err(Into::into)
    }

    fn delete_sticker(&mut self, uri: &str, key: &str) -> Result<()> {
        MpdClient::delete_sticker(self, uri, key).map_err(Into::into)
    }

    fn delete_all_stickers(&mut self, uri: &str) -> Result<()> {
        MpdClient::delete_all_stickers(self, uri).map_err(Into::into)
    }

    fn list_stickers(&mut self, uri: &str) -> Result<crate::mpd::commands::stickers::Stickers> {
        MpdClient::list_stickers(self, uri).map_err(Into::into)
    }

    fn find_stickers(&mut self, uri: &str, key: &str, filter: Option<crate::mpd::mpd_client::StickerFilter>) -> Result<crate::mpd::commands::stickers::StickersWithFile> {
        MpdClient::find_stickers(self, uri, key, filter).map_err(Into::into)
    }

    fn send_message(&mut self, channel: &str, content: &str) -> Result<()> {
        MpdClient::send_message(self, channel, content).map_err(Into::into)
    }

    fn delete_id(&mut self, id: u32) -> Result<()> {
        MpdClient::delete_id(self, id).map_err(Into::into)
    }

    fn play_id(&mut self, id: u32) -> Result<()> {
        MpdClient::play_id(self, id).map_err(Into::into)
    }

    fn move_id(&mut self, id: u32, to: QueuePosition) -> Result<()> {
        MpdClient::move_id(self, id, to).map_err(Into::into)
    }

    fn move_in_queue(&mut self, from: SingleOrRange, to: QueuePosition) -> Result<()> {
        MpdClient::move_in_queue(self, from, to).map_err(Into::into)
    }

    fn swap_positions(&mut self, swaps: Vec<(usize, usize)>) -> Result<()> {
        self.send_start_cmd_list()?;
        for swap in swaps {
            MpdCommand::send_swap_position(self, swap.0, swap.1)?;
        }
        self.send_execute_cmd_list()?;
        self.read_ok()?;
        Ok(())
    }

    fn delete_from_queue(&mut self, songs: SingleOrRange) -> Result<()> {
        MpdClient::delete_from_queue(self, songs).map_err(Into::into)
    }

    fn shuffle(&mut self, range: Option<SingleOrRange>) -> Result<()> {
        MpdClient::shuffle(self, range).map_err(Into::into)
    }

    fn save_queue_as_playlist(&mut self, name: &str, mode: Option<crate::mpd::mpd_client::SaveMode>) -> Result<()> {
        MpdClient::save_queue_as_playlist(self, name, mode).map_err(Into::into)
    }

    fn play_pos(&mut self, pos: usize) -> Result<()> {
        MpdClient::play_pos(self, pos).map_err(Into::into)
    }

    fn add_downloaded_file_to_queue(&mut self, path: std::path::PathBuf, cache_dir: Option<&std::path::Path>, position: Option<QueuePosition>) -> Result<()> {
        MpdClientExt::add_downloaded_file_to_queue(self, path, cache_dir, position).map_err(Into::into)
    }

    fn next(&mut self) -> Result<()> {
        MpdClient::next(self).map_err(Into::into)
    }

    fn prev(&mut self) -> Result<()> {
        MpdClient::prev(self).map_err(Into::into)
    }

    fn seek_current(&mut self, value: ValueChange) -> Result<()> {
        MpdClient::seek_current(self, value).map_err(Into::into)
    }

    fn get_status(&mut self) -> Result<Status> {
        MpdClient::get_status(self).map_err(Into::into)
    }

    fn get_current_song(&mut self) -> Result<Option<Song>> {
        MpdClient::get_current_song(self).map_err(Into::into)
    }

    fn get_queue(&mut self) -> Result<Vec<Song>> {
        MpdClient::playlist_info(self).map(|v| v.unwrap_or_default()).map_err(Into::into)
    }

    fn add(&mut self, uri: &str, position: Option<QueuePosition>) -> Result<()> {
        MpdClient::add(self, uri, position).map_err(Into::into)
    }

    fn clear_queue(&mut self) -> Result<()> {
        MpdClient::clear(self).map_err(Into::into)
    }

    fn volume(&mut self, volume: ValueChange) -> Result<()> {
        MpdClient::volume(self, volume).map_err(Into::into)
    }

    fn crossfade(&mut self, seconds: u32) -> Result<()> {
        MpdClient::crossfade(self, seconds).map_err(Into::into)
    }

    fn set_repeat(&mut self, enabled: bool) -> Result<()> {
        MpdClient::repeat(self, enabled).map_err(Into::into)
    }

    fn set_random(&mut self, enabled: bool) -> Result<()> {
        MpdClient::random(self, enabled).map_err(Into::into)
    }

    fn set_single(&mut self, single: OnOffOneshot) -> Result<()> {
        MpdClient::single(self, single).map_err(Into::into)
    }

    fn set_consume(&mut self, consume: OnOffOneshot) -> Result<()> {
        MpdClient::consume(self, consume).map_err(Into::into)
    }

    fn enqueue_multiple(&mut self, items: Vec<Enqueue>, autoplay_idx: Option<usize>, position: Option<QueuePosition>, replace: bool) -> Result<()> {
        MpdClientExt::enqueue_multiple(self, items, autoplay_idx, position, replace).map_err(Into::into)
    }

    fn delete_multiple(&mut self, items: Vec<PlayerDelete>) -> Result<()> {
        MpdClientExt::delete_multiple(self, items).map_err(Into::into)
    }

    fn create_playlist(&mut self, name: &str, items: Vec<String>) -> Result<()> {
        MpdClientExt::create_playlist(self, name, items).map_err(Into::into)
    }

    fn add_to_playlist(&mut self, playlist_name: &str, uri: &str, position: Option<usize>) -> Result<()> {
        MpdClient::add_to_playlist(self, playlist_name, uri, position).map_err(Into::into)
    }

    fn add_to_playlist_multiple(&mut self, playlist_name: &str, song_paths: Vec<String>) -> Result<()> {
        MpdClientExt::add_to_playlist_multiple(self, playlist_name, song_paths).map_err(Into::into)
    }

    fn get_songs_info(&mut self, song_uris: Vec<String>) -> Result<Vec<Song>> {
        self.send_start_cmd_list()?;
        for uri in song_uris {
            MpdCommand::send_lsinfo(self, Some(&uri))?;
        }
        self.send_execute_cmd_list()?;
        self.read_response::<crate::mpd::commands::lsinfo::LsInfo>().map(|v| v.into_songs().collect()).map_err(Into::into)
    }

    fn fetch_song_stickers(&mut self, song_uris: Vec<String>) -> Result<HashMap<String, HashMap<String, String>>> {
        MpdClientExt::fetch_song_stickers(self, song_uris).map_err(Into::into)
    }

    fn set_sticker_multiple(&mut self, key: &str, value: String, items: Vec<Enqueue>) -> Result<()> {
        MpdClientExt::set_sticker_multiple(self, key, value, items).map_err(Into::into)
    }

    fn delete_sticker_multiple(&mut self, key: &str, items: Vec<Enqueue>) -> Result<()> {
        MpdClientExt::delete_sticker_multiple(self, key, items).map_err(Into::into)
    }

    fn find(&mut self, filter: &[Filter<'_>]) -> Result<Vec<Song>> {
        MpdClient::find(self, filter).map_err(Into::into)
    }

    fn search(&mut self, filter: &[Filter<'_>], ignore_diacritics: bool) -> Result<Vec<Song>> {
        MpdClient::search(self, filter, ignore_diacritics).map_err(Into::into)
    }

    fn list_tag(&mut self, tag: Tag, filter: Option<&[Filter<'_>]>) -> Result<Vec<String>> {
        MpdClient::list_tag(self, tag, filter).map(|v| v.0).map_err(Into::into)
    }

    fn list_all(&mut self, path: Option<&str>) -> Result<Vec<Song>> {
        Ok(MpdClient::list_all(self, path)?.into_files().map(|file| Song { file, ..Song::default() }).collect())
    }

    fn lsinfo(&mut self, path: Option<&str>) -> Result<LsInfo> {
        MpdClient::lsinfo(self, path).map_err(Into::into)
    }

    fn list_playlists(&mut self) -> Result<Vec<Playlist>> {
        MpdClient::list_playlists(self).map_err(Into::into)
    }

    fn list_partitioned_outputs(&mut self, current_partition: &str) -> Result<Vec<crate::shared::mpd_client_ext::PartitionedOutput>> {
        MpdClientExt::list_partitioned_outputs(self, current_partition).map_err(Into::into)
    }

    fn wait_for_event(&mut self) -> Result<Vec<IdleEvent>> {
        MpdClient::idle(self, None).map_err(Into::into)
    }

    fn get_break_handle(&self) -> Result<Box<dyn PlayerBreakHandle>> {
        Ok(Box::new(MpdBreakHandle(self.stream.try_clone()?)))
    }

    fn set_read_timeout(&mut self, timeout: Option<std::time::Duration>) -> Result<()> {
        Client::set_read_timeout(self, timeout).map_err(Into::into)
    }

    fn set_write_timeout(&mut self, timeout: Option<std::time::Duration>) -> Result<()> {
        Client::set_write_timeout(self, timeout).map_err(Into::into)
    }

    fn reconnect(&mut self) -> Result<()> {
        Client::reconnect(self).map(|_| ()).map_err(Into::into)
    }

    fn supported_commands(&self) -> HashSet<String> {
        self.supported_commands.clone()
    }

    fn version(&self) -> Version {
        self.version
    }

    fn config(&self) -> Option<crate::mpd::commands::mpd_config::MpdConfig> {
        self.config.clone()
    }
}

struct MpdBreakHandle(TcpOrUnixStream);
impl PlayerBreakHandle for MpdBreakHandle {
    fn break_idle(&self) -> Result<()> {
        let mut stream = self.0.try_clone()?;
        stream.write_all(b"noidle\n")?;
        Ok(())
    }
}

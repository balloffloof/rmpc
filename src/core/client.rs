use std::{
    collections::VecDeque,
    io::{self},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::Builder,
    time::Duration,
};

use anyhow::Result;
use crossbeam::{
    channel::{Receiver, Sender, bounded},
    select,
};
use drop_guard::ClientDropGuard;
use parking_lot::Mutex;

use crate::{
    config::Config,
    core::player::Player,
    mpd::{
        commands::idle::IdleEvent,
        errors::MpdError,
    },
    shared::{
        events::{AppEvent, ClientRequest, WorkDone},
        macros::{status_error, try_break, try_skip},
    },
};

pub fn init(
    client_rx: Receiver<ClientRequest>,
    event_tx: Sender<AppEvent>,
    client: Box<dyn Player>,
    config: Arc<Config>,
) -> io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("client".to_owned())
        .spawn(move || client_task(&client_rx, &event_tx, client, &config))
}

static HEALTHY: AtomicBool = AtomicBool::new(true);

macro_rules! health {
    ($e:expr, $msg:literal) => {
        {
            if HEALTHY.load(Ordering::Relaxed) == false {
                log::error!("Client is not healhty. Trying to end threads and reconnect");
                break;
            }

            match $e {
                Ok(v) => v,
                Err(e) => {
                    log::error!(error:? = e; $msg);
                    HEALTHY.store(false, Ordering::Relaxed);
                    break;
                }
            }
        }
    };
}

fn should_skip_request(buffer: &VecDeque<ClientRequest>, request: &ClientRequest) -> bool {
    buffer.iter().any(|request2| {
        if let (ClientRequest::Query(q1), ClientRequest::Query(q2)) = (&request, &request2) {
            q1.should_be_skipped(q2)
        } else {
            false
        }
    })
}

#[allow(clippy::single_match_else)]
fn client_task(
    request_rx: &Receiver<ClientRequest>,
    event_tx: &Sender<AppEvent>,
    client: Box<dyn Player>,
    config: &Config,
) {
    // TODO probably a good idea to drop the channels on each reconnect loop
    let mut first_loop = true;
    let (client_received_tx, client_received_rx) = &bounded::<()>(0);
    let (client_return_tx, client_return_rx) = &bounded::<Box<dyn Player>>(1);

    std::thread::scope(|s| {
        client_return_tx.send(client).expect("Player init to succeed");

        loop {
            log::trace!(first_loop; "Starting worker threads");

            HEALTHY.store(true, Ordering::Relaxed);

            let _ = client_received_rx.try_iter().collect::<Vec<_>>();

            log::trace!(first_loop; "Trying to get returned player");
            let mut client = match client_return_rx.recv() {
                Ok(client) => client,
                Err(err) => {
                    log::error!(err:?; "Did not receive player from the return channel");
                    break;
                }
            };
            let is_client_ok =
                check_connection(first_loop, client.as_mut(), request_rx, event_tx, config);
            first_loop = false;

            if is_client_ok {
                let break_handle = Arc::new(Mutex::new(client.get_break_handle().expect("Failed to get break handle")));
                let break_handle_work = Arc::clone(&break_handle);

                let idle = Builder::new()
                    .name("idle".to_string())
                    .spawn_scoped(s, move || {
                        'outer: loop {
                            log::trace!("Waiting to acquire player");
                            let client = health!(client_return_rx.recv(), "Failed to receive player from request thread");
                            let mut client = ClientDropGuard::new(client_return_tx, client);
                            let timeout = config.mpd_idle_read_timeout_ms;
                            log::trace!(timeout:?; "Successfully acquired player, setting read timeout");

                            health!(client.set_read_timeout(config.mpd_idle_read_timeout_ms), "Failed to set read timeout for idle player");

                            log::trace!("Sending player received confirmation");
                            health!(client_received_tx.send_timeout((), Duration::from_secs(3)), "Failed to send player received confirmation");

                            log::trace!("Idle confirmation sent, waiting for events");
                            let events: Vec<IdleEvent> = loop {
                                match client.wait_for_event() {
                                    Ok(events) => break events,
                                    Err(err) => match err.downcast_ref::<MpdError>() {
                                        Some(MpdError::TimedOut(err)) => {
                                            if !HEALTHY.load(Ordering::Relaxed) {
                                                log::warn!(err:?; "Not healthy. Reading idle events timed out");
                                                break 'outer;
                                            }
                                        }
                                        _ => {
                                            log::error!(err:?; "Encountered error while reading idle events");
                                            HEALTHY.store(false, Ordering::Relaxed);
                                            break 'outer
                                        }
                                    },
                                }
                            };

                            log::trace!(events:?; "Got idle events");
                            for ev in events {
                                if let Err(err) = event_tx.send(AppEvent::IdleEvent(ev)) {
                                    log::error!(err:?; "Failed to send idle event");
                                    break 'outer;
                                }
                            }

                            log::trace!("Stopping idle, dropping player");
                            drop(client);
                            log::trace!("Player dropped, waiting for confirmation");
                            health!(client_received_rx.recv_timeout(Duration::from_secs(3)), "Did not receive confirmation from worker thread");
                            log::trace!("Confirmation received");
                        }
                        log::trace!("idle loop ended");
                    })
                    .expect("failed to spawn thread");

                try_skip!(client_return_tx.send(client), "Failed to request for player idle");
                try_break!(client_received_rx.recv(), "Idle confirmation failed");

                let work = Builder::new()
                    .name("request".to_string())
                    .spawn_scoped(s, move || {
                        let mut buffer = VecDeque::new();

                        loop {
                            log::trace!("Waiting for player requests");
                            let msg = select! {
                                recv(request_rx) -> msg => {
                                    health!(msg, "Failed to receive player request")
                                }
                                recv(client_return_rx) -> client => {
                                    let client = match client {
                                        Ok(client) => ClientDropGuard::new(client_return_tx, client),
                                        Err(err) => {
                                            log::error!(err:?; "Failed to receive player from idle thread");
                                            HEALTHY.store(false, Ordering::Relaxed);
                                            break;
                                        }
                                    };

                                    if !HEALTHY.load(Ordering::Relaxed) {
                                        log::error!("Received player from idle thread while not healthy. Breaking the loop.");
                                        break;
                                    }

                                    log::trace!("Received player from idle. No work to do. Sending it back.");
                                    health!(client_received_tx.send_timeout((), Duration::from_secs(3)), "Failed to send player received confirmation");
                                    drop(client);
                                    log::trace!("Waiting for confirmation from idle thread");
                                    health!(client_received_rx.recv_timeout(Duration::from_secs(3)), "Did not receive confirmation from idle thread");
                                    continue;
                                }
                            };
                            buffer.push_back(msg);

                            log::trace!(buffer:?; "Got requests. Trying to receive player from idle thread");
                            health!(break_handle_work.lock().break_idle(), "Failed to break idle");

                            let client = health!(client_return_rx.recv(), "Failed to receive player from idle thread");
                            let mut client = ClientDropGuard::new(client_return_tx, client);
                            log::trace!("Successfully received player from idle thread. Sending confirmation.");

                            health!(client_received_tx.send_timeout((), Duration::from_secs(3)), "Failed to send player received confirmation");

                            log::trace!(timeout:? = config.mpd_read_timeout; "Setting read timeout");
                            health!(client.set_read_timeout(Some(config.mpd_read_timeout)), "Failed to set read timeout");

                            while let Some(request) = buffer.pop_front() {
                                while let Ok(request) = request_rx.try_recv() {
                                    log::trace!(count = buffer.len(), buffer:?; "Got more requests");
                                    buffer.push_back(request);
                                }

                                if should_skip_request(&buffer, &request) {
                                    log::trace!(request:?; "Skipping duplicated request");
                                    continue;
                                }

                                match handle_client_request(&mut *client, request) {
                                    Ok(result) => {
                                        health!(
                                            event_tx.send(AppEvent::WorkDone(Ok(result))),
                                            "Failed to send work done success event"
                                        );
                                    }
                                    Err(err) => match err.downcast_ref::<MpdError>() {
                                        Some(MpdError::TimedOut(err)) => {
                                            status_error!(err:?; "Reading response from player timed out, will try to reconnect");
                                            health!(client.reconnect(), "Failed to reconnect");
                                            health!(client.set_write_timeout(Some(config.mpd_write_timeout)), "Failed to set write timeout");
                                            *break_handle_work.lock() = health!(client.get_break_handle(), "Failed to get break handle");
                                        },
                                        _ => {
                                            log::error!(error:? = err; "Failed to handle player request");
                                            health!(
                                                event_tx.send(AppEvent::WorkDone(Err(err))),
                                                "Failed to send work done error event"
                                            );
                                        },
                                    },
                                }
                            }

                            log::trace!("All requests processed, returning player to idle thread");
                            drop(client);
                            log::trace!("Player returned to idle thread. Waiting for confirmation");
                            health!(client_received_rx.recv_timeout(Duration::from_secs(3)), "Did not receive confirmation from idle thread");
                        }

                        log::error!("Work loop ended.");
                        HEALTHY.store(false, Ordering::Relaxed);
                    })
                    .expect("failed to spawn thread");

                idle.join().expect("idle thread not to panic");
                work.join().expect("work thread not to panic");
            } else {
                client_return_tx.send(client).expect("To be able to return the player");
            }

            let wait_time = std::time::Duration::from_secs(1);
            log::debug!(wait_time:?; "Lost connection to player, waiting before trying again");
            try_skip!(
                event_tx.send(AppEvent::LostConnection),
                "Failed to send lost connection event"
            );
            std::thread::sleep(wait_time);
        }
    });
}

mod drop_guard {
    use std::ops::DerefMut;

    use crossbeam::channel::Sender;

    use crate::core::player::Player;

    pub struct ClientDropGuard<'sender> {
        tx: &'sender Sender<Box<dyn Player>>,
        client: Option<Box<dyn Player>>,
    }

    impl std::fmt::Debug for ClientDropGuard<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ClientDropGuard").finish()
        }
    }

    impl<'sender> ClientDropGuard<'sender> {
        pub fn new(tx: &'sender Sender<Box<dyn Player>>, client: Box<dyn Player>) -> Self {
            Self { tx, client: Some(client) }
        }
    }

    impl Drop for ClientDropGuard<'_> {
        fn drop(&mut self) {
            if let Some(client) = self.client.take() {
                log::trace!("Sending back player on drop");
                if let Err(err) = self.tx.send(client) {
                    log::error!(error:? = err; "Failed to send player back on drop");
                }
            }
        }
    }

    impl std::ops::Deref for ClientDropGuard<'_> {
        type Target = dyn Player;

        fn deref(&self) -> &Self::Target {
            self.client.as_ref().expect("Cannot deref because client was None").as_ref()
        }
    }

    impl DerefMut for ClientDropGuard<'_> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.client.as_mut().expect("Cannot deref_mut because client was None").as_mut()
        }
    }
}

fn check_connection(
    first_loop: bool,
    client: &mut dyn Player,
    client_rx: &Receiver<ClientRequest>,
    event_tx: &Sender<AppEvent>,
    config: &Config,
) -> bool {
    if first_loop {
        true
    } else if client.reconnect().is_ok() {
        // empty the work queue after reconnect as they might no longer be
        // relevant
        let _ = client_rx.try_iter().collect::<Vec<_>>();
        try_skip!(event_tx.send(AppEvent::Reconnected), "Failed to send reconnected event");
        if let Err(err) = client.set_read_timeout(Some(config.mpd_read_timeout)) {
            log::error!(error:? = err; "Failed to set read timeout");
            return false;
        }
        if let Err(err) = client.set_write_timeout(Some(config.mpd_write_timeout)) {
            log::error!(error:? = err; "Failed to set write timeout");
            return false;
        }
        true
    } else {
        false
    }
}

fn handle_client_request(client: &mut dyn Player, request: ClientRequest) -> Result<WorkDone> {
    match request {
        ClientRequest::Query(query) => Ok(WorkDone::MpdCommandFinished {
            id: query.id,
            target: query.target,
            data: (query.callback)(client)?,
        }),
        ClientRequest::Command(command) => {
            (command.callback)(client)?;
            Ok(WorkDone::None)
        }
        ClientRequest::QuerySync(query) => {
            let result = (query.callback)(client)?;
            query.tx.send(result)?;
            Ok(WorkDone::None)
        }
    }
}

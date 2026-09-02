use std::{
    collections::HashMap,
    io::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Waker,
    thread::{self, JoinHandle},
};

use mio::{Events, Interest, Poll, Registry, Token, event::Source};

use crate::config::Config;

struct Registrations {
    registry: Registry,
    waker_table: HashMap<Token, (Interest, Waker)>,
}

pub struct Reactor {
    registrations: Mutex<Registrations>,
    next_token: AtomicUsize,
    handle: Mutex<Option<JoinHandle<Result<(), ReactorError>>>>,
}

#[derive(Debug)]
pub enum ReactorError {
    IoError(Error),
}

impl From<Error> for ReactorError {
    fn from(value: Error) -> Self {
        ReactorError::IoError(value)
    }
}

impl From<ReactorError> for Error {
    fn from(value: ReactorError) -> Self {
        match value {
            ReactorError::IoError(e) => e,
        }
    }
}

impl Reactor {
    pub fn new(config: &Config) -> Result<Arc<Self>, ReactorError> {
        let poll = Poll::new()?;
        let registry = poll.registry().try_clone()?;
        let reactor = Arc::new(Self {
            registrations: Mutex::new(Registrations {
                registry,
                waker_table: HashMap::new(),
            }),
            next_token: AtomicUsize::new(0),
            handle: Mutex::new(None),
        });

        let events_capacity = config.events_capacity;
        let thread_reactor = reactor.clone();
        let handle = thread::spawn(move || thread_reactor.run(poll, events_capacity));
        *reactor.handle.lock().unwrap() = Some(handle);

        Ok(reactor)
    }

    pub(crate) fn token(&self) -> Token {
        Token(self.next_token.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn register<S>(
        &self,
        source: &mut S,
        token: Token,
        interests: Interest,
        waker: Waker,
    ) -> Result<(), ReactorError>
    where
        S: Source + ?Sized,
    {
        let mut registrations = self.registrations.lock().unwrap();
        match registrations.waker_table.insert(token, (interests, waker)) {
            None => registrations.registry.register(source, token, interests)?,
            Some((old_interests, _)) if old_interests != interests => registrations
                .registry
                .reregister(source, token, interests)?,
            Some(_) => {}
        }
        Ok(())
    }

    pub(crate) fn deregister<S>(&self, source: &mut S, token: &Token) -> Result<(), ReactorError>
    where
        S: Source + ?Sized,
    {
        let mut registrations = self.registrations.lock().unwrap();
        if registrations.waker_table.remove(token).is_some() {
            registrations.registry.deregister(source)?;
        }
        Ok(())
    }

    fn run(&self, mut poll: Poll, events_capacity: usize) -> Result<(), ReactorError> {
        let mut events = Events::with_capacity(events_capacity);
        loop {
            poll.poll(&mut events, None)?;
            for event in events.iter() {
                let registrations = self.registrations.lock().unwrap();
                if let Some((_, waker)) = registrations.waker_table.get(&event.token()) {
                    waker.wake_by_ref();
                }
            }
        }
    }
}

//! Bounded lock-free per-peer fixed-window rate limiting.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

const EMPTY: u64 = 0;
const WRITING: u64 = 1;
const OCCUPIED: u64 = 2;
const STATUS_MASK: u64 = 0b11;
const READERS_SHIFT: u32 = 2;
const READERS_BITS: u32 = 14;
const READERS_INCREMENT: u64 = 1 << READERS_SHIFT;
const READERS_MASK: u64 = ((1 << READERS_BITS) - 1) << READERS_SHIFT;
const GENERATION_SHIFT: u32 = READERS_SHIFT + READERS_BITS;
const GENERATION_MASK: u64 = u64::MAX >> GENERATION_SHIFT;
const MAX_SLOT_RETRIES: usize = 8;
const MAX_KEY_RESERVATION_RETRIES: usize = 64;
const MAX_PROBES: usize = 64;
const RETENTION_SECONDS: u32 = 60;

/// Default maximum number of peers retained by one enabled rate limiter.
pub const DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY: usize = 16_384;

/// Result of one peer-rate-limit admission attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacellePeerRateLimitResult {
    /// The peer is within its fixed-window budget.
    Allowed,
    /// The peer has exhausted its fixed-window budget.
    RateLimited,
    /// The bounded peer table has no safely reusable entry, or an in-progress
    /// transition did not resolve within the bounded retry budget.
    TableFull,
}

impl NacellePeerRateLimitResult {
    /// Whether the admission attempt succeeded.
    pub const fn is_allowed(self) -> bool {
        matches!(self, Self::Allowed)
    }
}

/// A bounded, lock-free, exact-IP fixed-window rate limiter.
///
/// The table has a fixed capacity. Admission probes a fixed number of slots and
/// retries an in-progress slot transition only a bounded number of times. A
/// full table rejects a newly observed peer rather than allocating, scanning
/// every tracked peer, or blocking an unrelated request/connection on a mutex.
#[derive(Debug)]
pub struct NacellePeerRateLimiter {
    started_at: Instant,
    hasher: RandomState,
    capacity: usize,
    active_entries: AtomicUsize,
    // Cold peers claim their hash while selecting a slot so racing probes cannot
    // install the same exact IP in separate slots. Established peers do not use it.
    key_reservations: Box<[AtomicU64]>,
    slots: Box<[PeerRateSlot]>,
}

#[derive(Debug)]
struct PeerRateSlot {
    state: AtomicU64,
    key_kind: AtomicU8,
    key_high: AtomicU64,
    key_low: AtomicU64,
    rate: AtomicU64,
    last_seen: AtomicU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PeerRateKey {
    kind: u8,
    high: u64,
    low: u64,
}

struct SlotReader<'a> {
    slot: &'a PeerRateSlot,
}

struct KeyReservation<'a> {
    reservation: &'a AtomicU64,
}

impl Drop for SlotReader<'_> {
    fn drop(&mut self) {
        self.slot
            .state
            .fetch_sub(READERS_INCREMENT, Ordering::Release);
    }
}

impl Drop for KeyReservation<'_> {
    fn drop(&mut self) {
        self.reservation.store(0, Ordering::Release);
    }
}

impl PeerRateSlot {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(EMPTY),
            key_kind: AtomicU8::new(0),
            key_high: AtomicU64::new(0),
            key_low: AtomicU64::new(0),
            rate: AtomicU64::new(0),
            last_seen: AtomicU32::new(0),
        }
    }

    fn acquire_reader(&self, state: u64) -> Option<SlotReader<'_>> {
        if status(state) != OCCUPIED || readers(state) == max_readers() {
            return None;
        }
        self.state
            .compare_exchange(
                state,
                state + READERS_INCREMENT,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok()
            .map(|_| SlotReader { slot: self })
    }

    fn key(&self) -> PeerRateKey {
        PeerRateKey {
            kind: self.key_kind.load(Ordering::Relaxed),
            high: self.key_high.load(Ordering::Relaxed),
            low: self.key_low.load(Ordering::Relaxed),
        }
    }

    fn initialize(&self, key: PeerRateKey, tick: u32) {
        self.key_kind.store(key.kind, Ordering::Relaxed);
        self.key_high.store(key.high, Ordering::Relaxed);
        self.key_low.store(key.low, Ordering::Relaxed);
        self.rate.store(pack_rate(tick, 1), Ordering::Relaxed);
        self.last_seen.store(tick, Ordering::Relaxed);
    }

    fn publish(&self, writing_state: u64) {
        self.state.store(
            with_status(next_generation(writing_state), OCCUPIED),
            Ordering::Release,
        );
    }
}

impl NacellePeerRateLimiter {
    /// Construct a limiter that tracks at most `capacity` peers at one time.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let slot_count = capacity.saturating_mul(2).max(2);
        Self {
            started_at: Instant::now(),
            hasher: RandomState::new(),
            capacity,
            active_entries: AtomicUsize::new(0),
            key_reservations: std::iter::repeat_with(|| AtomicU64::new(0))
                .take(slot_count)
                .collect(),
            slots: std::iter::repeat_with(PeerRateSlot::new)
                .take(slot_count)
                .collect(),
        }
    }

    /// Maximum number of peer entries retained by this limiter.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Attempt to admit one event for `peer` under a per-second `limit`.
    pub fn try_acquire(&self, peer: IpAddr, limit: usize) -> NacellePeerRateLimitResult {
        if limit == 0 {
            return NacellePeerRateLimitResult::RateLimited;
        }
        self.try_acquire_at(peer, limit, self.current_tick())
    }

    fn current_tick(&self) -> u32 {
        self.started_at.elapsed().as_secs() as u32
    }

    fn try_acquire_at(&self, peer: IpAddr, limit: usize, tick: u32) -> NacellePeerRateLimitResult {
        if limit == 0 {
            return NacellePeerRateLimitResult::RateLimited;
        }
        let key = PeerRateKey::from(peer);
        let hash = self.hasher.hash_one(key);
        let start = (hash as usize) % self.slots.len();
        let reservation_key = hash.max(1);

        if let Some(result) = self.try_admit_existing(key, limit, tick, start) {
            return result;
        }

        for _ in 0..MAX_KEY_RESERVATION_RETRIES {
            if let Some(_reservation) = self.try_reserve_key(reservation_key) {
                return self.try_acquire_reserved(key, limit, tick, start);
            }

            if let Some(result) = self.try_admit_existing(key, limit, tick, start) {
                return result;
            }
            std::thread::yield_now();
        }

        NacellePeerRateLimitResult::TableFull
    }

    fn try_admit_existing(
        &self,
        key: PeerRateKey,
        limit: usize,
        tick: u32,
        start: usize,
    ) -> Option<NacellePeerRateLimitResult> {
        for offset in 0..self.slots.len().min(MAX_PROBES) {
            let slot = &self.slots[(start + offset) % self.slots.len()];
            for _ in 0..MAX_SLOT_RETRIES {
                let state = slot.state.load(Ordering::Acquire);
                match status(state) {
                    EMPTY => break,
                    WRITING => std::hint::spin_loop(),
                    OCCUPIED => {
                        let Some(_reader) = slot.acquire_reader(state) else {
                            continue;
                        };
                        if slot.key() == key {
                            return Some(admit_existing(slot, limit, tick));
                        }
                        break;
                    }
                    _ => unreachable!("slot status uses two bits"),
                }
            }
        }
        None
    }

    fn try_acquire_reserved(
        &self,
        key: PeerRateKey,
        limit: usize,
        tick: u32,
        start: usize,
    ) -> NacellePeerRateLimitResult {
        for offset in 0..self.slots.len().min(MAX_PROBES) {
            let slot = &self.slots[(start + offset) % self.slots.len()];
            let mut probe_next = false;
            for _ in 0..MAX_SLOT_RETRIES {
                let state = slot.state.load(Ordering::Acquire);
                match status(state) {
                    EMPTY => {
                        if !self.reserve_entry() {
                            probe_next = true;
                            break;
                        }
                        let writing = writer_state(state);
                        if slot
                            .state
                            .compare_exchange(state, writing, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            slot.initialize(key, tick);
                            slot.publish(writing);
                            return NacellePeerRateLimitResult::Allowed;
                        }
                        self.active_entries.fetch_sub(1, Ordering::Release);
                    }
                    WRITING => std::hint::spin_loop(),
                    OCCUPIED => {
                        let Some(reader) = slot.acquire_reader(state) else {
                            continue;
                        };
                        if slot.key() == key {
                            return admit_existing(slot, limit, tick);
                        }
                        let reusable = expired(slot.last_seen.load(Ordering::Acquire), tick);
                        drop(reader);
                        if !reusable {
                            probe_next = true;
                            break;
                        }
                        if readers(state) != 0 {
                            continue;
                        }
                        let writing = writer_state(state);
                        if slot
                            .state
                            .compare_exchange(state, writing, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            slot.initialize(key, tick);
                            slot.publish(writing);
                            return NacellePeerRateLimitResult::Allowed;
                        }
                    }
                    _ => unreachable!("slot status uses two bits"),
                }
            }
            // If this slot kept changing, it may contain `key`. Probing past an
            // inconclusive slot could install a duplicate and exceed the limit.
            if !probe_next {
                return NacellePeerRateLimitResult::TableFull;
            }
        }
        NacellePeerRateLimitResult::TableFull
    }

    fn try_reserve_key(&self, key: u64) -> Option<KeyReservation<'_>> {
        let reservation = &self.key_reservations[(key as usize) % self.key_reservations.len()];
        reservation
            .compare_exchange(0, key, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| KeyReservation { reservation })
    }

    fn reserve_entry(&self) -> bool {
        let mut active = self.active_entries.load(Ordering::Acquire);
        loop {
            if active >= self.capacity {
                return false;
            }
            match self.active_entries.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => active = observed,
            }
        }
    }
}

impl From<IpAddr> for PeerRateKey {
    fn from(peer: IpAddr) -> Self {
        match peer {
            IpAddr::V4(peer) => Self {
                kind: 4,
                high: 0,
                low: u64::from(u32::from(peer)),
            },
            IpAddr::V6(peer) => {
                let octets = peer.octets();
                let high = u64::from_be_bytes(octets[..8].try_into().expect("IPv6 high half"));
                let low = u64::from_be_bytes(octets[8..].try_into().expect("IPv6 low half"));
                Self { kind: 6, high, low }
            }
        }
    }
}

impl Hash for PeerRateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.high.hash(state);
        self.low.hash(state);
    }
}

fn admit_existing(slot: &PeerRateSlot, limit: usize, tick: u32) -> NacellePeerRateLimitResult {
    let limit = u32::try_from(limit).unwrap_or(u32::MAX);
    loop {
        let current = slot.rate.load(Ordering::Acquire);
        let (window, count) = unpack_rate(current);
        let next = if window != tick {
            pack_rate(tick, 1)
        } else if count >= limit {
            slot.last_seen.store(tick, Ordering::Release);
            return NacellePeerRateLimitResult::RateLimited;
        } else {
            pack_rate(window, count + 1)
        };
        if slot
            .rate
            .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            slot.last_seen.store(tick, Ordering::Release);
            return NacellePeerRateLimitResult::Allowed;
        }
    }
}

fn pack_rate(window: u32, count: u32) -> u64 {
    (u64::from(window) << 32) | u64::from(count)
}

fn unpack_rate(rate: u64) -> (u32, u32) {
    ((rate >> 32) as u32, rate as u32)
}

fn status(state: u64) -> u64 {
    state & STATUS_MASK
}

fn readers(state: u64) -> u64 {
    (state & READERS_MASK) >> READERS_SHIFT
}

fn max_readers() -> u64 {
    READERS_MASK >> READERS_SHIFT
}

fn next_generation(state: u64) -> u64 {
    let generation = (state >> GENERATION_SHIFT) & GENERATION_MASK;
    generation.wrapping_add(1).max(1) & GENERATION_MASK
}

fn with_status(generation: u64, status: u64) -> u64 {
    (generation << GENERATION_SHIFT) | status
}

fn writer_state(state: u64) -> u64 {
    with_status(next_generation(state), WRITING)
}

fn expired(last_seen: u32, tick: u32) -> bool {
    tick.wrapping_sub(last_seen) >= RETENTION_SECONDS
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;

    use super::*;

    #[test]
    fn limits_one_peer_within_one_window() {
        let limiter = NacellePeerRateLimiter::new(4);
        let peer = "127.0.0.1".parse().expect("valid peer");

        assert_eq!(
            limiter.try_acquire_at(peer, 2, 7),
            NacellePeerRateLimitResult::Allowed
        );
        assert_eq!(
            limiter.try_acquire_at(peer, 2, 7),
            NacellePeerRateLimitResult::Allowed
        );
        assert_eq!(
            limiter.try_acquire_at(peer, 2, 7),
            NacellePeerRateLimitResult::RateLimited
        );
        assert_eq!(
            limiter.try_acquire_at(peer, 2, 8),
            NacellePeerRateLimitResult::Allowed
        );
    }

    #[test]
    fn full_table_rejects_new_peer_until_an_entry_expires() {
        let limiter = NacellePeerRateLimiter::new(1);
        let first = "127.0.0.1".parse().expect("valid peer");
        let second = "127.0.0.2".parse().expect("valid peer");

        assert_eq!(
            limiter.try_acquire_at(first, 1, 1),
            NacellePeerRateLimitResult::Allowed
        );
        assert_eq!(
            limiter.try_acquire_at(second, 1, 1),
            NacellePeerRateLimitResult::TableFull
        );
        assert_eq!(
            limiter.try_acquire_at(second, 1, RETENTION_SECONDS + 1),
            NacellePeerRateLimitResult::Allowed
        );
        assert_eq!(limiter.active_entries.load(Ordering::Acquire), 1);
    }

    #[test]
    fn expired_slot_is_not_replaced_while_a_reader_is_active() {
        let limiter = NacellePeerRateLimiter::new(1);
        let first = "127.0.0.1".parse().expect("valid peer");
        let second = "127.0.0.2".parse().expect("valid peer");

        assert_eq!(
            limiter.try_acquire_at(first, 1, 1),
            NacellePeerRateLimitResult::Allowed
        );

        let first_key = PeerRateKey::from(first);
        let first_slot =
            &limiter.slots[(limiter.hasher.hash_one(first_key) as usize) % limiter.slots.len()];
        let state = first_slot.state.load(Ordering::Acquire);
        let reader = first_slot
            .acquire_reader(state)
            .expect("initialized peer slot should acquire a reader");

        assert_eq!(
            limiter.try_acquire_at(second, 1, RETENTION_SECONDS + 1),
            NacellePeerRateLimitResult::TableFull
        );

        drop(reader);
        assert_eq!(
            limiter.try_acquire_at(second, 1, RETENTION_SECONDS + 1),
            NacellePeerRateLimitResult::Allowed
        );
    }

    #[test]
    fn distinguishes_ipv4_from_ipv4_mapped_ipv6() {
        let limiter = NacellePeerRateLimiter::new(2);
        let ipv4 = "127.0.0.1".parse().expect("valid IPv4 peer");
        let ipv6 = "::ffff:127.0.0.1".parse().expect("valid IPv6 peer");

        assert_eq!(
            limiter.try_acquire_at(ipv4, 1, 1),
            NacellePeerRateLimitResult::Allowed
        );
        assert_eq!(
            limiter.try_acquire_at(ipv6, 1, 1),
            NacellePeerRateLimitResult::Allowed
        );
    }

    #[test]
    fn same_key_reservation_exhaustion_fails_closed() {
        let limiter = NacellePeerRateLimiter::new(8);
        let peer = "127.0.0.1".parse().expect("valid peer");
        let key = PeerRateKey::from(peer);
        let reservation_key = limiter.hasher.hash_one(key).max(1);
        let reservation = limiter
            .try_reserve_key(reservation_key)
            .expect("test should reserve peer key");

        assert_eq!(
            limiter.try_acquire_at(peer, 3, 1),
            NacellePeerRateLimitResult::TableFull
        );

        drop(reservation);
        assert_eq!(
            limiter.try_acquire_at(peer, 3, 1),
            NacellePeerRateLimitResult::Allowed
        );
    }

    #[test]
    fn concurrent_admission_does_not_exceed_the_limit() {
        let rounds = if cfg!(miri) { 1 } else { 100 };
        for _ in 0..rounds {
            let limiter = Arc::new(NacellePeerRateLimiter::new(8));
            let barrier = Arc::new(Barrier::new(9));
            let peer = "127.0.0.1".parse().expect("valid peer");
            let mut threads = Vec::new();

            for _ in 0..8 {
                let limiter = limiter.clone();
                let barrier = barrier.clone();
                threads.push(std::thread::spawn(move || {
                    barrier.wait();
                    limiter.try_acquire_at(peer, 3, 1)
                }));
            }
            barrier.wait();

            let results: Vec<_> = threads
                .into_iter()
                .map(|thread| thread.join().expect("rate-limit thread should join"))
                .collect();
            let allowed = results
                .iter()
                .filter(|&&result| result == NacellePeerRateLimitResult::Allowed)
                .count();
            assert!(
                allowed <= 3,
                "admitted {allowed} requests, exceeding limit 3: {results:?}"
            );
        }
    }
}

//! Room actor runtime, event bus, opt-in tick.
//!
//! A Room is an actor (one Tokio task + a bounded mailbox) that exclusively owns a
//! user-defined state `S` and processes messages **sequentially** on `&mut S`, so game
//! logic never races without any locking (runtime §2, programming-model §1.7). Build one
//! with a [`RoomBuilder`]: register a handler per message type with
//! [`on`](RoomBuilder::on), then [`spawn`](RoomBuilder::spawn) to get a [`RoomHandle`].
//! Handlers respond through a [`RoomCtx`] ([`reply`](RoomCtx::reply) /
//! [`broadcast`](RoomCtx::broadcast) / [`send_to`](RoomCtx::send_to)), which the actor
//! turns into [`Outbound`] messages.
//!
//! This is the minimal Room: the `App::add_room` router integration, the event bus, tick,
//! and reconnect hooks arrive in later issues.
#![forbid(unsafe_code)]

mod room;

pub use room::{
    HandlerFuture, Outbound, PlayerId, RequestId, RoomBuilder, RoomClosed, RoomCtx, RoomHandle,
};

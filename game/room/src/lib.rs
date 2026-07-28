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
//! Framework lifecycle events (a player joining/leaving, a room opening/closing) flow
//! through the [`EventBus`] (runtime §4): any number of consumers — the game, service
//! layers, the observability layer — [`subscribe`](EventBus::subscribe) and receive each
//! [`RoomEvent`].
//!
//! This is the minimal Room: the `App::add_room` router integration, wiring producers to
//! the event bus, tick, and reconnect hooks arrive in later issues.
#![forbid(unsafe_code)]

mod event;
mod room;

pub use event::{EventBus, EventStreamError, EventSubscriber, RoomEvent};
pub use room::{
    HandlerFuture, Outbound, PlayerId, RequestId, RoomBuilder, RoomClosed, RoomCtx, RoomHandle,
};

//! Derive / attribute macros and neutral-schema generation.
#![forbid(unsafe_code)]

mod packet;

use proc_macro::TokenStream;

/// Derive `gsf_codec::Packet` for a serde message type, declaring its wire metadata.
///
/// ```ignore
/// #[derive(Serialize, Deserialize, Packet)]
/// #[packet(id = 7, channel = ReliableOrdered, visibility = Internal)]
/// struct PlayCard { card: u8 }
/// ```
///
/// - `id` — a `u16` packet id.
/// - `channel` — a `gsf_codec::ChannelKind` variant (e.g. `ReliableOrdered`).
/// - `visibility` — a `gsf_codec::Visibility` variant (`Internal` or `Public`).
///
/// A missing, unknown, or malformed attribute produces a `compile_error!`, never a panic.
#[proc_macro_derive(Packet, attributes(packet))]
pub fn derive_packet(input: TokenStream) -> TokenStream {
    packet::derive(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

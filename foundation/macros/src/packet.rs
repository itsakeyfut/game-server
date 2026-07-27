//! The `#[derive(Packet)]` implementation.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Ident, LitInt, parse2};

/// Expand `#[derive(Packet)]` into an `impl ::gsf_codec::Packet`.
///
/// The generated code hard-codes `::gsf_codec::*` paths, so it is coupled to the
/// crate keeping that name — if the `gsf-*` crate names (a TBD placeholder) change,
/// update these paths too. A `#[packet(crate = "...")]` escape hatch can be added if a
/// consumer ever needs to rename the dependency.
///
/// Returns `Err` (which the entry point turns into a `compile_error!`) on a missing,
/// unknown, duplicate, or malformed `#[packet(...)]` attribute, or a generic target —
/// it never panics.
pub fn derive(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = parse2(input)?;

    // A packet is a concrete `DeserializeOwned` message, so it has no generic
    // parameters: a generic `impl` cannot satisfy the `Serialize + DeserializeOwned`
    // supertrait bound (the type params would be unbounded), and one shared `const ID`
    // across instantiations would break the registry. Reject generics with a clear
    // message rather than emitting an `impl` that fails with a confusing serde error.
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "`#[derive(Packet)]` does not support generic types",
        ));
    }

    let PacketAttr {
        id,
        channel,
        visibility,
    } = PacketAttr::parse(&input)?;
    let ident = &input.ident;

    // `channel` / `visibility` idents pass straight through to enum-variant paths (the
    // compiler validates the variant name); `id` was validated as a `u16` above.
    Ok(quote! {
        impl ::gsf_codec::Packet for #ident {
            const ID: ::gsf_codec::PacketId = ::gsf_codec::PacketId(#id);
            const CHANNEL: ::gsf_codec::ChannelKind = ::gsf_codec::ChannelKind::#channel;
            const VISIBILITY: ::gsf_codec::Visibility = ::gsf_codec::Visibility::#visibility;
        }
    })
}

/// The parsed `#[packet(id = …, channel = …, visibility = …)]` attribute.
struct PacketAttr {
    id: u16,
    channel: Ident,
    visibility: Ident,
}

impl PacketAttr {
    fn parse(input: &DeriveInput) -> syn::Result<Self> {
        let mut id: Option<u16> = None;
        let mut channel: Option<Ident> = None;
        let mut visibility: Option<Ident> = None;
        let mut seen = false;

        for attr in &input.attrs {
            if !attr.path().is_ident("packet") {
                continue;
            }
            seen = true;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    if id.is_some() {
                        return Err(meta.error("duplicate `id`"));
                    }
                    // Validate as `u16` here so both out-of-range and suffixed literals
                    // fail at the `id` span with one consistent message.
                    id = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
                } else if meta.path.is_ident("channel") {
                    if channel.is_some() {
                        return Err(meta.error("duplicate `channel`"));
                    }
                    channel = Some(meta.value()?.parse()?);
                } else if meta.path.is_ident("visibility") {
                    if visibility.is_some() {
                        return Err(meta.error("duplicate `visibility`"));
                    }
                    visibility = Some(meta.value()?.parse()?);
                } else {
                    return Err(meta.error(
                        "unknown `packet` key (expected `id`, `channel`, or `visibility`)",
                    ));
                }
                Ok(())
            })?;
        }

        if !seen {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "`#[derive(Packet)]` requires `#[packet(id = …, channel = …, visibility = …)]`",
            ));
        }

        let missing = |key: &str| {
            syn::Error::new_spanned(&input.ident, format!("`#[packet(...)]` is missing `{key}`"))
        };
        Ok(Self {
            id: id.ok_or_else(|| missing("id"))?,
            channel: channel.ok_or_else(|| missing("channel"))?,
            visibility: visibility.ok_or_else(|| missing("visibility"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::derive;

    fn accepts(input: proc_macro2::TokenStream) -> bool {
        derive(input).is_ok()
    }
    fn rejects(input: proc_macro2::TokenStream) -> bool {
        derive(input).is_err()
    }

    #[test]
    fn derive_should_accept_a_complete_packet() {
        assert!(accepts(quote! {
            #[packet(id = 1, channel = ReliableOrdered, visibility = Internal)]
            struct Foo { x: u8 }
        }));
    }

    #[test]
    fn derive_should_reject_a_missing_attribute() {
        assert!(rejects(quote! { struct Foo { x: u8 } }));
    }

    #[test]
    fn derive_should_reject_a_missing_key() {
        assert!(rejects(quote! {
            #[packet(id = 1, channel = ReliableOrdered)]
            struct Foo { x: u8 }
        }));
    }

    #[test]
    fn derive_should_reject_an_unknown_key() {
        assert!(rejects(quote! {
            #[packet(id = 1, channel = ReliableOrdered, visibility = Internal, bogus = 2)]
            struct Foo { x: u8 }
        }));
    }

    #[test]
    fn derive_should_reject_a_duplicate_key() {
        assert!(rejects(quote! {
            #[packet(id = 1, id = 2, channel = ReliableOrdered, visibility = Internal)]
            struct Foo { x: u8 }
        }));
    }

    #[test]
    fn derive_should_reject_an_out_of_range_id() {
        assert!(rejects(quote! {
            #[packet(id = 70000, channel = ReliableOrdered, visibility = Internal)]
            struct Foo { x: u8 }
        }));
    }

    #[test]
    fn derive_should_reject_a_generic_type() {
        assert!(rejects(quote! {
            #[packet(id = 1, channel = ReliableOrdered, visibility = Internal)]
            struct Foo<T> { x: T }
        }));
    }
}

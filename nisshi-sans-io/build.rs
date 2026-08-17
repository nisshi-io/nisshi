// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use convert_case::{Case, Casing};
use nisshi_model::{CommonStruct, Field, Listener, Message, MessageKind, wv::Wv};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    env, error,
    fmt::{self, Display},
    fs,
    io::{self, BufRead, BufReader, Cursor, Seek, Write},
    path::Path,
};
use syn::{Expr, Type};

#[derive(Debug)]
#[allow(dead_code)]
enum Error {
    ExpectingArrayExpr(Box<Expr>),
    ExpectingPathExpr(Box<Expr>),
    ExpectingPathIdentExpr(Box<Expr>),
    ExpectingTupleExpr(Box<Expr>),
    Glob(glob::GlobError),
    Io(io::Error),
    Json(serde_json::Error),
    KafkaModel(nisshi_model::Error),
    Pattern(glob::PatternError),
    Syn(syn::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<glob::GlobError> for Error {
    fn from(value: glob::GlobError) -> Self {
        Error::Glob(value)
    }
}

impl From<glob::PatternError> for Error {
    fn from(value: glob::PatternError) -> Self {
        Error::Pattern(value)
    }
}

impl From<syn::Error> for Error {
    fn from(value: syn::Error) -> Self {
        Error::Syn(value)
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::Json(value)
    }
}

impl From<nisshi_model::Error> for Error {
    fn from(value: nisshi_model::Error) -> Self {
        Error::KafkaModel(value)
    }
}

impl error::Error for Error {}

type Result<T, E = Error> = std::result::Result<T, E>;

fn read_value<P>(filename: P) -> Result<Value>
where
    P: AsRef<Path>,
{
    fs::File::open(filename)
        .map(BufReader::new)
        .and_then(|r| {
            r.lines()
                .try_fold(Cursor::new(Vec::new()), |mut acc, line| {
                    if let Ok(mut line) = line {
                        if let Some(position) = line.find("//") {
                            line.truncate(position);
                        }
                        acc.write_all(line.as_bytes())?;
                    }
                    Ok(acc)
                })
        })
        .and_then(|mut r| r.rewind().map(|()| r))
        .and_then(|r| serde_json::from_reader(r).map_err(Into::into))
        .map_err(Into::into)
}

fn kind(
    parent: Option<&Field>,
    module: &syn::Path,
    f: &Field,
    dependencies: &[Type],
) -> TokenStream {
    let _ = (module, dependencies);

    #[cfg(feature = "diagnostics")]
    eprintln!(
        "module: {}, field: {}, dependencies: {:?}, primitive: {}, nullable.is_none: {}, \
         versions.is_mandatory: {}",
        module.to_token_stream(),
        f.name(),
        dependencies
            .iter()
            .map(|t| t.to_token_stream().to_string())
            .collect::<Vec<String>>(),
        f.kind().is_primitive(),
        f.nullable().is_none(),
        f.versions()
            .is_mandatory(parent.map(|parent| parent.versions()))
    );

    if f.tag().is_some() {
        let t = f.kind().type_name();

        if f.kind().is_sequence() {
            quote! {
                Option<Vec<#t>>
            }
        } else {
            quote! {
                Option<#t>
            }
        }
    } else if f.kind().is_sequence() {
        let t = f.kind().type_name();
        quote! {
            Option<Vec<#t>>
        }
    } else {
        let t = f.kind().type_name();
        if f.nullable().is_none() && f.versions().is_mandatory(parent.map(Field::versions)) {
            quote! {
                #t
            }
        } else {
            quote! {
                Option<#t>
            }
        }
    }
}

fn tag_kind(
    _parent: Option<&Field>,
    module: &syn::Path,
    f: &Field,
    _dependencies: &[Type],
) -> TokenStream {
    #[cfg(feature = "diagnostics")]
    eprintln!(
        "module: {}, field: {}, primitive: {}, nullable.is_none: {}",
        module.to_token_stream(),
        f.name(),
        f.kind().is_primitive(),
        f.nullable().is_none(),
    );

    if f.kind().is_sequence() {
        let t = f.kind().type_name();
        quote! {
            Vec<#module::#t>
        }
    } else if f.kind().is_primitive() {
        let t = f.kind().type_name();
        quote! {
            #t
        }
    } else {
        let t = f.kind().type_name();
        quote! {
            #module::#t
        }
    }
}

fn body_into_version(messages: &[Message]) -> TokenStream {
    let variants = messages
        .iter()
        .map(|message| {
            let name = message.type_name();

            quote! {
                Self::#name(message) => message.into_version(api_version).into(),
            }
        })
        .collect::<Vec<_>>();

    quote! {
        impl crate::IntoVersion for Body {
            fn into_version(self, api_version: i16) -> Self {
                match self {
                    #(#variants)*
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn body_enum(messages: &[Message], include_tag: bool) -> TokenStream {
    let variants = messages.iter().map(|message| {
        let name = message.type_name();
        let module =
            syn::parse_str::<syn::Path>(&name.to_token_stream().to_string().to_case(Case::Snake))
                .unwrap();

        quote! {
            #name(#module::#name)
        }
    });

    if include_tag {
        let from_mezzanine = {
            let conversions = messages.iter().map(|message| {
                let name = message.type_name();
                let module = syn::parse_str::<syn::Path>(
                    &name.to_token_stream().to_string().to_case(Case::Snake),
                )
                .unwrap();

                quote! {
                    mezzanine::Body::#name(inner) => {
                        Body::#name(crate::#module::#name::from(inner))
                    }
                }
            });

            quote! {
                impl From<mezzanine::Body> for Body {
                    fn from(value: mezzanine::Body) -> Self {
                        match value {
                            #(#conversions),*
                        }
                    }
                }
            }
        };

        quote! {
            #[non_exhaustive]
            #[derive(Clone, Debug, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            #[serde(from = "mezzanine::Body")]
            #[serde(into = "mezzanine::Body")]
            #[doc = "A Kafka API request or response message body."]
            pub enum Body {
                #(#variants),*
            }

            #from_mezzanine
        }
    } else {
        let from_tagged = {
            let conversions = messages.iter().map(|message| {
                let name = message.type_name();
                let module = syn::parse_str::<syn::Path>(
                    &name.to_token_stream().to_string().to_case(Case::Snake),
                )
                .unwrap();

                quote! {
                    crate::Body::#name(inner) => {
                        Body::#name(#module::#name::from(inner))
                    }
                }
            });

            quote! {
                impl From<crate::Body> for Body {
                    fn from(value: crate::Body) -> Self {
                        match value {
                            #(#conversions),*
                        }
                    }
                }
            }
        };

        quote! {
            #[derive(Clone, Debug, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            pub(crate) enum Body {
                #(#variants),*
            }

            #from_tagged
        }
    }
}

fn visibility_field_kind(
    parent: Option<&Field>,
    visibility: Option<&TokenStream>,
    fields: &[Field],
    module: &syn::Path,
    dependencies: &[Type],
    include_tag: bool,
) -> Vec<TokenStream> {
    fields
        .iter()
        .filter(|field| include_tag || field.tag().is_none())
        .map(|field| {
            let ident = field.ident();
            let kind = kind(parent, module, field, dependencies);

            field.about().map_or(
                quote! {
                    #visibility #ident: #kind
                },
                |about| {
                    let about = about.replace("[", "\\[").replace("]", "\\]");

                    quote! {
                        #[doc = #about]
                        #visibility #ident: #kind
                    }
                },
            )
        })
        .collect()
}

fn root_message_struct(message: &Message, include_tag: bool) -> TokenStream {
    let name = &message.type_name();
    let api_key = message.api_key();
    let fields = message.fields();
    let common_structs = message.common_structs();
    let message_name = message.name();

    let module =
        syn::parse_str::<syn::Path>(&name.to_token_stream().to_string().to_case(Case::Snake))
            .unwrap();

    let tokens = message_struct(&module, None, name, fields, common_structs, include_tag);

    if include_tag {
        quote! {
            pub mod #module {
                use super::*;

                #tokens

                impl From<#name> for Body {
                    fn from(value: #name) -> Body {
                        Body::#name(value)
                    }
                }

                impl TryFrom<Body> for #name {
                    type Error = Error;

                    fn try_from(value: Body) -> Result<Self, Self::Error> {
                        if let Body::#name(inner) = value {
                            Ok(inner)
                        } else {
                            Err(Error::UnexpectedType(format!("{value:?}")))
                        }
                    }
                }

                impl ApiKey for #name {
                    const KEY:i16 = #api_key;
                }

                impl ApiName for #name {
                    const NAME: &'static str = #message_name;
                }

            }

            pub use #module::#name;

        }
    } else {
        quote! {
            pub mod #module {
                #tokens
            }
        }
    }
}

fn message_struct_into_version(name: &Type, fields: &[Field]) -> TokenStream {
    if fields.iter().any(|f| f.tag().is_some()) {
        let none = fields.iter().filter(|f| f.tag().is_some()).map(|field| {
            let f = field.ident();
            let start = field.versions().start;
            let end = field.versions().end;

            quote! {
                if !(#start ..= #end).contains(&api_version) {
                    self.#f = None;
                }
            }
        });

        quote! {
            impl crate::IntoVersion for #name {
                fn into_version(mut self, api_version: i16) -> Self {
                    #(#none)*
                    self
                }
            }
        }
    } else {
        quote! {
            impl crate::IntoVersion for #name {
                fn into_version(self, _api_version: i16) -> Self {
                    self
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn message_struct(
    module: &syn::Path,
    parent: Option<&Field>,
    name: &Type,
    fields: &[Field],
    common_structs: Option<&[CommonStruct]>,
    include_tag: bool,
) -> TokenStream {
    let dependencies: Vec<Type> = fields
        .iter()
        .filter(|f| f.fields().is_some())
        .map(|f| f.kind().type_name())
        .chain(
            common_structs
                .unwrap_or(&[][..])
                .iter()
                .map(CommonStruct::type_name),
        )
        .collect();

    let token_streams: Vec<TokenStream> = fields
        .iter()
        .filter_map(|f| {
            f.fields().as_ref().map(|children| {
                message_struct(
                    module,
                    Some(f),
                    &f.kind().type_name(),
                    children,
                    None,
                    include_tag,
                )
            })
        })
        .chain(
            common_structs
                .unwrap_or(&[][..])
                .iter()
                .map(|cs| common_struct(parent, module, &cs.type_name(), cs.fields(), include_tag)),
        )
        .collect();

    let vfk = visibility_field_kind(
        parent,
        Some(&quote!(pub)),
        fields,
        module,
        &dependencies,
        include_tag,
    );

    let maximum_allocation_size = maximum_allocation_size(name, fields, include_tag);

    if include_tag {
        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!("crate::mezzanine::{}",module.to_token_stream())).unwrap(),
                    field,
                    &dependencies,
                );

                let tag = field.tag().unwrap();

                if field.kind().is_primitive() {
                    quote! {
                        let #f = value.tag_buffer.as_ref().and_then(|tag_buffer| tag_buffer.decode::<#k>(&#tag).ok().unwrap_or(None))
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        let #f = if let Some(tag_buffer) = value.tag_buffer.as_ref() {
                            if let Ok(Some(#f)) = tag_buffer.decode::<#k>(&#tag)
                            {
                                Some(#f.into_iter().map(Into::into).collect())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                } else {
                    quote! {
                        let #f = if let Some(tag_buffer) = value.tag_buffer.as_ref() {
                            if let Ok(Some(#f)) =
                                tag_buffer.decode::<#k>(&#tag)
                            {
                                Some(#f.into())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                }

            })
            .collect();

        let assignments = fields
            .iter()
            .map(|field| {
                let f = field.ident();

                if field.tag().is_some() {
                    quote! {
                        #f
                    }
                } else if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else if field.nullable().is_some() {
                    quote! {
                        #f: value.#f.map(|#f|#f.into())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect::<Vec<_>>();

        let builders = fields
            .iter()
            .map(|field| {
                let ident = field.ident();
                let kind = kind(parent, module, field, &dependencies);

                quote! {
                    pub fn #ident(mut self, #ident: #kind) -> Self {
                        self.#ident = #ident;
                        self
                    }
                }
            })
            .collect::<Vec<_>>();

        let mezzanine_name = syn::parse_str::<syn::Path>(&format!(
            "crate::mezzanine::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let from_mezzanine = (!fields.is_empty()).then(|| {
            quote! {
                impl From<#mezzanine_name> for #name {
                    fn from(value: #mezzanine_name) -> Self {
                        #(#tags;)*

                        Self {
                            #(#assignments,)*
                        }
                    }
                }
            }
        });

        let derived = if fields.iter().any(Field::has_float) {
            quote! {
                #[derive(Clone, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        };

        let visibility = if include_tag {
            quote! {
                pub
            }
        } else {
            quote! {
                pub(crate)
            }
        };

        let into_version = message_struct_into_version(name, fields);

        quote! {
            #[non_exhaustive]
            #derived
            #visibility struct #name {
                #(#vfk,)*
            }

            #from_mezzanine

            #maximum_allocation_size

            #into_version

            impl #name {
                #(#builders)*
            }

            #(#token_streams)*
        }
    } else {
        #[cfg(feature = "diagnostics")]
        eprintln!(
            "mezzanine, module: {}, name: {}",
            module.to_token_stream(),
            name.to_token_stream(),
        );

        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!(
                        "crate::mezzanine::{}",
                        module.to_token_stream()
                    ))
                    .unwrap(),
                    field,
                    &dependencies,
                );

                let tag = field.tag().unwrap();

                #[cfg(feature = "diagnostics")]
                eprintln!(
                    "mezzanine, module: {}, name: {}, field: {}",
                    module.to_token_stream(),
                    name.to_token_stream(),
                    f.to_token_stream(),
                );

                if field.kind().is_primitive() {
                    quote! {
                        if let Some(#f) = value.#f
                            && let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &#f) {
                            tag_buffer.push(encoded);
                        }
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into_iter().map(Into::into).collect();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                }
            })
            .collect();

        let tag_capacity = tags.len();

        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else if field.nullable().is_some() {
                    quote! {
                        #f: value.#f.map(|#f|#f.into())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let tagged_name = syn::parse_str::<syn::Path>(&format!(
            "crate::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let from_tagged = (!fields.is_empty()).then(|| {
            quote! {
                impl From<#tagged_name> for #name {
                    fn from(value: #tagged_name) -> Self {
                        #[allow(unused_mut)]
                        let mut tag_buffer = Vec::with_capacity(#tag_capacity);

                        #(#tags)*

                        Self {
                            #(#assignments,)*
                            tag_buffer: Some(tag_buffer.into()),
                        }
                    }
                }
            }
        });

        let derived = if fields.iter().any(Field::has_float) {
            quote! {
                #[derive(Clone, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        };

        quote! {
            #derived
            pub(crate) struct #name {
                #(#vfk,)*
                pub tag_buffer: Option<crate::primitive::tagged::TagBuffer>,
            }

            #from_tagged

            #(#token_streams)*
        }
    }
}

fn maximum_allocation_size(name: &Type, fields: &[Field], include_tag: bool) -> TokenStream {
    let sizes = fields
        .iter()
        .filter(|field| include_tag || field.tag().is_none())
        .map(|field| {
            let f = field.ident();

            quote! {
                total += self.#f.maximum_allocation_size()?
            }
        })
        .collect::<Vec<_>>();

    quote! {
        impl crate::MaximumAllocationSize for #name {
            fn maximum_allocation_size(&self) -> Result<usize, crate::Error> {
                let mut total:usize = 0;

                #(#sizes;)*

                Ok(total)
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn common_struct(
    parent: Option<&Field>,
    module: &syn::Path,
    name: &Type,
    fields: &[Field],
    include_tag: bool,
) -> TokenStream {
    let vis = quote!(pub);
    let vfk = visibility_field_kind(parent, Some(&vis), fields, module, &[], include_tag);
    let maximum_allocation_size = maximum_allocation_size(name, fields, include_tag);

    if include_tag {
        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| include_tag || field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let builders = fields
            .iter()
            .map(|field| {
                let ident = field.ident();
                let kind = kind(parent, module, field, &[]);

                quote! {
                    pub fn #ident(mut self, #ident: #kind) -> Self {
                        self.#ident = #ident;
                        self
                    }
                }
            })
            .collect::<Vec<_>>();

        let from = syn::parse_str::<syn::Path>(&format!(
            "crate::mezzanine::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let derived = if fields.iter().any(Field::has_float) {
            quote! {
                #[derive(Clone, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        };

        let visibility = if include_tag {
            quote! {
                pub
            }
        } else {
            quote! {
                pub(crate)
            }
        };

        quote! {
            #[non_exhaustive]
            #derived
            #visibility struct #name {
                #(#vfk,)*
            }

            impl #name {
                #(#builders)*
            }

            #maximum_allocation_size

            impl From<#from> for #name {
                fn from(value: #from) -> Self {
                    Self {
                        #(#assignments,)*
                    }
                }
            }
        }
    } else {
        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!(
                        "crate::mezzanine::{}",
                        module.to_token_stream()
                    ))
                    .unwrap(),
                    field,
                    &[],
                );

                let tag = field.tag().unwrap();

                #[cfg(feature = "diagnostics")]
                eprintln!(
                    "mezzanine, module: {}, name: {}, field: {}",
                    module.to_token_stream(),
                    name.to_token_stream(),
                    f.to_token_stream(),
                );

                if field.kind().is_primitive() {
                    quote! {
                        if let Some(#f) = value.#f {
                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &#f) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into_iter().map(Into::into).collect();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                }
            })
            .collect();

        let tag_capacity = tags.len();

        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let from = syn::parse_str::<syn::Path>(&format!(
            "crate::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let derived = if fields.iter().any(Field::has_float) {
            quote! {
                #[derive(Clone, Debug, Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        } else {
            quote! {
                #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            }
        };

        quote! {
            #derived
            pub(crate) struct #name {
                #(#vfk,)*
                pub tag_buffer: Option<crate::primitive::tagged::TagBuffer>,
            }

            impl From<#from> for #name {
                fn from(value: #from) -> Self {
                    #[allow(unused_mut)]
                    let mut tag_buffer = Vec::with_capacity(#tag_capacity);

                    #(#tags)*

                    Self {
                        #(#assignments,)*
                        tag_buffer: Some(tag_buffer.into()),
                    }
                }
            }

        }
    }
}

fn root(messages: &[Message], include_tag: bool) -> Vec<TokenStream> {
    messages
        .iter()
        .map(|message| root_message_struct(message, include_tag))
        .collect()
}

fn process(messages: &[Message], include_tag: bool) -> TokenStream {
    let body_enum = body_enum(messages, include_tag);
    let root = root(messages, include_tag);

    if include_tag {
        let as_names = messages
            .iter()
            .map(|message| {
                let name = message.type_name();

                let module = syn::parse_str::<syn::Path>(
                    &name.to_token_stream().to_string().to_case(Case::Snake),
                )
                .unwrap_or_else(|_| panic!("module: {}", &name.to_token_stream().to_string()));

                let as_name = syn::parse_str::<syn::Path>(
                    &format!("As{}", name.to_token_stream()).to_case(Case::Snake),
                )
                .unwrap();

                quote! {
                    pub fn #as_name(self) -> Option<#module::#name> {
                        if let Self::#name(value) = self {
                            Some(value)
                        } else {
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        let request_responses = {
            let mapping = {
                let mut mapping: BTreeMap<i16, (Option<Type>, Option<Type>)> = BTreeMap::new();

                for message in messages.iter() {
                    _ = mapping
                        .entry(message.api_key())
                        .and_modify(|entry| match message.kind() {
                            MessageKind::Request => {
                                assert_eq!(entry.0.replace(message.type_name()), None)
                            }

                            MessageKind::Response => {
                                assert_eq!(entry.1.replace(message.type_name()), None)
                            }
                        })
                        .or_insert(match message.kind() {
                            MessageKind::Request => (Some(message.type_name()), None),

                            MessageKind::Response => (None, Some(message.type_name())),
                        });
                }

                mapping
            };

            mapping
                .into_iter()
                .filter(|(_, (request, response))| request.is_some() && response.is_some())
                .map(|(_, (request, response))| {
                    quote! {
                        impl Request for #request {
                            type Response = #response;
                        }

                        impl Response for #response {
                            type Request = #request;
                        }
                    }
                })
                .collect::<Vec<_>>()
        };

        let matchers = messages
            .iter()
            .filter(|message| message.kind() == MessageKind::Request)
            .map(|message| {
                let name = message.type_name();

                quote! {
                    impl<State> rama::matcher::Matcher<State, Frame> for #name {
                        fn matches(
                            &self,
                            ext: Option<&mut rama::context::Extensions>,
                            ctx: &rama::Context<State>,
                            req: &Frame,
                        ) -> bool {
                            req.api_key().is_ok_and(|api_key| api_key == Self::KEY)
                        }
                    }

                    impl<State, T> rama::matcher::Matcher<State, T> for #name
                    where
                        T: ApiKey,
                    {
                        fn matches(
                            &self,
                            ext: Option<&mut rama::context::Extensions>,
                            ctx: &rama::Context<State>,
                            req: &T,
                        ) -> bool {
                            T::KEY == Self::KEY
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        let api_keys = messages
            .iter()
            .map(|message| {
                let name = message.type_name();

                quote! {
                    Self::#name(_) => #name::KEY,
                }
            })
            .collect::<Vec<_>>();

        let api_names = messages
            .iter()
            .map(|message| {
                let name = message.type_name();

                quote! {
                    Self::#name(_) => #name::NAME,
                }
            })
            .collect::<Vec<_>>();

        let maximum_allocation_size = messages
            .iter()
            .map(|message| {
                let name = message.type_name();

                quote! {
                    Self::#name(message) => message.maximum_allocation_size(),
                }
            })
            .collect::<Vec<_>>();

        let api_versions = body_into_version(messages);

        quote! {
            #(#root)*

            #body_enum

            #(#request_responses)*

            #(#matchers)*

            impl Body {
                #(#as_names)*

                pub fn api_key(&self) -> i16 {
                    match self {
                        #(#api_keys)*
                    }
                }

                pub fn api_name(&self) -> &str {
                    match self {
                        #(#api_names)*
                    }
                }
            }

            impl crate::MaximumAllocationSize for Body {
                fn maximum_allocation_size(&self) -> Result<usize, crate::Error> {
                    match self {
                        #(#maximum_allocation_size)*
                    }
                }
            }

            #api_versions
        }
    } else {
        quote! {
            mod mezzanine {
                #(#root)*

                #body_enum
            }
        }
    }
}

fn all(pattern: &str) -> Result<Vec<Message>> {
    glob::glob(pattern).map_err(Into::into).and_then(|paths| {
        paths
            .map(|path| {
                path.map_err(Into::into)
                    .inspect(|path| println!("cargo::rerun-if-changed={}", path.display()))
                    .and_then(read_value)
                    .and_then(|v| Message::try_from(&Wv::from(&v)).map_err(Into::into))
            })
            .collect::<Result<Vec<_>>>()
    })
}

fn each_field_meta(
    field: &Field,
    common_structs: &Option<HashMap<Type, &CommonStruct>>,
) -> TokenStream {
    let name = field.name().to_case(Case::Snake);
    let version = field.versions();
    let nullable = OptionWrapper::from(field.nullable());
    let kind = field.kind().name();
    let tag = OptionWrapper::from(field.tag());
    let tagged = OptionWrapper::from(field.tagged());

    let children = field.fields().as_ref().map_or_else(
        || {
            common_structs.as_ref().map_or(Vec::new(), |m| {
                m.get(&field.kind().type_name()).map_or(Vec::new(), |cs| {
                    cs.fields()
                        .iter()
                        .map(|f| each_field_meta(f, common_structs))
                        .collect()
                })
            })
        },
        |fields| {
            fields
                .iter()
                .map(|f| each_field_meta(f, common_structs))
                .collect()
        },
    );

    quote! {
        (#name,
        &nisshi_model::FieldMeta {
            version: #version,
            nullable: #nullable,
            kind: nisshi_model::KindMeta(#kind),
            tag: #tag,
            tagged: #tagged,
            fields: &[#(#children),*],
        })
    }
}

fn each_message_meta(message: &Message) -> TokenStream {
    let name = message.name();
    let api_key = message.api_key();
    let version = message.version();
    let message_kind = message.kind();

    let common_structs = message
        .common_structs()
        .as_ref()
        .map(|v| v.iter().map(|cs| (cs.type_name(), cs)))
        .map(HashMap::from_iter);

    let children = message
        .fields()
        .iter()
        .map(|f| each_field_meta(f, &common_structs));

    quote! {
        (#name,
        &nisshi_model::MessageMeta {
            name: #name,
            api_key: #api_key,
            version: #version,
            message_kind: #message_kind,
            fields: &[#(#children),*],
        })
    }
}

fn message_meta(messages: &[Message]) -> TokenStream {
    let len = messages.len();

    let meta = messages.iter().map(each_message_meta);

    quote! {
        pub static MESSAGE_META : [(&str, &nisshi_model::MessageMeta); #len] = [#(#meta, )*];
    }
}

pub fn main() {
    let files = "message/[A-Z]*Re[qs]*.json";

    let messages = all(files).unwrap_or_else(|e| panic!("all: {e:?}"));

    let broker_api_keys = messages
        .iter()
        .filter(|m| {
            m.listeners()
                .is_some_and(|listeners| listeners.contains(&Listener::Broker))
        })
        .map(|m| m.api_key())
        .collect::<Vec<_>>();

    let broker_messages = messages
        .into_iter()
        .filter(|message| {
            broker_api_keys.contains(&message.api_key()) && !message.fields().is_empty()
        })
        .collect::<Vec<_>>();

    let tagged = process(&broker_messages, true);
    let untagged = process(&broker_messages, false);

    let message_meta = message_meta(&broker_messages);

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("generate.rs");

    let q = quote! {
        #tagged
        #untagged
        #message_meta
    };

    let r = syn::parse_file(&q.to_string()).unwrap_or_else(|_| panic!("{}", q.to_string()));

    fs::write(&dest_path, prettyplease::unparse(&r)).unwrap();

    println!("cargo::rerun-if-changed=build.rs");
}

struct OptionWrapper<T>(Option<T>);

impl<T> From<Option<T>> for OptionWrapper<T> {
    fn from(value: Option<T>) -> Self {
        Self(value)
    }
}

impl<T: ToTokens> ToTokens for OptionWrapper<T> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        if let Some(ref v) = self.0 {
            tokens.extend(quote! {
                Some(#v)
            });
        } else {
            tokens.extend(quote! {
                None
            });
        }
    }
}

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

use std::{collections::HashMap, env::vars, fmt, result, str::FromStr};

mod cli;

pub use cli::Cli;
use regex::{Regex, Replacer};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Box(#[from] Box<dyn std::error::Error + Send + Sync>),
    Cat(Box<nisshi_cat::Error>),
    Client(Box<nisshi_client::Error>),
    DotEnv(#[from] dotenv::Error),
    Generate(#[from] nisshi_generator::Error),
    InvalidLength(#[from] sha2::digest::InvalidLength),
    Perf(#[from] nisshi_perf::Error),
    Proxy(#[from] nisshi_proxy::Error),
    Regex(#[from] regex::Error),
    SansIo(#[from] nisshi_sans_io::Error),
    Schema(Box<nisshi_schema::Error>),
    Server(Box<nisshi_broker::Error>),
    Tls(#[from] rustls::Error),
    TlsPkiPem(#[from] rustls::pki_types::pem::Error),
    Topic(#[from] nisshi_topic::Error),
    Url(#[from] url::ParseError),
}

impl From<nisshi_cat::Error> for Error {
    fn from(value: nisshi_cat::Error) -> Self {
        Self::Cat(Box::new(value))
    }
}

impl From<nisshi_client::Error> for Error {
    fn from(value: nisshi_client::Error) -> Self {
        Self::Client(Box::new(value))
    }
}

impl From<nisshi_schema::Error> for Error {
    fn from(value: nisshi_schema::Error) -> Self {
        Self::Schema(Box::new(value))
    }
}

impl From<nisshi_broker::Error> for Error {
    fn from(value: nisshi_broker::Error) -> Self {
        Self::Server(Box::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct VarRep(HashMap<String, String>);

impl From<HashMap<String, String>> for VarRep {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value)
    }
}

impl VarRep {
    fn replace(&self, haystack: &str) -> Result<String> {
        Regex::new(r"\$\{(?<var>[^\}]+)\}")
            .map(|re| re.replace(haystack, self).into_owned())
            .map_err(Into::into)
    }
}

impl Replacer for &VarRep {
    fn replace_append(&mut self, caps: &regex::Captures<'_>, dst: &mut String) {
        if let Some(variable) = caps.name("var")
            && let Some(value) = self.0.get(variable.as_str())
        {
            dst.push_str(value);
        }
    }
}

#[derive(Clone, Debug)]
pub struct EnvVarExp<T>(T);

impl<T> EnvVarExp<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromStr for EnvVarExp<T>
where
    T: FromStr,
    Error: From<<T as FromStr>::Err>,
{
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        VarRep::from(vars().collect::<HashMap<_, _>>())
            .replace(s)
            .and_then(|s| T::from_str(&s).map_err(Into::into))
            .map(|t| Self(t))
    }
}

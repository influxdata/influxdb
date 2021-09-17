#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use crate::descriptor::{Descriptor, DescriptorSet, Package};
use crate::generator::{generate_enum, generate_message, Config};
use crate::message::resolve_message;
use std::io::{BufWriter, Error, ErrorKind, Result, Write};
use std::path::PathBuf;

mod descriptor;
mod escape;
mod generator;
mod message;

#[derive(Debug, Default)]
pub struct Builder {
    descriptors: descriptor::DescriptorSet,
    out_dir: Option<PathBuf>,
}

impl Builder {
    /// Create a new `Builder`
    pub fn new() -> Self {
        Self {
            descriptors: DescriptorSet::new(),
            out_dir: None,
        }
    }

    /// Register an encoded `FileDescriptorSet` with this `Builder`
    pub fn register_descriptors(&mut self, descriptors: &[u8]) -> Result<&mut Self> {
        self.descriptors.register_encoded(descriptors)?;
        Ok(self)
    }

    /// Generates code for all registered types where `prefixes` contains a prefix of
    /// the fully-qualified path of the type
    pub fn build<S: AsRef<str>>(&mut self, prefixes: &[S]) -> Result<()> {
        let mut output: PathBuf = self.out_dir.clone().map(Ok).unwrap_or_else(|| {
            std::env::var_os("OUT_DIR")
                .ok_or_else(|| {
                    Error::new(ErrorKind::Other, "OUT_DIR environment variable is not set")
                })
                .map(Into::into)
        })?;
        output.push("FILENAME");

        let write_factory = move |package: &Package| {
            output.set_file_name(format!("{}.serde.rs", package));

            let file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&output)?;

            Ok(BufWriter::new(file))
        };

        let writers = generate(&self.descriptors, prefixes, write_factory)?;
        for (_, mut writer) in writers {
            writer.flush()?;
        }

        Ok(())
    }
}

fn generate<S: AsRef<str>, W: Write, F: FnMut(&Package) -> Result<W>>(
    descriptors: &DescriptorSet,
    prefixes: &[S],
    mut write_factory: F,
) -> Result<Vec<(Package, W)>> {
    let config = Config {
        extern_types: Default::default(),
    };

    let iter = descriptors.iter().filter(move |(t, _)| {
        prefixes
            .iter()
            .any(|prefix| t.matches_prefix(prefix.as_ref()))
    });

    // Exploit the fact descriptors is ordered to group together types from the same package
    let mut ret: Vec<(Package, W)> = Vec::new();
    for (type_path, descriptor) in iter {
        let writer = match ret.last_mut() {
            Some((package, writer)) if package == type_path.package() => writer,
            _ => {
                let package = type_path.package();
                ret.push((package.clone(), write_factory(package)?));
                &mut ret.last_mut().unwrap().1
            }
        };

        match descriptor {
            Descriptor::Enum(descriptor) => generate_enum(&config, type_path, descriptor, writer)?,
            Descriptor::Message(descriptor) => {
                if let Some(message) = resolve_message(descriptors, descriptor) {
                    generate_message(&config, &message, writer)?
                }
            }
        }
    }

    Ok(ret)
}

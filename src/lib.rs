/*!
Store binary blobs with hypercore. Inspired by the [JavaScript implementation](https://github.com/holepunchto/hyperblobs).
```rust
# use rust::add;
assert_eq!(add(1, 2), 3);
```
*/
#![warn(
    missing_debug_implementations,
    missing_docs,
    redundant_lifetimes,
    non_local_definitions,
    unsafe_code,
    non_local_definitions
)]

use std::time::Duration;

use derive_builder::Builder;
//use hypercore::replication::CoreMethods;
use tracing::instrument;

const DEFAULT_BLOCK_SIZE: u64 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
/// Errors for [`Hyperblobs`].
pub enum Error {}
type Result<T> = std::result::Result<T, Error>;

/// Options for [`Hyperblobs::put`]
#[derive(Debug, Clone)]
pub struct PutOptions {
    /// The block size that will be used when storing blobs.
    /// TODO What happens when this conflicts with [`HyperblobsBuilder::block_size`]
    pub block_size: u64,
    /// Relative offset to start within the blob
    pub start: u64,
    /// End offset within the blob
    pub end: u64,
    /// number of bytes to read
    pub length: u64,
}

/// Result of [`Hyperblobs::put`] used for fetching data with [`Hyperblobs::get`].
#[derive(Debug, Clone)]
pub struct BlobId {
    /// total byte offset? or relative to starting block
    pub byte_offset: u64,
    /// starting block of the putted data
    pub block_offset: u64,
    /// length in blocks of the putted data
    pub block_length: u64,
    /// byte length of put data
    pub byte_length: u64,
}

/// Options for [`Hyperblobs::get`].
#[derive(Debug, Clone, Builder)]
pub struct GetOptions {
    /// How long to wait for a block to download. If `None`, do not download.
    /// NB: this default comes from JS, where `wait = true` but `timeout = 0`.
    /// How is that different from just `wait = false`?
    /// That combo seems like it has the effect of not returning the block when not local
    /// but still sending out a request for the block, so it would get downloaded for later?
    /// TODO experiment and explore js to figure this out
    pub timeout: Option<Duration>,
}

#[derive(Debug, Builder)]
/// Blob store for hypercore
pub struct Hyperblobs {
    /// The block size that will be used when storing blobs
    #[builder(default = "DEFAULT_BLOCK_SIZE")]
    block_size: u64,
}

impl Hyperblobs {
    /// Insert a blob into the store
    #[instrument]
    pub async fn put(&self, _blob: &[u8], _opts: &PutOptions) -> BlobId {
        todo!()
    }

    /// Get a blob from the store
    // TODO should this return a Option?
    #[instrument]
    pub async fn get(&self, _id: &BlobId, _opts: &GetOptions) -> Result<Vec<u8>> {
        todo!()
    }

    /// Stream over the requested blob, in-order, as they are received
    pub async fn read_stream(&self, _id: &BlobId, _opts: &GetOptions) -> Result<()> {
        // possible return sig adapted from Hyperbeee
        //) -> Result<impl toko_stream::Stream<Item = Vec<u8> + 'a>>
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        todo!()
    }
}

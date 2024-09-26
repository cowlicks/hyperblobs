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

use std::{ops::Rem, time::Duration};

use derive_builder::Builder;
use hypercore::replication::{CoreMethods, CoreMethodsError};
use replicator::ReplicatingCore;
use tracing::instrument;

const DEFAULT_BLOCK_SIZE: u64 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
/// Errors for [`Hyperblobs`].
pub enum Error {
    #[error("Error in CoreMethods")]
    /// Error in CoreMethods
    CoreMethodsError(#[from] CoreMethodsError),
}
type Result<T> = std::result::Result<T, Error>;

/// Options for [`Hyperblobs::put`]
#[derive(Debug, Clone, Default, Builder)]
pub struct PutOptions {
    /// The block size that will be used when storing blobs.
    /// TODO What happens when this conflicts with [`HyperblobsBuilder::block_size`]
    #[builder(default = "Some(DEFAULT_BLOCK_SIZE)")]
    pub block_size: Option<u64>,
    // TODO these just seem to be various ways to represent a range
    // remove them for now. Experiment with js and see what happens when
    // - end & length conflict
    /// Relative offset to start within the blob
    pub start: Option<u64>,
    /// number of bytes to read
    pub length: Option<u64>,
    /*
    /// End offset within the blob
    pub end: u64,
    */
}

/// Options for [`Hyperblobs::get`].
#[derive(Debug, Clone, Default)]
pub struct GetOptions {
    /// How long to wait for a block to download. If `None`, do not download.
    /// NB: this default comes from JS, where `wait = true` but `timeout = 0`.
    /// How is that different from just `wait = false`?
    /// That combo seems like it has the effect of not returning the block when not local
    /// but still sending out a request for the block, so it would get downloaded for later?
    /// TODO experiment and explore js to figure this out
    pub timeout: Option<Duration>,
}

/// Result of [`Hyperblobs::put`] used for fetching data with [`Hyperblobs::get`].
#[derive(Debug, Clone)]
pub struct PutBlob {
    /// Number of bytes before the blob
    pub byte_offset: u64,
    /// starting block of the putted data
    pub block_offset: u64,
    /// length in blocks of the putted data
    pub block_length: u64,
    /// length of put data in bytes
    pub byte_length: u64,
}

/// Arguments for getting a blob
#[derive(Debug, Clone)]
pub struct GetBlob {
    /// starting block of the putted data
    pub block_offset: u64,
    /// Number of bytes to get
    pub byte_length: u64,
}

impl From<&PutBlob> for GetBlob {
    fn from(
        &PutBlob {
            block_offset,
            byte_length,
            ..
        }: &PutBlob,
    ) -> Self {
        Self {
            block_offset,
            byte_length,
        }
    }
}

// this is dumb
impl From<&GetBlob> for GetBlob {
    fn from(value: &GetBlob) -> Self {
        value.clone()
    }
}

#[allow(missing_docs)]
#[derive(Debug, Builder)]
/// Blob store for hypercore
pub struct Hyperblobs {
    /// The block size that will be used when storing blobs
    #[builder(default = "DEFAULT_BLOCK_SIZE")]
    block_size: u64,
    core: ReplicatingCore,
}

fn u64_from_usize(x: usize) -> u64 {
    u64::try_from(x).expect("TODO")
}

impl Hyperblobs {
    /// Insert a blob into the store
    #[instrument]
    pub async fn put(&self, blob: &[u8], _opts: &PutOptions) -> Result<PutBlob> {
        let byte_length = u64_from_usize(blob.len());

        let blocks: Vec<Vec<u8>> = blob
            .chunks(self.block_size.try_into().expect("TODO"))
            .map(Vec::from)
            .collect();

        let block_length = u64_from_usize(blocks.len());

        let res = self.core.append_batch(&blocks).await?;

        let out = PutBlob {
            byte_offset: res.byte_length - u64::try_from(byte_length).expect("TODO"),
            block_offset: res.length - block_length,
            block_length,
            byte_length,
        };
        Ok(out)
    }

    /// Get a blob from the store
    /// TODO what happens when the data in BlobId conflicts with itself? like the byte_length
    /// doesn't match the block_length?
    /// * what should happen when bid.block_offset > core.info().length
    /// * what about when bid.block_offset < cor.length but block_offset + block length >
    /// core.length
    /// * what about when byte_length doesn't match the blocks?
    ///   greater than? less than?
    /// Answer: it looks like only consider block_offset and byte_length
    /// looking at the code, block_offset is just used for prefetching
    /// byte_offset (without opts.start/end
    #[instrument]
    pub async fn get<T: Into<GetBlob> + std::fmt::Debug>(
        &self,
        id: T,
        _opts: &GetOptions,
    ) -> Result<Vec<u8>> {
        let GetBlob {
            byte_length,
            block_offset,
        } = id.into();
        let mut out = vec![];
        let n_blocks = byte_length.div_ceil(self.block_size);
        dbg!();
        for bi in block_offset..(block_offset + n_blocks) {
            match self.core.get(bi).await? {
                // TODO how should we handle this
                None => break,
                Some(block) => {
                    dbg!(bi);
                    if bi == block_offset + n_blocks - 1 {
                        dbg!(self.block_size);
                        let x = byte_length.rem(self.block_size);
                        dbg!(&x);
                        let x = usize::try_from(x).expect("TODO");
                        dbg!(&x);
                        out.extend_from_slice(&block[..x]);
                    } else {
                        out.extend(block);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Stream over the requested blob, in-order, as they are received
    pub async fn read_stream(&self, _id: &PutBlob, _opts: &GetOptions) -> Result<()> {
        // possible return sig adapted from Hyperbeee
        //) -> Result<impl toko_stream::Stream<Item = Vec<u8> + 'a>>
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypercore::{HypercoreBuilder, Storage};

    async fn ram_blobs(block_size: u64) -> Hyperblobs {
        let core = HypercoreBuilder::new(Storage::new_memory().await.unwrap())
            .build()
            .await
            .unwrap();
        HyperblobsBuilder::default()
            .block_size(block_size)
            .core(core.into())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let blobs = ram_blobs(2).await;
        let data = b"Hello, world!";
        let id = blobs.put(data, &Default::default()).await?;
        assert_eq!(id.byte_offset, 0);
        assert_eq!(id.block_offset, 0);
        assert_eq!(id.block_length, data.len().div_ceil(2) as u64);
        assert_eq!(id.byte_length, data.len() as u64);

        let res = blobs.get(&id, &Default::default()).await?;
        assert_eq!(res, data);

        let res = blobs
            .get(
                &GetBlob {
                    block_offset: 0,
                    byte_length: 5,
                },
                &Default::default(),
            )
            .await?;
        assert_eq!(res, b"Hello");

        let data2 = b"More data!!!!?";
        let id = blobs.put(data2, &Default::default()).await?;
        dbg!(&id);
        let res = blobs.get(&id, &Default::default()).await?;
        println!("{}", String::from_utf8_lossy(&res));
        assert_eq!(res, data2);

        Ok(())
    }
}

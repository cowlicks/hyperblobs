/*!
Store binary blobs with hypercore. Inspired by the [JavaScript implementation](https://github.com/holepunchto/hyperblobs).
```rust
# println!("This line not shown in the blocks");
println!("This line is");
```
*/
#![warn(
    missing_debug_implementations,
    missing_docs,
    redundant_lifetimes,
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
    /// Relative offset to start within the blob
    pub start: Option<u64>,
    /// number of bytes to read
    pub length: Option<u64>,
    // We omit the `end` and `block_size` options that JS has.
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

impl HyperblobsBuilder {
    //fn storage(&mut self, store: Storage)
}

impl Hyperblobs {
    /// Insert a blob into the store
    #[instrument]
    pub async fn put(&self, blob: &[u8], _opts: &PutOptions) -> Result<PutBlob> {
        let blocks: Vec<Vec<u8>> = blob
            .chunks(self.block_size as usize)
            .map(Vec::from)
            .collect();

        let byte_length = blob.len() as u64;
        let block_length = blocks.len() as u64;

        let res = self.core.append_batch(&blocks).await?;

        let out = PutBlob {
            byte_offset: res.byte_length - byte_length,
            block_offset: res.length - block_length,
            block_length,
            byte_length,
        };
        Ok(out)
    }

    /// Get a blob from the store
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
        for block_index in block_offset..(block_offset + n_blocks) {
            match self.core.get(block_index).await? {
                None => todo!("How to handle this?"),
                Some(block) => {
                    // if last block
                    if block_index == block_offset + n_blocks - 1 {
                        // only take up to the requested byte length
                        let rem = byte_length.rem(self.block_size);
                        if rem != 0 {
                            out.extend_from_slice(&block[..(rem as usize)]);
                        }  else {
                            out.extend(block);
                        }
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
        let id2 = blobs.put(data2, &Default::default()).await?;
        assert_eq!(id2.byte_offset, id.byte_length);
        assert_eq!(id2.block_offset, id.block_length);
        assert_eq!(id2.block_length, data2.len().div_ceil(2) as u64);
        dbg!(id2.block_length);
        assert_eq!(id2.byte_length, data2.len() as u64);
        dbg!(&id2);
        let res = blobs.get(&id2, &Default::default()).await?;
        println!("{}", String::from_utf8_lossy(&res));
        assert_eq!(res, data2);

        Ok(())
    }

    #[tokio::test]
    async fn remainders() -> Result<()> {
        let blobs = ram_blobs(3).await;

        let data = b"abcefg";
        let id = blobs.put(data, &Default::default()).await?;
        let res = blobs.get(&id, &Default::default()).await?;
        assert_eq!(res, data);

        let data = b"abcef";
        let id = blobs.put(data, &Default::default()).await?;
        let res = blobs.get(&id, &Default::default()).await?;
        assert_eq!(res, data);

        let data = b"abce";
        let id = blobs.put(data, &Default::default()).await?;
        let res = blobs.get(&id, &Default::default()).await?;
        assert_eq!(res, data);
        Ok(())
    }
}

#![allow(dead_code)]

use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use shiplift::{image::ImageBuildChunk, PullOptions};
use std::{future::Future, io};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Default)]
pub struct Docker {
	docker: shiplift::Docker,
}
impl Docker {
	pub fn new() -> Self {
		Self::default()
	}

	/// Pull images. Rule of thumb for parallelism: thread::available_parallelism().unwrap().get()
	pub async fn images_pull(&self, images: Vec<String>, parallelism: usize) -> Result<(), shiplift::Error> {
		stream::iter(images.iter().map(|image| async move {
			let (image, tag) = image.rsplit_once(':').expect("image name must contain a tag");
			let opts = PullOptions::builder().image(image).tag(tag).build();
			for chunk in self.docker.images().pull(&opts).try_collect::<Vec<_>>().await? {
				match chunk {
					ImageBuildChunk::Update { stream: _ }
					| ImageBuildChunk::Digest { aux: _ }
					| ImageBuildChunk::PullStatus { status: _, id: _, progress: _, progress_detail: _ } => (),
					ImageBuildChunk::Error { error, error_detail } => {
						return Err(shiplift::Error::InvalidResponse(format!("{}: {:?}", error, error_detail)));
					}
				}
			}
			Ok(())
		}))
		.buffer_unordered(parallelism)
		.try_collect()
		.await
	}

	/// Export the images to a tar
	pub fn images_export(&self, images: &[String]) -> impl AsyncRead + '_ {
		let stream = self.docker.images().export(images.iter().map(|x| &**x).collect());
		tokio_util::io::StreamReader::new(stream.map(|item| item.map(Bytes::from).map_err(|err| io::Error::new(io::ErrorKind::Other, err))))
	}

	/// Import the images from a tar (optionally gzip, bzip2 or xz)
	pub fn images_import(&self) -> (impl AsyncWrite + '_, impl Future<Output = Result<(), shiplift::Error>> + Unpin + '_) {
		let (writer, reader) = tokio::io::duplex(16 * 1024 * 1024);
		(
			writer,
			self.docker
				.images()
				.import(tokio_util::io::ReaderStream::with_capacity(reader, 16 * 1024 * 1024).map(|x| x.map(|bytes| bytes.as_ref().to_owned())))
				.map(|chunk| {
					chunk.and_then(|chunk| {
						match chunk {
							ImageBuildChunk::Update { stream: _ }
							| ImageBuildChunk::Digest { aux: _ }
							| ImageBuildChunk::PullStatus { status: _, id: _, progress: _, progress_detail: _ } => (),
							ImageBuildChunk::Error { error, error_detail } => {
								return Err(shiplift::Error::InvalidResponse(format!("{}: {:?}", error, error_detail)));
							}
						}
						Ok(())
					})
				})
				.try_collect(),
		)
	}
}

impl From<shiplift::Docker> for Docker {
	fn from(docker: shiplift::Docker) -> Self {
		Self { docker }
	}
}

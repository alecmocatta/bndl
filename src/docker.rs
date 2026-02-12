#![allow(dead_code)]

use bollard::{config::BuildInfo, errors::Error, query_parameters::ImportImageOptions};
use futures::{StreamExt, TryStreamExt};
use std::{future::Future, io, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};

pub struct Docker {
	docker: bollard::Docker,
}
impl Docker {
	pub fn new() -> Self {
		Self { docker: bollard::Docker::connect_with_local_defaults().unwrap().with_timeout(Duration::MAX) }
	}

	/// Export the images to a tar
	pub fn images_export(&self, images: &[String]) -> impl AsyncRead + '_ {
		let stream = self.docker.export_images(&images.iter().map(ToString::to_string).collect::<Vec<_>>().iter().map(|x| &**x).collect::<Vec<_>>());
		tokio_util::io::StreamReader::new(stream.map(|item| item.map_err(io::Error::other)))
	}

	/// Import the images from a tar (optionally gzip, bzip2 or xz)
	pub fn images_import(&self) -> (impl AsyncWrite + '_, impl Future<Output = Result<(), Error>> + Unpin + '_) {
		let (writer, reader) = tokio::io::duplex(16 * 1024 * 1024);
		(
			writer,
			self.docker
				.import_image_stream(
					ImportImageOptions { quiet: false, platform: None },
					tokio_util::io::ReaderStream::with_capacity(reader, 16 * 1024 * 1024),
					None,
				)
				.map(|chunk| match chunk {
					Ok(BuildInfo { id: _, stream: _, error_detail, status: _, progress_detail: _, aux: _ }) => {
						if let Some(error_detail) = error_detail {
							Err(Error::DockerStreamError { error: format!("{error_detail:?}") })
						} else {
							Ok(())
						}
					}
					Err(err) => Err(err),
				})
				.try_collect(),
		)
	}
}

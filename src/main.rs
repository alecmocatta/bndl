mod aws;
mod docker;

use async_compression::tokio::bufread::ZstdDecoder;
use futures::{Stream, StreamExt};
use serde::Deserialize;
use std::{env, fs, future::Future, io, path::Path, str, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use aws::Region;
use docker::Docker;

#[derive(Deserialize, Debug)]
struct AwsConfig {
	access_key_id: String,
	secret_access_key: String,
	#[serde(with = "serde_with::rust::display_fromstr")]
	region: Region,
}

#[derive(Deserialize, Debug)]
struct Config {
	bucket: String,
	branch: String,
}

#[tokio::main]
async fn main() {
	if env::var("RUST_BACKTRACE").is_err() {
		env::set_var("RUST_BACKTRACE", "1");
	}

	let aws_config: AwsConfig = envy::prefixed("AWS_").from_env().unwrap();
	let config: Config = envy::prefixed("BNDL_").from_env().unwrap();

	let s3_client = &aws::s3_new(&aws_config.access_key_id, &aws_config.secret_access_key, aws_config.region);

	interval(Duration::from_secs(10))
		.then(|_| async {
			// check /branches/main on s3
			let mut ret = String::new();
			aws::download(s3_client, config.bucket.clone(), format!("branches/{}", config.branch))
				.await
				.expect("couldn't get branches/main")
				.read_to_string(&mut ret)
				.await
				.unwrap();
			ret
		})
		.dedup()
		.then_cancel(|tree_hash| async {
			println!("downloading {}", tree_hash);
			let entry = Path::new(&tree_hash).join("__entry");
			if let Ok(entrypoint) = fs::read_to_string(&entry) {
				let args = shlex::split(&entrypoint).unwrap();
				return (tree_hash, args);
			}
			fs::remove_dir_all(&tree_hash).or_else(|e| (e.kind() == io::ErrorKind::NotFound).then_some(()).ok_or(e)).unwrap();
			fs::create_dir(&tree_hash).unwrap();

			// scope to ensure pb is dropped before potential panics https://github.com/mitsuhiko/indicatif/issues/121
			let (docker_result, entrypoint) = {
				let tar = aws::download(s3_client, config.bucket.clone(), format!("{}/backend.tar.zst", tree_hash)).await.unwrap();
				let tar = ZstdDecoder::new(tar);
				let tar = tokio::io::BufReader::with_capacity(16 * 1024 * 1024, tar);

				let mut entries = tokio_tar::Archive::new(tar);
				let mut entries = entries.entries().unwrap();
				let docker = Docker::new();
				let (writer, results) = docker.images_import();
				let mut docker_tar = tokio_tar::Builder::new(writer);

				let mut entrypoint = None;

				let ((), docker_result) = futures::join!(
					async {
						while let Some(entry) = entries.next().await {
							let mut entry = entry.unwrap();
							let path = entry.path().unwrap().into_owned();
							if path == Path::new("__entry") {
								entrypoint = Some(String::new());
								entry.read_to_string(entrypoint.as_mut().unwrap()).await.unwrap();
							} else if let Ok(path) = path.strip_prefix("__docker") {
								if !path.to_str().unwrap().is_empty() {
									let mut header = entry.header().clone();
									docker_tar.append_data(&mut header, path, entry).await.unwrap();
								}
							} else {
								let _ = entry.unpack_in(&tree_hash).await.unwrap();
							}
						}
						let mut docker_tar = docker_tar.into_inner().await.unwrap();
						docker_tar.shutdown().await.unwrap();
					},
					results
				);
				(docker_result, entrypoint)
			};
			docker_result.unwrap();
			let args = shlex::split(entrypoint.as_ref().unwrap()).unwrap();
			assert!(!args.is_empty());
			fs::write(entry, entrypoint.unwrap()).unwrap();
			(tree_hash, args)
		})
		.for_each_cancel(|(tree_hash, args)| async move {
			loop {
				let backoff = tokio::time::Instant::now() + Duration::from_secs(1);
				println!("executing: {:?}", args);
				let mut entrypoint = tokio::process::Command::new(Path::new(&tree_hash).join(&args[0]));
				entrypoint.args(&args[1..]);
				entrypoint.stdin(std::process::Stdio::null());
				entrypoint.kill_on_drop(true);
				let exit = entrypoint.status().await.unwrap();
				println!("exited with: {:?}", exit);
				tokio::time::sleep_until(backoff).await;
			}
		})
		.await;
}

mod then_cancel {
	use derive_new::new;
	use futures::{Stream, ready, stream::FusedStream};
	use pin_project::pin_project;
	use std::{
		fmt,
		future::Future,
		pin::Pin,
		task::{Context, Poll},
	};

	#[pin_project]
	#[derive(new)]
	pub struct ThenCancel<St, Fut, F> {
		#[pin]
		stream: St,
		f: F,
		#[pin]
		#[new(default)]
		future: Option<Fut>,
	}

	impl<St, Fut, F> fmt::Debug for ThenCancel<St, Fut, F>
	where
		St: fmt::Debug,
		Fut: fmt::Debug,
	{
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.debug_struct("ThenCancel").field("stream", &self.stream).field("future", &self.future).finish()
		}
	}

	impl<St, Fut, F> FusedStream for ThenCancel<St, Fut, F>
	where
		St: FusedStream,
		F: FnMut(St::Item) -> Fut,
		Fut: Future,
	{
		fn is_terminated(&self) -> bool {
			self.future.is_none() && self.stream.is_terminated()
		}
	}

	impl<St, Fut, F> Stream for ThenCancel<St, Fut, F>
	where
		St: Stream,
		F: FnMut(St::Item) -> Fut,
		Fut: Future,
	{
		type Item = Fut::Output;

		fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
			let mut self_ = self.project();
			loop {
				match self_.stream.as_mut().poll_next(cx) {
					Poll::Ready(Some(item)) => self_.future.set(Some((self_.f)(item))),
					Poll::Ready(None) => return Poll::Ready(None),
					Poll::Pending => break,
				}
			}
			if let Some(fut) = self_.future.as_mut().as_pin_mut() {
				let item = ready!(fut.poll(cx));
				self_.future.set(None);
				return Poll::Ready(Some(item));
			}
			Poll::Pending
		}

		fn size_hint(&self) -> (usize, Option<usize>) {
			let (_min, max) = self.stream.size_hint();
			(0, max)
		}
	}
}

mod for_each_cancel {
	use derive_new::new;
	use futures::{Stream, future::FusedFuture, ready, stream::FusedStream};
	use pin_project::pin_project;
	use std::{
		fmt,
		future::Future,
		pin::Pin,
		task::{Context, Poll},
	};

	#[pin_project]
	#[derive(new)]
	pub struct ForEachCancel<St, Fut, F> {
		#[pin]
		stream: St,
		f: F,
		#[pin]
		#[new(default)]
		future: Option<Fut>,
	}

	impl<St, Fut, F> fmt::Debug for ForEachCancel<St, Fut, F>
	where
		St: fmt::Debug,
		Fut: fmt::Debug,
	{
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.debug_struct("ForEachCancel").field("stream", &self.stream).field("future", &self.future).finish()
		}
	}

	impl<St, Fut, F> FusedFuture for ForEachCancel<St, Fut, F>
	where
		St: FusedStream,
		F: FnMut(St::Item) -> Fut,
		Fut: Future<Output = ()>,
	{
		fn is_terminated(&self) -> bool {
			self.future.is_none() && self.stream.is_terminated()
		}
	}

	impl<St, Fut, F> Future for ForEachCancel<St, Fut, F>
	where
		St: Stream,
		F: FnMut(St::Item) -> Fut,
		Fut: Future<Output = ()>,
	{
		type Output = ();

		fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
			let mut self_ = self.project();
			loop {
				match self_.stream.as_mut().poll_next(cx) {
					Poll::Ready(Some(item)) => self_.future.set(Some((self_.f)(item))),
					Poll::Ready(None) => return Poll::Ready(()),
					Poll::Pending => break,
				}
			}
			if let Some(fut) = self_.future.as_mut().as_pin_mut() {
				ready!(fut.poll(cx));
				self_.future.set(None);
			}
			Poll::Pending
		}
	}
}

mod dedup {
	use derive_new::new;
	use futures::{Stream, ready};
	use pin_project::pin_project;
	use std::{
		fmt,
		pin::Pin,
		task::{Context, Poll},
	};

	#[pin_project]
	#[derive(new)]
	pub struct Dedup<S: Stream> {
		#[pin]
		stream: S,
		#[new(default)]
		item: Option<S::Item>,
	}

	impl<S: Stream> fmt::Debug for Dedup<S>
	where
		S: fmt::Debug,
		S::Item: fmt::Debug,
	{
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.debug_struct("Dedup").field("stream", &self.stream).field("item", &self.item).finish()
		}
	}

	impl<S: Stream> Stream for Dedup<S>
	where
		S::Item: Clone + PartialEq,
	{
		type Item = S::Item;

		fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
			let mut self_ = self.project();
			loop {
				let next = ready!(self_.stream.as_mut().poll_next(cx));
				if let Some(next) = &next {
					let eq = self_.item.as_ref() == Some(next);
					*self_.item = Some(next.clone());
					if eq {
						continue;
					}
				}
				return Poll::Ready(next);
			}
		}

		fn size_hint(&self) -> (usize, Option<usize>) {
			let (min, max) = self.stream.size_hint();
			(if self.item.is_none() && min > 0 { 1 } else { 0 }, max)
		}
	}
}

trait StreamExt_: Stream {
	fn then_cancel<Fut, F>(self, f: F) -> then_cancel::ThenCancel<Self, Fut, F>
	where
		F: FnMut(Self::Item) -> Fut,
		Fut: Future,
		Self: Sized,
	{
		assert_stream(then_cancel::ThenCancel::new(self, f))
	}
	fn for_each_cancel<Fut, F>(self, f: F) -> for_each_cancel::ForEachCancel<Self, Fut, F>
	where
		F: FnMut(Self::Item) -> Fut,
		Fut: Future<Output = ()>,
		Self: Sized,
	{
		assert_future(for_each_cancel::ForEachCancel::new(self, f))
	}
	fn dedup(self) -> dedup::Dedup<Self>
	where
		Self::Item: Clone + PartialEq,
		Self: Sized,
	{
		assert_stream(dedup::Dedup::new(self))
	}
}
impl<S> StreamExt_ for S where S: Stream {}

fn assert_future<F: Future>(f: F) -> F {
	f
}
fn assert_stream<S: Stream>(s: S) -> S {
	s
}

fn interval(duration: Duration) -> impl Stream<Item = ()> {
	tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(duration)).map(drop)
}

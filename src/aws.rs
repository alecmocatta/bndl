#![allow(dead_code, clippy::must_use_candidate)]

use aws_sdk_s3::{
	Client,
	config::{BehaviorVersion, Credentials},
	operation::{
		delete_objects::{DeleteObjectsError, DeleteObjectsOutput},
		head_object::{HeadObjectError, HeadObjectOutput},
		list_objects_v2::{ListObjectsV2Error, ListObjectsV2Input, ListObjectsV2Output},
	},
	primitives::ByteStream,
	types::{CompletedMultipartUpload, CompletedPart, Delete, Object, ObjectIdentifier},
};
use aws_smithy_http_client::Builder;
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_types::{
	region::Region as AwsRegion,
	sdk_config::{SdkConfig, SharedCredentialsProvider},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::{Bytes, BytesMut};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, stream};
use md5::{Digest, Md5};
use std::{error::Error, fmt, fmt::Display, future::Future, io, ops::Not as _, str, str::FromStr, sync::Arc, time::Duration};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Region(AwsRegion);

#[allow(non_upper_case_globals)]
impl Region {
	pub const UsEast1: Self = Self(AwsRegion::from_static("us-east-1"));

	pub fn from_static(region: &'static str) -> Self {
		Self(AwsRegion::from_static(region))
	}
	pub fn from(region: String) -> Self {
		Self(AwsRegion::new(region))
	}
	pub fn name(&self) -> &str {
		self.0.as_ref()
	}
}
impl FromStr for Region {
	type Err = RegionError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(Self(AwsRegion::new(s.to_owned())))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RegionError {}
impl Display for RegionError {
	fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match *self {}
	}
}

#[derive(Clone, Debug)]
pub struct Config(pub SdkConfig);

impl Config {
	pub fn new(access_key_id: &str, secret_access_key: &str, region: Region) -> Self {
		// TODO: work out how to use webpki-roots
		// cc https://github.com/smithy-lang/smithy-rs/discussions/3022
		let http_client = Builder::new()
			.tls_provider(aws_smithy_http_client::tls::Provider::Rustls(aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring))
			.tls_context(
				aws_smithy_http_client::tls::TlsContext::builder()
					.with_trust_store(aws_smithy_http_client::tls::TrustStore::empty().with_native_roots(true))
					.build()
					.unwrap(),
			)
			.build_https();

		Self(
			SdkConfig::builder()
				.http_client(http_client)
				.credentials_provider(SharedCredentialsProvider::new(Credentials::new(access_key_id, secret_access_key, None, None, "static")))
				.region(region.0)
				.behavior_version(BehaviorVersion::v2026_01_12())
				.build(),
		)
	}
	pub fn s3(&self) -> S3 {
		S3::new(&self.0)
	}
}

// https://en.wikipedia.org/wiki/Bandwidth-delay_product
// bandwidth * latency
// e.g. https://www.google.com/search?q=10+gigabit+per+second+*+1+second+in+bytes
// TODO: get numbers for EC2 & CI <> S3 & Cloudfront
// TODO: look into https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html
const BDP: u64 = 312_500_000;

#[derive(Clone)]
pub struct S3(pub(crate) Client);

impl S3 {
	fn new(config: &SdkConfig) -> Self {
		Self(Client::new(config))
	}

	/// NOTE: In case of transient errors, this method will retry indefinitely.
	pub async fn upload(
		&self, bucket: String, key: String, content_encoding: Option<String>, content_type: String, cache_control: Option<u64>, read: impl AsyncRead,
	) -> Result<(), Box<dyn Error + Send + Sync>> {
		let pb = &indicatif::ProgressBar::new(0).with_finish(indicatif::ProgressFinish::AndLeave);
		pb.set_draw_target(indicatif::ProgressDrawTarget::hidden()); // Comment out to restore progress bars.
		pb.set_style(
			indicatif::ProgressStyle::default_bar()
				.template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
				.unwrap()
				.progress_chars("#>-"),
		);

		let part_size = 16 * 1024 * 1024;
		assert!((5_242_880..5_368_709_120).contains(&part_size)); // S3's min and max 5MiB and 5GiB
		let parallelism = BDP.div_ceil(part_size).try_into().unwrap();

		let upload_id = &async {
			aws_retry(|| async {
				self.0
					.create_multipart_upload()
					.bucket(bucket.clone())
					.key(key.clone())
					.set_content_encoding(content_encoding.clone())
					.content_type(content_type.clone())
					.cache_control(match cache_control {
						Some(secs) => format!("public, max-age={secs}"),
						None => String::from("no-store, must-revalidate"),
					})
					.send()
					.await
			})
			.await
			.map(|res| res.upload_id.unwrap())
			.map_err(Arc::new)
		}
		.shared();

		tokio::pin!(read);

		let (bucket, key) = (&bucket, &key);

		#[expect(clippy::disallowed_methods)]
		let e_tags = stream::unfold(
			read,
			// This closure reads from the stream.
			|mut read| async move {
				let part_size = part_size.try_into().unwrap();
				let mut buf = BytesMut::with_capacity(part_size);

				loop {
					let n = match read.read_buf(&mut buf).await {
						Ok(n) => n,
						Err(e) => return Some((Err(e), read)),
					};
					assert!(buf.len() <= part_size);
					if n == 0 || buf.len() == part_size {
						break;
					}
				}

				pb.inc_length(buf.len().try_into().unwrap());

				buf.is_empty().not().then_some((Ok::<_, io::Error>(buf.freeze()), read))
			},
		)
		.enumerate()
		// This closure uploads the part that was read.
		.map(|(i, buf): (usize, Result<Bytes, _>)| async move {
			let buf = &buf?;
			assert!(i < 10_000); // S3's max
			Ok::<_, io::Error>(
				aws_retry(|| async {
					let buf = buf.clone();
					let buf_len = buf.len();
					let ret = self
						.0
						.upload_part()
						.bucket(bucket.clone())
						.key(key.clone())
						.upload_id(upload_id.clone().await.unwrap())
						.part_number((i + 1).try_into().unwrap())
						.content_length(buf_len.try_into().unwrap())
						.content_md5(BASE64.encode(Md5::new().chain_update(&buf).finalize()))
						.body(ByteStream::from(buf.clone()))
						.send()
						.await;
					pb.inc(buf_len.try_into().unwrap());
					ret
				})
				.await
				.unwrap()
				.e_tag
				.unwrap(),
			)
		})
		.buffered(parallelism)
		.try_collect::<Vec<String>>()
		.await?;

		if !e_tags.is_empty() {
			let _ = aws_retry(|| async {
				self.0
					.complete_multipart_upload()
					.bucket(bucket.clone())
					.key(key.clone())
					.upload_id(upload_id.clone().await.unwrap())
					.multipart_upload(
						CompletedMultipartUpload::builder()
							.set_parts(Some(
								e_tags
									.iter()
									.enumerate()
									.map(|(i, e_tag)| CompletedPart::builder().part_number((i + 1).try_into().unwrap()).e_tag(e_tag.clone()).build())
									.collect(),
							))
							.build(),
					)
					.send()
					.await
			})
			.await?;
		} else {
			let _ = aws_retry(|| async {
				self.0
					.put_object()
					.bucket(bucket.clone())
					.key(key.clone())
					.set_content_encoding(content_encoding.clone())
					.content_type(content_type.clone())
					.cache_control(match cache_control {
						Some(secs) => format!("public, max-age={secs}"),
						None => String::from("no-store, must-revalidate"),
					})
					.content_md5(BASE64.encode(Md5::new().finalize()))
					.send()
					.await
			})
			.await
			.unwrap();
		}

		Ok(())
	}

	/// NOTE: In case of transient errors, this method will retry indefinitely.
	pub async fn download(&self, bucket: String, key: String) -> Result<impl AsyncBufRead + '_, io::Error> {
		let (part_size, parts_count): (u64, Option<u64>) = {
			let head =
				self.head_object(bucket.clone(), key.clone(), Some(1)).await.map_err(|e| io::Error::new(io::ErrorKind::NotFound, e.to_string()))?;

			(head.content_length.unwrap().try_into().unwrap(), head.parts_count.map(|x| x.try_into().unwrap()))
		};

		// NOTE: If the object is multipart, obtain its total content length.
		let length = if parts_count.is_some() {
			let head = self
				.head_object(bucket.clone(), key.clone(), None)
				.await
				// This request is guaranteed to succeed because of the previous `head_object` request.
				.unwrap();

			head.content_length.unwrap().try_into().unwrap()
		} else {
			part_size
		};

		println!("[AWS][S3] Downloading {bucket}/{key} (length: {length}, part_size: {part_size}, parts_count: {parts_count:?})");

		let pb = indicatif::ProgressBar::new(length).with_finish(indicatif::ProgressFinish::AndLeave);
		pb.set_draw_target(indicatif::ProgressDrawTarget::hidden()); // Comment out to restore progress bars.

		pb.set_style(
			indicatif::ProgressStyle::default_bar()
				.template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
				.unwrap()
				.progress_chars("#>-"),
		);

		if let Some(parts) = parts_count {
			let parallelism = BDP.div_ceil(part_size).try_into().unwrap();

			assert!(part_size * parts < length + part_size && length <= part_size * parts);

			#[expect(clippy::disallowed_methods)]
			let body = tokio_util::io::StreamReader::new(
				stream::iter((0..parts).map(move |i| {
					let (pb, bucket, key) = (pb.clone(), bucket.clone(), key.clone());

					// This is the part of the code that does the actual downloading.
					async move {
						let range = part_size * i..(part_size * (i + 1)).min(length);
						let cap: usize = (range.end - range.start).try_into().unwrap();

						'async_read: loop {
							let body = aws_retry(|| async {
								self.0.get_object().bucket(bucket.clone()).key(key.clone()).part_number((i + 1).try_into().unwrap()).send().await
							})
							.await
							.unwrap()
							.body
							.into_async_read();

							let mut body = pb.wrap_async_read(body);
							let mut buf = BytesMut::with_capacity(cap);

							while buf.len() != cap {
								let _bytes = match body.read_buf(&mut buf).await {
									Ok(bytes) => bytes,
									Err(e) => {
										println!("Got transient http error: {e:?}. Retrying.");
										continue 'async_read;
									}
								};
								assert!(buf.len() <= cap);
							}
							break Ok::<_, io::Error>(buf);
						}
					}
				}))
				.buffered(parallelism),
			);

			Ok(tokio_util::either::Either::Left(body))
		} else {
			let body = aws_retry(|| async { self.0.get_object().bucket(bucket.clone()).key(key.clone()).send().await })
				.await
				.unwrap()
				.body
				.into_async_read();

			let body = pb.wrap_async_read(body);
			let body = tokio::io::BufReader::with_capacity(16 * 1024 * 1024, body);

			Ok(tokio_util::either::Either::Right(body))
		}
	}

	/// This stream repeatedly runs the same request, taking care of adjusting the
	/// [ListObjectsV2Input::continuation_token] as appropriate.
	///
	/// NOTE: In case of transient errors, this method will retry indefinitely.
	fn stream_list_objects_v2(
		&self, request: ListObjectsV2Input,
	) -> impl Stream<Item = Result<ListObjectsV2Output, SdkError<ListObjectsV2Error, HttpResponse>>> + '_ {
		type State<'c> = Option<(ListObjectsV2Input, &'c S3)>;

		async fn retrieve_list_fragment(
			state: State<'_>,
		) -> Option<(Result<ListObjectsV2Output, SdkError<ListObjectsV2Error, HttpResponse>>, State<'_>)> {
			let (mut request, me) = state?;

			let bucket = request.bucket.clone().unwrap();
			let response = aws_retry(|| async {
				me.0.list_objects_v2()
					.bucket(bucket.clone())
					.set_prefix(request.prefix.clone())
					.set_start_after(request.start_after.clone())
					.set_continuation_token(request.continuation_token.clone())
					.set_max_keys(request.max_keys)
					.send()
					.await
			})
			.await;

			match response {
				Ok(output) if output.next_continuation_token.is_some() => {
					request.continuation_token = output.next_continuation_token.clone();
					Some((Ok(output), Some((request, me))))
				}
				Ok(output) => Some((Ok(output), None)),
				Err(error) => Some((Err(error), None)),
			}
		}

		stream::unfold(Some((request, self)), retrieve_list_fragment)
	}

	/// Returns all [Object]s in a bucket.
	///
	/// # Panics
	/// Should never panic.
	///
	/// # Errors
	/// Errors are for failed paginated requests, the iterator will terminate at the first failed
	/// request (or when the bucket has been exausted).
	pub async fn list_objects(
		&self, bucket: String, prefix: Option<String>, start_after: Option<String>, max_keys: Option<u32>,
	) -> Result<impl Iterator<Item = Object>, SdkError<ListObjectsV2Error, HttpResponse>> {
		let responses: Vec<_> = self
			.stream_list_objects_v2(
				ListObjectsV2Input::builder()
					.bucket(bucket)
					.set_prefix(prefix)
					.set_start_after(start_after)
					.set_max_keys(max_keys.map(|x| x.try_into().unwrap()))
					.build()
					.unwrap(),
			)
			.collect()
			.await;

		let mut objects = vec![];

		for response in responses {
			let output = response?;
			// Empty bucket returns `None`.
			if let Some(contents) = output.contents {
				objects.push(contents);
			}
		}

		Ok(objects.into_iter().flatten())
	}

	// NOTE: This method is only used in `ttask::rollout_gc`
	pub async fn delete_objects(
		&self, bucket: String, obj_chunk: Vec<ObjectIdentifier>,
	) -> Result<DeleteObjectsOutput, SdkError<DeleteObjectsError, HttpResponse>> {
		aws_retry(|| async {
			self.0
				.delete_objects()
				.bucket(bucket.clone())
				.delete(Delete::builder().set_objects(Some(obj_chunk.clone())).build().unwrap())
				.send()
				.await
		})
		.await
	}

	/// NOTE: In case of transient errors, this method will retry indefinitely.
	pub async fn head_object(
		&self, bucket: String, key: String, part_number: Option<i32>,
	) -> Result<HeadObjectOutput, SdkError<HeadObjectError, HttpResponse>> {
		aws_retry(|| async { self.0.head_object().bucket(bucket.clone()).key(key.clone()).set_part_number(part_number).send().await }).await
	}
}

/// Wraps [Future] which returns a [Result<_, SdkError>] and automatically retries it if some
/// transient errors are found.
///
/// # Panics
/// Should never panic if the passed in function does not.
///
/// # Errors
/// The errors that are not caught are passed on transparently.
pub async fn aws_retry<F, Fut, T, E>(mut f: F) -> Result<T, SdkError<E, HttpResponse>>
where
	F: FnMut() -> Fut,
	Fut: Future<Output = Result<T, SdkError<E, HttpResponse>>>,
{
	loop {
		#[expect(clippy::match_wild_err_arm)]
		match f().await {
			Err(SdkError::ConstructionFailure(_)) => unreachable!(),
			Err(SdkError::DispatchFailure(e)) => {
				println!("[AWS] Got transient error: {e:?}. Retrying.");
			}
			Err(SdkError::TimeoutError(e)) => {
				println!("[AWS] Got transient error: {e:?}. Retrying.");
			}
			Err(SdkError::ResponseError(res)) => {
				println!("[AWS] Got transient error: {res:?}. Retrying.");
			}
			Err(SdkError::ServiceError(res)) if should_retry(res.raw()) => {
				println!("[AWS] Got transient response error: {:?}. Retrying.", res.raw());
			}
			res @ (Ok(_) | Err(SdkError::ServiceError(_))) => break res,
			Err(_) => panic!("[AWS] non_exhaustive"),
		}
		tokio::time::sleep(Duration::from_secs(2)).await;
	}
}

// https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-retries.html
// https://docs.aws.amazon.com/general/latest/gr/api-retries.html
// https://aws.amazon.com/premiumsupport/knowledge-center/http-5xx-errors-s3/
// backblaze gives us 501 when you give it options it doesn't support
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
// maybe also ExpiredToken TokenRefreshRequired?
// Throttling: cloudfront: https://github.com/tablyinc/tably/actions/runs/4376469277/jobs/7658623957#step:7:52
// https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/modules/_aws_sdk_service_error_classification.html
// 499: cloudflare https://github.com/tablyinc/tably/issues/1009#issuecomment-1411922970
// 522, 524: cloudflare https://github.com/tablyinc/tably/actions/runs/4376482439/jobs/7677545729#step:5:454
// https://support.cloudflare.com/hc/en-us/articles/115003011431-Troubleshooting-Cloudflare-5XX-errors
fn should_retry(res: &HttpResponse) -> bool {
	match (res.status().as_u16(), str::from_utf8(res.body().bytes().unwrap_or_default())) {
		(429 | 500 | 502 | 503 | 504 | 509 | 522 | 524, _) => true,
		(400, body) if body.unwrap().contains("RequestTimeout") || body.unwrap().contains("Throttling") => true,
		(403, body) if body.unwrap().contains("RequestTimeTooSkewed") => true,
		(499, body) if body.unwrap().contains("Client Disconnect") => true,
		(_, body) if body.is_ok_and(|body| body.contains("Please try again.")) => true,
		_ => false,
	}
}

impl fmt::Debug for S3 {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_tuple("S3").finish()
	}
}

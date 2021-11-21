use async_compression::{tokio::write::GzipEncoder, Level};
use count_write::CountWrite;
use futures::StreamExt;
use std::{
	collections::{BTreeSet, HashSet}, convert::TryInto, fs, io, path::{Path, PathBuf}, pin::Pin
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use walkdir::WalkDir;

use docker::Docker;

pub async fn bundle(binary: PathBuf, resource_dirs: HashSet<PathBuf>, tar: impl AsyncWrite + Send) -> Result<u64, io::Error> {
	let mut resources = resource_dirs.into_iter().collect::<Vec<_>>();
	resources.sort();
	// recurse resource dirs and sort file list
	let resources = resources
		.into_iter()
		.map(|resources| {
			WalkDir::new(resources)
				.sort_by(|a, b| a.file_name().cmp(b.file_name()))
				.into_iter()
				.filter_map(|entry| {
					let entry = entry.unwrap();
					match entry.file_type() {
						t if t.is_dir() => Some(false),
						t if t.is_file() => Some(true),
						_ => None,
					}
					.map(|is_file| (is_file, entry.into_path()))
				})
				.collect::<Vec<_>>()
		})
		.collect::<Vec<_>>();

	let tar = CountWrite::from(tar);
	tokio::pin!(tar);
	let tar_: Pin<&mut (dyn AsyncWrite + Unpin + Send + '_)> = Pin::new(&mut tar);
	let tar_: Pin<&'static mut (dyn AsyncWrite + Unpin + Send + 'static)> = unsafe { std::mem::transmute(tar_) };

	// create a deterministic .tar.gz
	let tar_ = GzipEncoder::with_quality(tar_, Level::Precise(6)); // 6 = good default
	let mut tar_ = tokio_tar::Builder::new(tar_);
	tar_.mode(tokio_tar::HeaderMode::Deterministic);

	// add the entry point to tar
	let mut entry = shlex::join([binary.to_str().unwrap()]);
	entry.push('\n');
	let mut header = tokio_tar::Header::new_gnu();
	header.set_mtime(0);
	header.set_uid(0);
	header.set_gid(0);
	header.set_mode(0o755);
	header.set_size(entry.len().try_into().unwrap());
	tar_.append_data(&mut header, "__entry", entry.as_bytes()).await.unwrap();

	// add resources (check for docker images) to tar
	let mut docker_images = BTreeSet::new();
	let mut binaries = BTreeSet::new();
	for resources in resources {
		for (is_file, resource) in resources {
			if is_file {
				if resource.file_name().unwrap() == "docker" {
					let images = fs::read_to_string(&resource).unwrap();
					let images = images.split('\n').filter(|image| !image.is_empty()).map(ToOwned::to_owned).collect::<Vec<_>>();
					docker_images.extend(images);
				} else if resource.file_name().unwrap() == "binary" {
					let images = fs::read_to_string(&resource).unwrap();
					let images = images.split('\n').filter(|image| !image.is_empty()).map(ToOwned::to_owned).collect::<Vec<_>>();
					binaries.extend(images);
				}
				tar_.append_file(&resource, &mut tokio::fs::File::open(&resource).await.unwrap()).await.unwrap();
			} else {
				builder_append_dir(&mut tar_, &resource).await.unwrap();
			}
		}
	}

	// add binaries to tar
	let mut binaries = binaries.into_iter().map(|resource| binary.parent().unwrap().join(resource)).collect::<Vec<_>>();
	binaries.sort();
	binaries.insert(0, binary);
	for resource in binaries {
		tar_.append_file(&resource, &mut tokio::fs::File::open(&resource).await.unwrap()).await.unwrap();
	}

	// add docker images resources to tar
	if !docker_images.is_empty() {
		let mut docker_images = docker_images.into_iter().collect::<Vec<_>>();
		docker_images.sort();
		let docker_dir = Path::new("__docker");
		let docker = Docker::new();
		builder_append_dir(&mut tar_, &docker_dir).await.unwrap();
		let docker_tar = docker.images_export(docker_images);
		tokio::pin!(docker_tar);
		let mut entries = tokio_tar::Archive::new(docker_tar).entries().unwrap();
		while let Some(entry) = entries.next().await {
			let entry = entry.unwrap();
			let mut header = entry.header().clone();
			let path = docker_dir.join(header.path().unwrap());
			tar_.append_data(&mut header, path, entry).await.unwrap();
		}
	}

	// flush writers
	let mut tar_ = tar_.into_inner().await.unwrap();
	tar_.shutdown().await.unwrap();
	let _tar = tar_.into_inner();
	let tar_len = tar.count();

	Ok(tar_len)
}

async fn builder_append_dir<W, P>(self_: &mut tokio_tar::Builder<W>, path: P) -> io::Result<()>
where
	W: tokio::io::AsyncWrite + Unpin + Send + 'static,
	P: AsRef<Path>,
{
	let mut header = tokio_tar::Header::new_gnu();
	header.set_mtime(0);
	header.set_uid(0);
	header.set_gid(0);
	header.set_mode(0o755);
	header.set_size(0);
	header.set_entry_type(tokio_tar::EntryType::Directory);
	self_.append_data(&mut header, path, &[] as &[u8]).await
}

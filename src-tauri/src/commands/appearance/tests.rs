use super::*;
use image::{Rgb, RgbImage, Rgba, RgbaImage};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("trivium-wallpaper-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn managed(&self) -> PathBuf {
        self.0.join("appearance").join(MANAGED_NAME)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn fixture(format: ImageFormat) -> Vec<u8> {
    let image = DynamicImage::ImageRgb8(RgbImage::from_pixel(24, 12, Rgb([80, 120, 160])));
    let mut bytes = Cursor::new(Vec::new());
    image.write_to(&mut bytes, format).unwrap();
    bytes.into_inner()
}

#[test]
fn png_jpeg_and_webp_are_normalized_to_static_managed_images() {
    for format in [ImageFormat::Png, ImageFormat::Jpeg, ImageFormat::WebP] {
        let input = fixture(format);
        let prepared = prepare(&input).unwrap();
        assert_eq!((prepared.width, prepared.height), (24, 12));
        assert_eq!(prepared.mime, "image/jpeg");
        assert_eq!(
            image::guess_format(&prepared.bytes).unwrap(),
            ImageFormat::Jpeg
        );
        assert!(prepared.bytes.len() <= MAX_BYTES);
        let response = prepared.response();
        assert!(response.data_url.starts_with("data:image/jpeg;base64,"));
        assert!(serde_json::to_value(response)
            .unwrap()
            .get("dataUrl")
            .is_some());
    }
}

#[test]
fn transparent_png_keeps_alpha_and_rejects_truncated_or_non_image_input() {
    let image = DynamicImage::ImageRgba8(RgbaImage::from_pixel(8, 4, Rgba([90, 120, 160, 80])));
    let mut bytes = Cursor::new(Vec::new());
    image.write_to(&mut bytes, ImageFormat::Png).unwrap();
    let prepared = prepare(bytes.get_ref()).unwrap();
    assert_eq!(prepared.mime, "image/png");
    assert_eq!(
        decode_static(&prepared.bytes, MAX_PIXELS, MAX_EDGE)
            .unwrap()
            .to_rgba8()
            .get_pixel(0, 0)[3],
        80
    );
    for invalid in [
        &b"{\"version\":1}"[..],
        &b"GIF89a"[..],
        &bytes.get_ref()[..30],
    ] {
        assert!(prepare(invalid).is_err());
    }
}

#[test]
fn output_geometry_respects_both_long_edge_and_pixel_budget() {
    for (width, height) in [(8000, 4000), (5600, 5600), (32000, 1), (1, 32000), (24, 12)] {
        let (w, h) = target_dimensions(width, height);
        assert!(w > 0 && h > 0 && w <= MAX_EDGE && h <= MAX_EDGE);
        assert!(u64::from(w) * u64::from(h) <= MAX_PIXELS);
    }
    let image = DynamicImage::ImageRgb8(RgbImage::from_pixel(4000, 1, Rgb([8, 16, 32])));
    let mut bytes = Cursor::new(Vec::new());
    image.write_to(&mut bytes, ImageFormat::Png).unwrap();
    let prepared = prepare(bytes.get_ref()).unwrap();
    assert_eq!((prepared.width, prepared.height), (3840, 1));
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
fn chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(data);
    out.extend_from_slice(&crc32(&out[4..]).to_be_bytes());
    out
}

#[test]
fn oversized_header_and_animated_png_are_rejected_before_pixel_decode() {
    let original = fixture(ImageFormat::Png);
    let mut oversized = original.clone();
    oversized[16..20].copy_from_slice(&32000u32.to_be_bytes());
    oversized[20..24].copy_from_slice(&32000u32.to_be_bytes());
    let crc = crc32(&oversized[12..29]);
    oversized[29..33].copy_from_slice(&crc.to_be_bytes());
    let error = prepare(&oversized).err().unwrap();
    assert!(error.contains("32 million"), "{error}");
    let mut animated = original[..33].to_vec();
    animated.extend_from_slice(&chunk(b"acTL", &[0, 0, 0, 1, 0, 0, 0, 0]));
    animated.extend_from_slice(&original[33..]);
    let error = prepare(&animated).err().unwrap();
    assert!(error.contains("Animated PNG"), "{error}");
}

#[test]
fn animated_webp_is_rejected_without_decoding_frames() {
    fn webp_chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
        let mut chunk = kind.to_vec();
        chunk.extend_from_slice(&(data.len() as u32).to_le_bytes());
        chunk.extend_from_slice(data);
        if data.len() % 2 != 0 {
            chunk.push(0);
        }
        chunk
    }
    let still = fixture(ImageFormat::WebP);
    let mut body = b"WEBP".to_vec();
    body.extend_from_slice(&webp_chunk(b"VP8X", &[2, 0, 0, 0, 23, 0, 0, 11, 0, 0]));
    body.extend_from_slice(&webp_chunk(b"ANIM", &[0, 0, 0, 0, 0, 0]));
    let mut frame = vec![0, 0, 0, 0, 0, 0, 23, 0, 0, 11, 0, 0, 1, 0, 0, 0];
    frame.extend_from_slice(&still[12..]);
    body.extend_from_slice(&webp_chunk(b"ANMF", &frame));
    let mut animated = b"RIFF".to_vec();
    animated.extend_from_slice(&(body.len() as u32).to_le_bytes());
    animated.extend_from_slice(&body);
    let error = prepare(&animated).err().unwrap();
    assert!(error.contains("Animated WebP"), "{error}");
}

#[test]
fn failed_import_keeps_the_previous_copy_and_never_changes_the_source() {
    let directory = Directory::new();
    let source = directory.0.join("selected.webp");
    let original = fixture(ImageFormat::WebP);
    fs::write(&source, &original).unwrap();
    let path = directory.managed();
    let response = import_file(&path, &source).unwrap();
    assert_eq!(fs::read(&source).unwrap(), original);
    let saved = fs::read(&path).unwrap();
    let wrong = directory.0.join("other.json");
    fs::write(&wrong, b"{}").unwrap();
    assert!(import_file(&path, &wrong).is_err());
    assert_eq!(fs::read(&path).unwrap(), saved);
    let loaded = load_file(&path).unwrap().unwrap();
    assert_eq!(loaded.data_url, response.data_url);
    let unrelated = path.parent().unwrap().join("other.json");
    fs::write(&unrelated, b"{}").unwrap();
    clear_file(&path).unwrap();
    assert!(load_file(&path).unwrap().is_none());
    assert_eq!(fs::read(&unrelated).unwrap(), b"{}");
    assert_eq!(fs::read(&source).unwrap(), original);
}

#[test]
fn oversized_files_are_refused_without_reading_them_into_memory() {
    let directory = Directory::new();
    let source = directory.0.join("oversized.png");
    File::create(&source)
        .unwrap()
        .set_len(MAX_BYTES as u64 + 1)
        .unwrap();
    assert!(read_bounded(&source).err().unwrap().contains("16 MiB"));
    assert!(!directory.managed().exists());
    let mut buffer = BoundedBuffer::default();
    buffer.bytes.resize(MAX_BYTES, 0);
    assert!(buffer.write_all(&[1]).is_err());
    assert!(buffer.full);
    assert_eq!(buffer.bytes.len(), MAX_BYTES);
}

#[test]
fn managed_copy_is_profile_local_and_corruption_is_not_forwarded_to_the_webview() {
    let first = Directory::new();
    let other = Directory::new();
    let source = first.0.join("selected.png");
    fs::write(&source, fixture(ImageFormat::Png)).unwrap();
    import_file(&first.managed(), &source).unwrap();
    assert!(load_file(&other.managed()).unwrap().is_none());
    fs::write(first.managed(), b"not an image").unwrap();
    assert!(load_file(&first.managed()).is_err());
}

#[cfg(unix)]
#[test]
fn managed_directory_cannot_redirect_reads_or_deletion_outside_the_profile() {
    let profile = Directory::new();
    let elsewhere = Directory::new();
    let source = profile.0.join("selected.png");
    let original = fixture(ImageFormat::Png);
    fs::write(&source, &original).unwrap();
    let external = elsewhere.0.join(MANAGED_NAME);
    fs::write(&external, &original).unwrap();
    std::os::unix::fs::symlink(&elsewhere.0, profile.0.join("appearance")).unwrap();
    assert!(load_file(&profile.managed()).is_err());
    assert!(clear_file(&profile.managed()).is_err());
    assert!(import_file(&profile.managed(), &source).is_err());
    assert_eq!(fs::read(external).unwrap(), original);
    assert_eq!(fs::read(source).unwrap(), original);
}

#[test]
fn worker_is_off_the_caller_thread_and_serializes_decodes() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    };
    let caller = std::thread::current().id();
    let active = Arc::new(AtomicUsize::new(0));
    let shared = active.clone();
    let (started, ready) = mpsc::sync_channel(1);
    let (release, gate) = mpsc::sync_channel(1);
    let first = tauri::async_runtime::spawn(image_task(move || {
        assert_ne!(std::thread::current().id(), caller);
        assert_eq!(shared.fetch_add(1, Ordering::SeqCst), 0);
        started.send(()).unwrap();
        gate.recv().unwrap();
        shared.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }));
    ready
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    // Dropping the IPC waiter must not release admission while its worker
    // still holds the decoded image. This is deterministic, without sleeps.
    first.abort();
    assert!(tauri::async_runtime::block_on(first).is_err());
    assert!(IMAGE_ADMISSION.get().unwrap().try_lock().is_err());
    let second = tauri::async_runtime::spawn(image_task(move || {
        assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
        active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }));
    release.send(()).unwrap();
    tauri::async_runtime::block_on(async {
        second.await.unwrap().unwrap();
    });
}

//! A wallpaper is a bounded, normalized copy in this application's data directory.
//! No original file path or arbitrary URL is retained in appearance settings.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use image::codecs::{jpeg::JpegDecoder, png::PngDecoder, webp::WebPDecoder};
use image::{DynamicImage, ImageDecoder, ImageEncoder, ImageFormat, Limits};
use serde::Serialize;
use tauri::Manager;
use uuid::Uuid;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_INPUT_PIXELS: u64 = 32_000_000;
const MAX_DECODE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_INPUT_EDGE: u32 = 32_768;
const MAX_EDGE: u32 = 3_840;
const MAX_PIXELS: u64 = 8_000_000;
const MANAGED_NAME: &str = "wallpaper.bin";

static IMAGE_ADMISSION: OnceLock<tauri::async_runtime::Mutex<()>> = OnceLock::new();

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WallpaperImage {
    pub data_url: String,
    pub width: u32,
    pub height: u32,
}

struct PreparedImage {
    bytes: Vec<u8>,
    mime: &'static str,
    width: u32,
    height: u32,
}

impl PreparedImage {
    fn response(self) -> WallpaperImage {
        WallpaperImage {
            data_url: format!("data:{};base64,{}", self.mime, STANDARD.encode(self.bytes)),
            width: self.width,
            height: self.height,
        }
    }
}

fn managed_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    app.path()
        .app_local_data_dir()
        .map(|path| path.join("appearance").join(MANAGED_NAME))
        .map_err(|error| format!("Could not locate appearance storage: {error}"))
}

async fn image_task<T, F>(work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    // Waiters occupy no blocking-pool threads. The worker owns the permit so a
    // dropped IPC future cannot admit another decoder before this one finishes.
    let admission = IMAGE_ADMISSION
        .get_or_init(|| tauri::async_runtime::Mutex::new(()))
        .lock()
        .await;
    tauri::async_runtime::spawn_blocking(move || {
        let _admission = admission;
        work()
    })
    .await
    .map_err(|error| format!("Background image task failed: {error}"))?
}

#[tauri::command]
pub async fn import_wallpaper(
    app: tauri::AppHandle,
    path: String,
) -> Result<WallpaperImage, String> {
    let destination = managed_path(&app)?;
    image_task(move || import_file(&destination, Path::new(&path))).await
}

#[tauri::command]
pub async fn load_wallpaper(app: tauri::AppHandle) -> Result<Option<WallpaperImage>, String> {
    let destination = managed_path(&app)?;
    image_task(move || load_file(&destination)).await
}

#[tauri::command]
pub async fn clear_wallpaper(app: tauri::AppHandle) -> Result<(), String> {
    let destination = managed_path(&app)?;
    image_task(move || clear_file(&destination)).await
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, String> {
    let file =
        File::open(path).map_err(|error| format!("Could not open background image: {error}"))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("Could not inspect background image: {error}"))?;
    if !metadata.is_file() {
        return Err("Choose a regular PNG, JPEG or WebP image file.".into());
    }
    if metadata.len() > MAX_BYTES as u64 {
        return Err("Background image must be 16 MiB or smaller.".into());
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("Could not read background image: {error}"))?;
    if bytes.len() > MAX_BYTES {
        return Err("Background image must be 16 MiB or smaller.".into());
    }
    Ok(bytes)
}

fn decoder_limits() -> Limits {
    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_INPUT_EDGE);
    limits.max_image_height = Some(MAX_INPUT_EDGE);
    limits.max_alloc = Some(MAX_DECODE_BYTES);
    limits
}

fn check_dimensions(width: u32, height: u32, pixels: u64, edge: u32) -> Result<(), String> {
    if width == 0 || height == 0 || width > edge || height > edge {
        return Err(format!(
            "Background image dimensions must be between 1 and {edge} pixels."
        ));
    }
    if u64::from(width) * u64::from(height) > pixels {
        return Err(format!(
            "Background image exceeds the {} million pixel limit.",
            pixels / 1_000_000
        ));
    }
    Ok(())
}

fn decode<D: ImageDecoder>(mut decoder: D, pixels: u64, edge: u32) -> Result<DynamicImage, String> {
    let (width, height) = decoder.dimensions();
    // Check the header and output allocation before constructing a pixel buffer.
    check_dimensions(width, height, pixels, edge)?;
    if decoder.total_bytes() > MAX_DECODE_BYTES {
        return Err("Background image needs too much decoded memory.".into());
    }
    decoder
        .set_limits(decoder_limits())
        .map_err(|error| format!("Background image exceeds decoding limits: {error}"))?;
    let orientation = decoder
        .orientation()
        .unwrap_or(image::metadata::Orientation::NoTransforms);
    let mut image = DynamicImage::from_decoder(decoder)
        .map_err(|error| format!("Could not decode background image: {error}"))?;
    image.apply_orientation(orientation);
    Ok(image)
}

fn decode_static(bytes: &[u8], pixels: u64, edge: u32) -> Result<DynamicImage, String> {
    let format = image::guess_format(bytes)
        .map_err(|_| "Choose a PNG, JPEG or WebP image file.".to_string())?;
    let error = |error| format!("Could not inspect background image: {error}");
    match format {
        ImageFormat::Png => {
            let decoder =
                PngDecoder::with_limits(Cursor::new(bytes), decoder_limits()).map_err(error)?;
            if decoder.is_apng().map_err(error)? {
                return Err(
                    "Animated PNG backgrounds are not supported. Choose a static image.".into(),
                );
            }
            decode(decoder, pixels, edge)
        }
        ImageFormat::Jpeg => decode(
            JpegDecoder::new(Cursor::new(bytes)).map_err(error)?,
            pixels,
            edge,
        ),
        ImageFormat::WebP => {
            let decoder = WebPDecoder::new(Cursor::new(bytes)).map_err(error)?;
            if decoder.has_animation() {
                return Err(
                    "Animated WebP backgrounds are not supported. Choose a static image.".into(),
                );
            }
            decode(decoder, pixels, edge)
        }
        _ => Err("Choose a PNG, JPEG or WebP image file.".into()),
    }
}

fn target_dimensions(width: u32, height: u32) -> (u32, u32) {
    let edge_scale = f64::from(MAX_EDGE) / f64::from(width.max(height));
    let pixel_scale = (MAX_PIXELS as f64 / (f64::from(width) * f64::from(height))).sqrt();
    let scale = 1.0_f64.min(edge_scale).min(pixel_scale);
    (
        (f64::from(width) * scale).floor().max(1.0) as u32,
        (f64::from(height) * scale).floor().max(1.0) as u32,
    )
}

#[derive(Default)]
struct BoundedBuffer {
    bytes: Vec<u8>,
    full: bool,
}
impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_BYTES.saturating_sub(self.bytes.len()) {
            self.full = true;
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "Encoded background image is too large",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn prepare(bytes: &[u8]) -> Result<PreparedImage, String> {
    if bytes.len() > MAX_BYTES {
        return Err("Background image must be 16 MiB or smaller.".into());
    }
    let mut image = decode_static(bytes, MAX_INPUT_PIXELS, MAX_INPUT_EDGE)?;
    let (width, height) = target_dimensions(image.width(), image.height());
    if (width, height) != (image.width(), image.height()) {
        image = image.resize_exact(width, height, image::imageops::FilterType::Triangle);
    }
    loop {
        let mut output = BoundedBuffer::default();
        let has_alpha = image.color().has_alpha();
        let result = if has_alpha {
            let pixels = image.to_rgba8();
            image::codecs::png::PngEncoder::new_with_quality(
                &mut output,
                image::codecs::png::CompressionType::Fast,
                image::codecs::png::FilterType::Adaptive,
            )
            .write_image(
                pixels.as_raw(),
                pixels.width(),
                pixels.height(),
                image::ExtendedColorType::Rgba8,
            )
        } else {
            let pixels = image.to_rgb8();
            image::codecs::jpeg::JpegEncoder::new_with_quality(&mut output, 90).write_image(
                pixels.as_raw(),
                pixels.width(),
                pixels.height(),
                image::ExtendedColorType::Rgb8,
            )
        };
        match result {
            Ok(()) => {
                return Ok(PreparedImage {
                    bytes: output.bytes,
                    mime: if has_alpha { "image/png" } else { "image/jpeg" },
                    width: image.width(),
                    height: image.height(),
                })
            }
            Err(_) if output.full && (image.width() > 1 || image.height() > 1) => {
                // Unusually noisy alpha images can exceed the encoded budget.
                // Downsample once more instead of retaining a huge data URL.
                image = image.resize_exact(
                    (image.width() / 2).max(1),
                    (image.height() / 2).max(1),
                    image::imageops::FilterType::Triangle,
                );
            }
            Err(error) => return Err(format!("Could not encode background image: {error}")),
        }
    }
}

fn validate_managed_directory(destination: &Path) -> Result<bool, String> {
    let directory = destination
        .parent()
        .ok_or("Invalid background storage directory.")?;
    match fs::symlink_metadata(directory) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err("Background storage must be a regular application directory.".into());
            }
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("Could not inspect background storage: {error}")),
    }
}

fn ensure_managed_directory(destination: &Path) -> Result<(), String> {
    if !validate_managed_directory(destination)? {
        fs::create_dir_all(destination.parent().unwrap())
            .map_err(|error| format!("Could not create background storage: {error}"))?;
    }
    Ok(())
}

fn import_file(destination: &Path, source: &Path) -> Result<WallpaperImage, String> {
    let prepared = prepare(&read_bounded(source)?)?;
    ensure_managed_directory(destination)?;
    let temporary = destination.with_file_name(format!(".wallpaper-{}.tmp", Uuid::new_v4()));
    let result = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&prepared.bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, destination)
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(format!("Could not save the background copy: {error}"));
    }
    Ok(prepared.response())
}

fn load_file(destination: &Path) -> Result<Option<WallpaperImage>, String> {
    if !validate_managed_directory(destination)? {
        return Ok(None);
    }
    match fs::symlink_metadata(destination) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("Could not inspect the background copy: {error}")),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err("The background copy must be a regular application file.".into());
        }
        Ok(_) => {}
    }
    let bytes = read_bounded(destination)?;
    let image = decode_static(&bytes, MAX_PIXELS, MAX_EDGE)?;
    let mime = match image::guess_format(&bytes) {
        Ok(ImageFormat::Png) => "image/png",
        Ok(ImageFormat::Jpeg) => "image/jpeg",
        _ => return Err("The background copy is not a normalized image. Choose it again.".into()),
    };
    Ok(Some(
        PreparedImage {
            bytes,
            mime,
            width: image.width(),
            height: image.height(),
        }
        .response(),
    ))
}

fn clear_file(destination: &Path) -> Result<(), String> {
    if !validate_managed_directory(destination)? {
        return Ok(());
    }
    match fs::remove_file(destination) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("Could not remove the background copy: {error}")),
    }
}

#[cfg(test)]
mod tests;

export interface WallpaperSettings {
  version: 2;
  enabled: boolean;
  strength: number;
}
export interface WallpaperImage { dataUrl: string; width: number; height: number; }
export const DEFAULT_WALLPAPER: WallpaperSettings = { version: 2, enabled: false, strength: 30 };
export function parseWallpaperSettings(value: unknown): WallpaperSettings | null {
  if (typeof value === "string") { try { value = JSON.parse(value); } catch { return null; } }
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const saved = value as Record<string, unknown>;
  if (typeof saved.enabled !== "boolean" || typeof saved.strength !== "number"
    || !Number.isInteger(saved.strength) || saved.strength < 0 || saved.strength > 100) return null;
  if (saved.version === 1) {
    if (typeof saved.behindTable !== "boolean" || typeof saved.fit !== "string" || !["cover", "contain"].includes(saved.fit)) return null;
  } else if (saved.version !== 2) return null;
  // Version 2 scopes wallpaper to the table body and always covers it. Old
  // placement/fit options no longer affect the retained image or visibility.
  return { version: 2, enabled: saved.enabled, strength: saved.strength };
}
export function parseWallpaperImage(value: unknown): WallpaperImage {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid background image.");
  const image = value as WallpaperImage;
  if (!Number.isInteger(image.width) || !Number.isInteger(image.height) || image.width < 1 || image.height < 1
    || Math.max(image.width, image.height) > 3840 || image.width * image.height > 8_000_000
    || typeof image.dataUrl !== "string" || image.dataUrl.length > 24_000_000
    || !/^data:image\/(?:png|jpeg);base64,[a-z\d+/]+={0,2}$/i.test(image.dataUrl)) throw new Error("Invalid or oversized background image.");
  return { dataUrl: image.dataUrl, width: image.width, height: image.height };
}
export function wallpaperVisible(settings: WallpaperSettings, image: WallpaperImage | null): boolean {
  return settings.enabled && settings.strength > 0 && image !== null;
}

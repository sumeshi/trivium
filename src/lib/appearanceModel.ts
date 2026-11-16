export interface WallpaperSettings {
  version: 1;
  enabled: boolean;
  fit: "cover" | "contain";
  strength: number;
  behindTable: boolean;
}
export interface WallpaperImage { dataUrl: string; width: number; height: number; }
export const DEFAULT_WALLPAPER: WallpaperSettings = { version: 1, enabled: false, fit: "cover", strength: 30, behindTable: false };
export function parseWallpaperSettings(value: unknown): WallpaperSettings | null {
  if (typeof value === "string") { try { value = JSON.parse(value); } catch { return null; } }
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const saved = value as WallpaperSettings;
  if (saved.version !== 1 || typeof saved.enabled !== "boolean" || typeof saved.behindTable !== "boolean"
    || !["cover", "contain"].includes(saved.fit) || !Number.isInteger(saved.strength) || saved.strength < 0 || saved.strength > 100) return null;
  return { version: 1, enabled: saved.enabled, fit: saved.fit, strength: saved.strength, behindTable: saved.behindTable };
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

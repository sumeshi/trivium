/** Public build identity; evidence SHA-256 values describe imported files. */
export const APP_VERSION = typeof __TRIVIUM_VERSION__ === "string" ? __TRIVIUM_VERSION__ : "development";
export const BUILD_COMMIT = typeof __TRIVIUM_COMMIT__ === "string" ? __TRIVIUM_COMMIT__ : "";
export const BUILD_LABEL = `${APP_VERSION}${BUILD_COMMIT ? ` · ${BUILD_COMMIT}` : ""}`;

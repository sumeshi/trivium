# Downloading the application

For normal investigation with a graphical table, download the **trivium-deb**
artifact from a successful **Debian build** on `develop`. Extract it and install
the `.deb` file. Windows release artifacts named **trivium-x86_64-pc-windows-msvc**
contain the graphical `.exe` application.

Artifacts named **trivium-cli-…** contain an optional command-line automation
program. They are intended for scripts such as Python and are not required to
use the graphical application. Its protocol is documented in [automation.md](automation.md).

Each artifact includes:

- `BUILD-INFO.json`: application version, full source commit, platform and the SHA-256 of each executable or package.
- `SHA256SUMS`: checksums for the extracted `.deb` or `.exe` files.
- `README.txt`: which program the artifact contains and how it is used.

GitHub separately displays a SHA-256 digest for its complete downloadable ZIP.
That digest differs from the checksum of the `.deb` or `.exe` inside it. To
identify an update, check the build's commit and `BUILD-INFO.json`; to verify
an extracted file, compare it with `SHA256SUMS`.

The application's **Workspace options** menu displays its version and short
source commit. The SHA-256 next to an opened file's name identifies that imported
evidence file, independently of the application version.

`develop` Debian, optional Linux CLI and native-test evidence artifacts are
retained for three days. Windows release artifacts are retained for thirty days.

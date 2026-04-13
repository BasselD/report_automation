For your setup, the easiest path is:

**PyInstaller + a small launcher script + a one-folder build.**

That gives users a normal `.exe` they can double-click, without installing Python, while your app still runs the way Streamlit expects. A local Streamlit app starts a local server and opens in the user’s browser. PyInstaller’s default output is a one-folder bundle, and PyInstaller is not a cross-compiler, so build the Windows `.exe` on Windows. ([PyInstaller][1])

One caveat. Streamlit’s **official** documented way to start an app is `streamlit run your_script.py`. For packaging, the cleanest working approach is to call Streamlit’s current `streamlit.web.bootstrap.run(...)` from a launcher script inside the bundled app. That function exists in current Streamlit source, but it is an internal path rather than the main public entry point, so pin your Streamlit version and retest when you upgrade it. ([Streamlit Docs][2])

---

## What you will build

Your final folder will look like this:

```text
your_project/
├── app.py
├── launcher.py
├── build.bat
├── .streamlit/
│   └── config.toml
├── assets/                # optional
└── pages/                 # optional, if your app is multipage
```

When the user runs:

```text
dist\MyLocalApp\MyLocalApp.exe
```

it will:

1. locate the bundled `app.py`
2. start Streamlit locally
3. open the app in the browser
4. let the app read data from the shared drive using the user’s own permissions

That matches normal Streamlit behavior, just packaged. ([Streamlit Docs][2])

---

# Step 1. Create a clean build environment

Use a virtual environment. It keeps the build predictable and cuts down on junk getting pulled into the executable.

### Windows CMD

```bash
py -3.11 -m venv .venv
.venv\Scripts\activate

python -m pip install --upgrade pip
pip install streamlit duckdb pyinstaller pandas pyarrow
```

If your app uses other packages like `plotly`, `openpyxl`, `numpy`, or `altair`, install those too before building.

Because PyInstaller is not a cross-compiler, do this on the same OS you plan to distribute to. For a Windows `.exe`, build on Windows. ([PyInstaller][3])

---

# Step 2. Add a local Streamlit config

Create this file:

## `.streamlit/config.toml`

```toml
[server]
headless = false
runOnSave = false

[browser]
serverAddress = "localhost"
gatherUsageStats = false
```

Why this matters:

* Streamlit looks for `.streamlit/config.toml` in the working directory for per-project settings. ([Streamlit Docs][4])
* `headless = false` allows Streamlit to open the browser window on start. Streamlit’s config reference says that when `headless` is false, it will attempt to open a browser window on start. ([Streamlit Docs][4])
* `browser.serverAddress = "localhost"` keeps the browser target local. ([Streamlit Docs][4])
* `gatherUsageStats = false` disables usage stats. ([Streamlit Docs][4])

---

# Step 3. Create the launcher script

This is the key piece.

Instead of trying to call `streamlit run` from an external Python install, the launcher starts Streamlit from inside the bundled app. It also handles PyInstaller’s bundled file location using `sys._MEIPASS`, which PyInstaller sets at runtime for frozen apps. ([PyInstaller][5])

## `launcher.py`

```python
import os
import sys
import socket
from pathlib import Path

from streamlit.web import bootstrap


def get_bundle_dir() -> Path:
    """
    Return the directory that contains bundled app files.

    In a normal Python run, this is the folder containing launcher.py.
    In a PyInstaller build, this is the extracted bundle folder.
    """
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


def find_free_port(start: int = 8501, end: int = 8599) -> int:
    """
    Pick a free localhost port so the app doesn't fail if 8501 is already busy.
    """
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("No free localhost port found.")


def main() -> None:
    bundle_dir = get_bundle_dir()
    app_path = bundle_dir / "app.py"

    if not app_path.exists():
        raise FileNotFoundError(f"Could not find bundled Streamlit app: {app_path}")

    # Important. Streamlit looks for .streamlit/config.toml in the working directory.
    os.chdir(bundle_dir)

    port = find_free_port()

    # These are the Streamlit config options we want to force at runtime.
    flag_options = {
        "server.headless": False,
        "server.address": "localhost",
        "server.port": port,
        "browser.serverAddress": "localhost",
        "browser.serverPort": port,
        "browser.gatherUsageStats": False,
    }

    bootstrap.load_config_options(flag_options)

    # Equivalent idea to "streamlit run app.py", but launched from inside Python.
    bootstrap.run(
        str(app_path),   # main_script_path
        False,           # is_hello
        [],              # args passed to app.py
        flag_options,    # Streamlit config overrides
    )


if __name__ == "__main__":
    main()
```

Why this works:

* Streamlit’s current bootstrap module exposes `load_config_options(...)` and `run(...)`. ([GitHub][6])
* In the Streamlit source, `run(...)` starts the server, and `_on_server_start(...)` opens the browser unless `server.headless` is true. ([GitHub][6])
* `sys._MEIPASS` is the PyInstaller runtime path for bundled files. ([PyInstaller][5])

---

# Step 4. Optional, add a helper for bundled local files

If your app uses local files like logos, CSS, templates, or a local DuckDB file shipped with the app, add this helper to `app.py`.

## Put this near the top of `app.py`

```python
import sys
from pathlib import Path


def bundled_path(*parts: str) -> Path:
    """
    Resolve a file path both in normal Python runs and in PyInstaller builds.
    """
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        base_dir = Path(sys._MEIPASS)
    else:
        base_dir = Path(__file__).resolve().parent

    return base_dir.joinpath(*parts)
```

### Example usage

```python
logo_path = bundled_path("assets", "logo.png")
```

PyInstaller documents that bundled apps access included files through the runtime bundle path, and `sys._MEIPASS` is the standard way to distinguish bundled runs from normal runs. ([PyInstaller][5])

---

# Step 5. Keep your shared-drive and DuckDB logic simple

If your app reads directly from a shared drive, keep that code as normal Python. Example:

```python
import duckdb

SHARED_DB = r"\\your-server\your-share\analytics\my_data.duckdb"

con = duckdb.connect(SHARED_DB, read_only=True)
df = con.execute("SELECT * FROM some_table LIMIT 100").df()
```

If you are using **DuckDB extensions** like `httpfs`, `spatial`, or `excel`, test carefully on a clean machine. DuckDB installs extensions from a repository and caches them in the user’s home directory, then loads them from there on future runs. ([DuckDB][7])

A safe extension pattern looks like this:

```python
import duckdb

def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path, read_only=True)

    # Only keep these if your app actually needs them.
    # They may require first-run extension install.
    # con.install_extension("httpfs")
    # con.load_extension("httpfs")

    return con
```

---

# Step 6. Create the build script

Start with a **one-folder** build and keep the console visible at first. PyInstaller’s default mode is one-folder, and that is easier to debug than hiding everything behind one file and one silent failure. PyInstaller documents `--onedir` and `--onefile`, and `--console` is the default while `--windowed` hides the console. ([PyInstaller][8])

## `build.bat`

```bat
@echo off
setlocal

set APP_NAME=MyLocalApp

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist "%APP_NAME%.spec" del /q "%APP_NAME%.spec"

pyinstaller --noconfirm --clean --onedir --console ^
  --name "%APP_NAME%" ^
  --add-data "app.py:." ^
  --add-data ".streamlit:.streamlit" ^
  --collect-all streamlit ^
  --collect-all duckdb ^
  launcher.py

echo.
echo Build complete.
echo Run this file:
echo dist\%APP_NAME%\%APP_NAME%.exe
echo.
pause
```

### If your app has local pages

Add this line before `launcher.py`:

```bat
  --add-data "pages:pages" ^
```

### If your app has local assets

Add this line too:

```bat
  --add-data "assets:assets" ^
```

Why these flags:

* `--add-data` bundles files or directories into the app. ([PyInstaller][8])
* `--collect-all streamlit` and `--collect-all duckdb` tell PyInstaller to collect submodules, data files, and binaries for those packages. ([PyInstaller][8])
* `--console` keeps a console window for standard I/O. `--windowed` hides it. ([PyInstaller][8])

---

# Step 7. Build it

From the activated virtual environment:

```bash
build.bat
```

Or run the equivalent command directly if you prefer.

PyInstaller writes the distributable app into the `dist` folder. In a one-folder build, that output is a folder containing the executable and supporting files. ([PyInstaller][8])

---

# Step 8. Test it correctly

Test in this order:

### 1. Test from source

Make sure this still works first:

```bash
streamlit run app.py
```

That is Streamlit’s documented local run path and should still be your baseline sanity check. ([Streamlit Docs][2])

### 2. Test the launcher directly

Before building, run:

```bash
python launcher.py
```

That checks the bootstrap path without PyInstaller in the way.

### 3. Test the built executable

Run:

```text
dist\MyLocalApp\MyLocalApp.exe
```

You should see the local browser tab open.

### 4. Test on a clean Windows machine

Best case:

* no Python installed
* same shared-drive permissions as your users
* same browser and endpoint security environment they use

That is where packaging lies get exposed. Machines are brutally honest.

---

# Step 9. Distribute it

For a one-folder build, distribute the **entire folder**, not just the `.exe`.

Zip and share:

```text
dist\MyLocalApp\
```

PyInstaller’s docs state that the bundled app to distribute is what gets written into `dist`, and in one-folder mode that is the executable folder. ([PyInstaller][8])

---

# Optional. Hide the console after everything works

Once the app is stable, change `--console` to `--windowed` in `build.bat`:

```bat
pyinstaller --noconfirm --clean --onedir --windowed ^
  --name "%APP_NAME%" ^
  --add-data "app.py:." ^
  --add-data ".streamlit:.streamlit" ^
  --collect-all streamlit ^
  --collect-all duckdb ^
  launcher.py
```

PyInstaller documents that `--windowed` removes the console window on Windows and macOS. ([PyInstaller][8])

My practical advice is to **keep `--console` until the app is fully stable**. A hidden console plus a packaging bug is a classic way to create a “nothing happens” app. Very elegant. Also very useless.

---

# Troubleshooting

## Missing module error at runtime

Add more package collection flags:

```bat
--collect-all pandas ^
--collect-all pyarrow ^
--collect-all plotly ^
```

If needed, add explicit hidden imports:

```bat
--hidden-import some.module ^
```

PyInstaller’s docs explicitly support `--collect-all`, `--collect-submodules`, `--collect-data`, and `--hidden-import` for packages it cannot fully discover automatically. ([PyInstaller][8])

## Browser does not open

Make sure:

* `server.headless` is `false`
* `browser.serverAddress` is `localhost`

Streamlit’s config says `headless = false` will attempt to open a browser window on start, and current bootstrap code skips browser opening when `server.headless` is true. ([Streamlit Docs][4])

## Your local files disappear after packaging

Use the `bundled_path(...)` helper shown above. PyInstaller documents that bundled files live in the runtime bundle path, exposed through `sys._MEIPASS`. ([PyInstaller][5])

## DuckDB extension works on your machine but not theirs

That usually means the target machine needs the extension installed or cannot reach the extension repository. DuckDB documents that extensions are installed from repositories and cached in the user’s home directory. ([DuckDB][7])

---

# The shortest version

If you only want the core files, here they are:

## `launcher.py`

```python
import os
import sys
import socket
from pathlib import Path
from streamlit.web import bootstrap


def get_bundle_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


def find_free_port(start: int = 8501, end: int = 8599) -> int:
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("No free localhost port found.")


def main() -> None:
    bundle_dir = get_bundle_dir()
    app_path = bundle_dir / "app.py"
    os.chdir(bundle_dir)

    port = find_free_port()

    flag_options = {
        "server.headless": False,
        "server.address": "localhost",
        "server.port": port,
        "browser.serverAddress": "localhost",
        "browser.serverPort": port,
        "browser.gatherUsageStats": False,
    }

    bootstrap.load_config_options(flag_options)
    bootstrap.run(str(app_path), False, [], flag_options)


if __name__ == "__main__":
    main()
```

## `.streamlit/config.toml`

```toml
[server]
headless = false
runOnSave = false

[browser]
serverAddress = "localhost"
gatherUsageStats = false
```

## `build.bat`

```bat
@echo off
setlocal

set APP_NAME=MyLocalApp

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist "%APP_NAME%.spec" del /q "%APP_NAME%.spec"

pyinstaller --noconfirm --clean --onedir --console ^
  --name "%APP_NAME%" ^
  --add-data "app.py:." ^
  --add-data ".streamlit:.streamlit" ^
  --collect-all streamlit ^
  --collect-all duckdb ^
  launcher.py

pause
```

---

Your app is a solid candidate for the **PyInstaller + launcher** approach. The app is already a single-file Streamlit app backed by DuckDB, it reads from a file path entered in the UI, and the logo is a separate file. That fits the local-browser model well. I did spot two packaging-related issues in your current code:

1. `base64` is used in `render_banner()` but never imported, so the app will crash when the logo file exists.
2. `DATA_PATH` is currently hardcoded to a local macOS path. For sharing to Windows users, that default should be changed to a UNC shared-drive path, or at least a placeholder UNC path. Your app already supports user override through a text input, which is good. 

For packaging, I recommend:

* **PyInstaller**
* **one-folder build first**
* a small **launcher.py**
* keep Streamlit opening in the user’s browser locally
* do **not** bundle the shared-drive data file itself

PyInstaller’s default output is a one-folder bundle, and it supports `--add-data`, `--collect-all`, `--windowed`, and related options for packaging data files and dependencies. ([PyInstaller][1])
For bundled apps, PyInstaller recommends locating bundled files relative to `__file__` in the entry script, because `__file__` points inside the bundle at runtime. ([PyInstaller][2])
Streamlit supports a per-project `.streamlit/config.toml`, and that file is read from the working directory where the app is launched. Streamlit’s bootstrap module also exposes `load_config_options(...)` and `run(...)`, which is the cleanest way to start the app from a packaged launcher. ([Streamlit Docs][3])

---

## What I would change in your app first

### 1. Add the missing import and make asset paths packaging-safe

Replace the top of your file with this:

```python
import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
from pathlib import Path
import io
import base64
import sys
```

Add this helper near the top, right after your imports:

```python
def bundled_path(*parts: str) -> Path:
    """
    Resolve a file path in both normal runs and PyInstaller builds.
    """
    return Path(__file__).resolve().parent.joinpath(*parts)
```

### 2. Update your configuration section

Replace this:

```python
#DATA_PATH = r"\\shared_drive\analytics\professional_spend.parquet"
DATA_PATH = r"/Users/bassel_instructor/Documents/datasets/medicare_synthetic_12k.parquet"
LOGO_PATH = r"logo.png"
```

with this:

```python
DATA_PATH = r"\\your-server\your-share\analytics\professional_spend.parquet"
LOGO_PATH = bundled_path("logo.png")
```

That keeps the logo local to the packaged app, while the data continues to come from the shared drive. Your current file already expects the logo to be a separate file and already lets the data path be overridden in the UI. 

### 3. Update `render_banner()`

Replace your current `render_banner()` with this:

```python
def render_banner(title, subtitle, color, logo_path):
    logo_path = Path(logo_path)

    if logo_path.exists():
        with open(logo_path, "rb") as f:
            ext = logo_path.suffix.lower().replace(".", "")
            mime = "jpeg" if ext == "jpg" else ext
            b64 = base64.b64encode(f.read()).decode()
        logo_html = f'<img src="data:image/{mime};base64,{b64}" class="banner-logo" alt="logo">'
    else:
        logo_html = '<span class="banner-logo-placeholder">[ logo.png ]</span>'

    st.markdown(
        f'<div class="app-banner" style="background-color:{color};">'
        f'<div><p class="banner-title">{title}</p>'
        f'<p class="banner-subtitle">{subtitle}</p></div>'
        f'{logo_html}</div>',
        unsafe_allow_html=True,
    )
```

That fixes the missing import problem and keeps the logo lookup clean. Your original function uses `base64.b64encode(...)` without importing `base64`. 

---

# Final project structure

Set your folder up like this:

```text
professional_spend_app/
│
├── professional_spend_report.py
├── launcher.py
├── build.bat
├── logo.png
└── .streamlit/
    └── config.toml
```

---

# Step 1. Create `launcher.py`

This file is the real entry point for PyInstaller.

```python
import os
import socket
from pathlib import Path

from streamlit.web import bootstrap


def find_free_port(start: int = 8501, end: int = 8599) -> int:
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("No free localhost port found.")


def main() -> None:
    bundle_dir = Path(__file__).resolve().parent
    app_path = bundle_dir / "professional_spend_report.py"

    if not app_path.exists():
        raise FileNotFoundError(f"Could not find app file: {app_path}")

    # Important. Streamlit reads .streamlit/config.toml from the working directory.
    os.chdir(bundle_dir)

    port = find_free_port()

    flag_options = {
        "server.headless": False,
        "server.address": "localhost",
        "server.port": port,
        "browser.serverAddress": "localhost",
        "browser.serverPort": port,
        "browser.gatherUsageStats": False,
    }

    bootstrap.load_config_options(flag_options)
    bootstrap.run(str(app_path), False, [], flag_options)


if __name__ == "__main__":
    main()
```

Why this launcher pattern works:

* Streamlit’s bootstrap module exposes `load_config_options(...)` and `run(...)`. ([GitHub][4])
* Streamlit reads `.streamlit/config.toml` from the working directory, so setting `os.chdir(bundle_dir)` matters. ([Streamlit Docs][3])
* For bundled apps, using paths relative to the bundled script location is the recommended pattern. ([PyInstaller][2])

---

# Step 2. Create `.streamlit/config.toml`

```toml
[server]
headless = false
runOnSave = false

[browser]
serverAddress = "localhost"
gatherUsageStats = false
```

This keeps the app local and browser-based, which is exactly what you want. Streamlit documents `.streamlit/config.toml` as the per-project config location in the working directory. ([Streamlit Docs][3])

---

# Step 3. Save your updated app file

Use your existing `professional_spend_report.py`, but apply these exact edits:

## Replace imports

```python
import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
from pathlib import Path
import io
import base64
import sys
```

## Add helper right after imports

```python
def bundled_path(*parts: str) -> Path:
    return Path(__file__).resolve().parent.joinpath(*parts)
```

## Replace config block

```python
DATA_PATH = r"\\your-server\your-share\analytics\professional_spend.parquet"
LOGO_PATH = bundled_path("logo.png")
BANNER_COLOR = "rgb(0, 40, 80)"
GRANULARITY_DEFAULT = "Managing Entity"
```

## Replace `render_banner()` with this version

```python
def render_banner(title, subtitle, color, logo_path):
    logo_path = Path(logo_path)

    if logo_path.exists():
        with open(logo_path, "rb") as f:
            ext = logo_path.suffix.lower().replace(".", "")
            mime = "jpeg" if ext == "jpg" else ext
            b64 = base64.b64encode(f.read()).decode()
        logo_html = f'<img src="data:image/{mime};base64,{b64}" class="banner-logo" alt="logo">'
    else:
        logo_html = '<span class="banner-logo-placeholder">[ logo.png ]</span>'

    st.markdown(
        f'<div class="app-banner" style="background-color:{color};">'
        f'<div><p class="banner-title">{title}</p>'
        f'<p class="banner-subtitle">{subtitle}</p></div>'
        f'{logo_html}</div>',
        unsafe_allow_html=True,
    )
```

Everything else in your app can stay as-is for the first pass. Your app already uses a text input for the shared-drive file path, DuckDB queries, chart selection, and downloadable output, so there is no need to rewrite the whole beast just to package it. 

---

# Step 4. Create `build.bat`

Use this exact file on Windows:

```bat
@echo off
setlocal

set APP_NAME=ProfessionalSpendApp

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist %APP_NAME%.spec del /q %APP_NAME%.spec

pyinstaller --noconfirm --clean --onedir --console ^
  --name "%APP_NAME%" ^
  --add-data "professional_spend_report.py:." ^
  --add-data ".streamlit:.streamlit" ^
  --add-data "logo.png:." ^
  --collect-all streamlit ^
  --collect-all duckdb ^
  --collect-all plotly ^
  --collect-all pandas ^
  --collect-all pyarrow ^
  launcher.py

echo.
echo Build complete.
echo.
echo Run:
echo dist\%APP_NAME%\%APP_NAME%.exe
echo.
pause
```

Why these flags:

* `--onedir` creates a one-folder bundle and is the default mode. ([PyInstaller][1])
* `--add-data SOURCE:DEST` includes files like your app script, logo, and `.streamlit` folder. ([PyInstaller][1])
* `--collect-all` pulls submodules, data, and binaries for packages that commonly need extra collection help. ([PyInstaller][1])
* `--console` keeps the terminal visible while debugging. Once stable, you can switch to `--windowed`. PyInstaller documents both modes. ([PyInstaller][1])

---

# Step 5. Build it

From a Windows terminal in that folder:

```bash
py -3.11 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install streamlit duckdb plotly pandas pyarrow pyinstaller
build.bat
```

After the build completes, run:

```text
dist\ProfessionalSpendApp\ProfessionalSpendApp.exe
```

---

# Step 6. Test in the right order

## First, run from source

```bash
streamlit run professional_spend_report.py
```

Make sure your current app works after the small fixes.

## Next, test the launcher directly

```bash
python launcher.py
```

If this fails, the problem is in the launcher or Streamlit setup, not PyInstaller.

## Then, build and test the executable

```text
dist\ProfessionalSpendApp\ProfessionalSpendApp.exe
```

## Finally, test on a clean Windows machine

This is the real exam, not the build box victory lap.

Test with:

* no Python installed
* access to the shared drive
* access to the target parquet or CSV
* whatever browser they normally use

---

# Step 7. Distribute it

Zip and share the **entire** folder:

```text
dist\ProfessionalSpendApp\
```

Not just the `.exe`. The whole folder. Lone `.exe` distribution is how people discover new forms of sadness.

---

# After it works, make it cleaner

Once you confirm the app launches correctly, change `--console` to `--windowed` in `build.bat`:

```bat
pyinstaller --noconfirm --clean --onedir --windowed ^
  --name "%APP_NAME%" ^
  --add-data "professional_spend_report.py:." ^
  --add-data ".streamlit:.streamlit" ^
  --add-data "logo.png:." ^
  --collect-all streamlit ^
  --collect-all duckdb ^
  --collect-all plotly ^
  --collect-all pandas ^
  --collect-all pyarrow ^
  launcher.py
```

PyInstaller documents `--windowed` as the no-console mode on Windows and macOS. ([PyInstaller][1])

---

# Known gotchas for your exact app

## 1. Shared drive path

Your current code already supports a path typed into the UI, and the help text explicitly mentions UNC paths like `\\server\share\file.parquet`. That is good. Use UNC paths, not mapped drives like `Z:\...`, unless every user has the same mapping. 

## 2. Missing logo

If `logo.png` is not next to the packaged app, your app falls back to a placeholder. That behavior already exists in your code. 

## 3. First-run failures with DuckDB extras

Your current app only uses core DuckDB operations, which is simpler than dealing with downloadable DuckDB extensions. That helps. 

## 4. SQL quoting edge case

You are building SQL by string interpolation for filters and file paths. That is okay for an internal first version, but if one of your filter values contains an apostrophe, it can break the query. Not a packaging blocker, just a future cleanup item. Your current code uses direct string concatenation in `where_clause()` and `append_dim_filter()`. 

---

# The shortest working recipe

If you want the stripped-down version, here it is.

## `launcher.py`

```python
import os
import socket
from pathlib import Path
from streamlit.web import bootstrap


def find_free_port(start=8501, end=8599):
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("No free localhost port found.")


def main():
    bundle_dir = Path(__file__).resolve().parent
    app_path = bundle_dir / "professional_spend_report.py"
    os.chdir(bundle_dir)

    port = find_free_port()

    flag_options = {
        "server.headless": False,
        "server.address": "localhost",
        "server.port": port,
        "browser.serverAddress": "localhost",
        "browser.serverPort": port,
        "browser.gatherUsageStats": False,
    }

    bootstrap.load_config_options(flag_options)
    bootstrap.run(str(app_path), False, [], flag_options)


if __name__ == "__main__":
    main()
```

## `.streamlit/config.toml`

```toml
[server]
headless = false
runOnSave = false

[browser]
serverAddress = "localhost"
gatherUsageStats = false
```

## `build.bat`

```bat
@echo off
setlocal

set APP_NAME=ProfessionalSpendApp

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist %APP_NAME%.spec del /q %APP_NAME%.spec

pyinstaller --noconfirm --clean --onedir --console ^
  --name "%APP_NAME%" ^
  --add-data "professional_spend_report.py:." ^
  --add-data ".streamlit:.streamlit" ^
  --add-data "logo.png:." ^
  --collect-all streamlit ^
  --collect-all duckdb ^
  --collect-all plotly ^
  --collect-all pandas ^
  --collect-all pyarrow ^
  launcher.py

pause
```

---

# My recommendation

Do this in two passes:

1. **Fix the app** with the small changes above.
2. **Package it** with the launcher and one-folder build.

That is the least painful route for what you want.

Also, I reviewed your uploaded script directly here. 

Paste the exact shared-drive filename pattern and whether your users are all on Windows, and I’ll tailor the final `DATA_PATH` default plus a production-ready `build.bat` for your environment.

[1]: https://pyinstaller.org/en/stable/usage.html "Using PyInstaller — PyInstaller 6.19.0 documentation"
[2]: https://pyinstaller.org/en/stable/runtime-information.html "Run-time Information — PyInstaller 6.19.0 documentation"
[3]: https://docs.streamlit.io/develop/api-reference/configuration/config.toml "config.toml - Streamlit Docs"
[4]: https://github.com/streamlit/streamlit/blob/develop/lib/streamlit/web/bootstrap.py "streamlit/lib/streamlit/web/bootstrap.py at develop · streamlit/streamlit · GitHub"

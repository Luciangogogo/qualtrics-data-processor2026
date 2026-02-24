# Qualtrics Data Processor

Python subproject for Qualtrics data processing: extract from Qualtrics API, transform, and load into PostgreSQL. Flask app with REST API.

## Prerequisites

- **pyenv** (and optionally **pyenv-virtualenv**) for Python version and virtualenv management
- PostgreSQL (for DB connection; can be local or remote)

## 1. Use the right Python version

This project uses Python **3.11** (see `.python-version`). With pyenv, the project directory will use that version automatically once the version is installed.

```bash
# Install Python 3.11 if you don't have it
pyenv install 3.11

# From the project root (qualtrics-data-processor), pyenv will use 3.11
cd /path/to/qualtrics-data-processor
pyenv version   # should show 3.11.x
```

## 2. Create a dedicated virtualenv (recommended)

So this project’s dependencies don’t affect other environments:

**Option A – pyenv-virtualenv (recommended with pyenv)**

```bash
cd /path/to/qualtrics-data-processor

# Create a virtualenv named qualtrics-data-processor using Python 3.11
pyenv virtualenv 3.11 qualtrics-data-processor

# Use this virtualenv whenever you're in this directory
pyenv local qualtrics-data-processor
```

After this, `pyenv` will auto-activate the `qualtrics-data-processor` virtualenv in this folder. Verify with:

```bash
which python   # should point to your pyenv shims/venv
pip --version
```

**Option B – built-in venv (no pyenv-virtualenv)**

```bash
cd /path/to/qualtrics-data-processor
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
```

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

## 4. Environment variables

The app reads configuration from a `.env` file in the project root.

```bash
cp .env.example .env
```

Edit `.env` and set at least:

| Variable | Description |
|----------|-------------|
| `QUALTRICS_API_TOKEN` | Your Qualtrics API token |
| `QUALTRICS_DATA_CENTER` | Qualtrics data center ID (e.g. `ca1`) |
| `DB_HOST` | PostgreSQL host |
| `DB_PORT` | PostgreSQL port (e.g. `5432`) |
| `DB_NAME` | Database name |
| `DB_USER` | Database user |
| `DB_PASSWORD` | Database password |

Optional: `FLASK_ENV=development`, `PORT=5000`, `LOG_LEVEL=INFO`, etc. See `.env.example`.

## 5. Run the project

From the **project root** (`qualtrics-data-processor`):

```bash
# Development server (Flask built-in)
python wsgi.py
```

Or:

```bash
python -m app.main
```

The API will listen on **http://0.0.0.0:5000** (or the port set by `PORT` in `.env`).

- **Health:** `GET http://localhost:5000/health`
- **API routes:** see `app/api/routes.py` for available endpoints.

### Production-style run (Gunicorn)

```bash
gunicorn -w 4 -b 0.0.0.0:5000 wsgi:app
```

## 6. Quick checks

- **Database connectivity:**

  ```bash
  python test_db_connection.py
  ```

- **Config / env:** Ensure all required env vars are set; the app will fail at startup with a clear error if any are missing.

## Summary (copy-paste)

```bash
cd /path/to/qualtrics-data-processor
pyenv install 3.11
pyenv virtualenv 3.11 qualtrics-data-processor
pyenv local qualtrics-data-processor
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your Qualtrics and DB values
python wsgi.py
```

Then open http://localhost:5000/health to confirm the app is running.

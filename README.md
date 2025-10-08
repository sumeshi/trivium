# Trivium

A modern web application for log analysis. Import CSV files, flag important records, add memos, and filter/search through your logs efficiently.

![Trivium Logo](frontend/public/logo.svg)

## Features

![Trivium ScreenShot](screenshot.png)

- **CSV Import/Export** - Load any CSV file, analyze it, and export with your flags and memos
- **Flag System** - Mark records with ◯ (OK), ? (Question), or ✗ (NG)
- **Memos** - Add notes to individual records
- **Search & Filter** - Real-time search and multi-flag filtering
- **Column Management** - Show/hide columns, settings are saved automatically
- **Sorting** - Click any column header to sort (handles dates, numbers, text intelligently)
- **Projects** - Manage multiple log files with descriptions
- **Dark Theme** - Catppuccin Frappé color scheme

## Quick Start

### Using Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/yourusername/trivium.git
cd trivium

# Start with Docker Compose
docker-compose up -d

# Open http://localhost:3000 in your browser
```

### Manual Setup

**Backend:**
```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Frontend:**
```bash
cd frontend
npm install
npm start
```

Open http://localhost:3000

## Usage

1. **Upload a CSV** - Click "Select CSV File" and upload
2. **Flag Records** - Click ◯/?/✗ icons to mark important logs
3. **Add Memos** - Click the comment icon to add notes
4. **Filter** - Use "Flag Filter" to show only specific flags
5. **Search** - Type in the search box to find logs
6. **Export** - Click "Export" to download with all your flags and memos

### Re-importing Exported Data

When you export and later re-import the CSV, all your flags and memos are automatically restored.

## Tech Stack

- **Backend**: FastAPI, SQLite, Pandas, Parquet
- **Frontend**: React, TypeScript, Material-UI

## License

MIT License - see [LICENSE](LICENSE)

## Acknowledgments

Inspired by [Timesketch](https://timesketch.org/)
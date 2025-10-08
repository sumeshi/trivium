from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List
import pandas as pd
from sqlalchemy import (
    create_engine, Column, Integer, String, MetaData, Table, ForeignKey, DateTime, event
)
from sqlalchemy.sql import func
from databases import Database
import datetime
import os
import io
from pathlib import Path

# --- Data Storage Setup ---
DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

DATABASE_URL = "sqlite:///./logs.db"
database = Database(DATABASE_URL)
metadata = MetaData()

projects_table = Table(
    "projects",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False),
    Column("description", String, nullable=True),
    Column("created_at", DateTime, default=func.now())
)

# Only store flags and memos, not the actual log data
# Flags stored as separate boolean columns for efficient filtering
flags_table = Table(
    "flags",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("project_id", Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    Column("row_index", Integer, nullable=False),
    Column("flag_ok", Integer, default=0),  # SQLite uses 0/1 for boolean
    Column("flag_question", Integer, default=0),
    Column("flag_ng", Integer, default=0),
    Column("memo", String, default=""),
)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=None
)
metadata.create_all(engine)

# Enable foreign key constraints for SQLite
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

# In-memory cache for DataFrames
df_cache = {}

# --- Helper Functions for Flag Conversion ---
def flag_to_bools(flag_str: str) -> tuple[int, int, int]:
    """Convert flag string to boolean tuple (ok, question, ng)."""
    if flag_str == "◯":
        return (1, 0, 0)
    elif flag_str == "?":
        return (0, 1, 0)
    elif flag_str == "✗":
        return (0, 0, 1)
    else:
        return (0, 0, 0)

def bools_to_flag(flag_ok: int, flag_question: int, flag_ng: int) -> str:
    """Convert boolean flags to string."""
    if flag_ok:
        return "◯"
    elif flag_question:
        return "?"
    elif flag_ng:
        return "✗"
    else:
        return ""

# --- Pydantic Models ---
class Project(BaseModel):
    id: int
    name: str
    description: str | None
    created_at: datetime.datetime
    total_records: int | None = 0
    flagged_records: int | None = 0
    hidden_columns: List[str] = []

class Log(BaseModel):
    id: int
    project_id: int
    data: dict
    flag: str
    memo: str

class FlagUpdate(BaseModel):
    flag: str
    memo: str | None = None

# --- FastAPI App ---
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# --- Lifecycle Events ---
@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# --- Helper Functions ---
def get_parquet_path(project_id: int) -> Path:
    return DATA_DIR / f"project_{project_id}.parquet"

def load_dataframe(project_id: int) -> pd.DataFrame:
    """Load DataFrame from cache or disk"""
    if project_id not in df_cache:
        parquet_path = get_parquet_path(project_id)
        if not parquet_path.exists():
            raise HTTPException(status_code=404, detail="Project data not found")
        df_cache[project_id] = pd.read_parquet(parquet_path)
    return df_cache[project_id]

async def get_flags_dict(project_id: int) -> dict:
    """Get flags and memos as a dictionary {row_index: {"flag": flag_str, "memo": memo}}"""
    query = flags_table.select().where(flags_table.c.project_id == project_id)
    flags = await database.fetch_all(query)
    return {
        flag["row_index"]: {
            "flag": bools_to_flag(flag["flag_ok"], flag["flag_question"], flag["flag_ng"]), 
            "memo": flag["memo"]
        } 
        for flag in flags
    }

# --- API Endpoints ---

@app.get("/api/projects")
async def get_projects():
    query = projects_table.select().order_by(projects_table.c.created_at.desc())
    projects = await database.fetch_all(query)
    
    # Enhance each project with record counts
    result = []
    for project in projects:
        project_dict = dict(project)
        
        # Get total records from Parquet file
        try:
            parquet_path = get_parquet_path(project["id"])
            if parquet_path.exists():
                df = pd.read_parquet(parquet_path)
                project_dict["total_records"] = len(df)
            else:
                project_dict["total_records"] = 0
        except:
            project_dict["total_records"] = 0
        
        # Get flagged records count
        flags_query = flags_table.select().where(
            (flags_table.c.project_id == project["id"]) & 
            ((flags_table.c.flag_ok == 1) | (flags_table.c.flag_question == 1) | (flags_table.c.flag_ng == 1))
        )
        flagged = await database.fetch_all(flags_query)
        project_dict["flagged_records"] = len(flagged)
        
        result.append(project_dict)
    
    return result

@app.post("/api/projects")
async def create_project(file: UploadFile = File(...), description: str = Form(default="")):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided.")
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV.")

    project_query = projects_table.insert().values(name=file.filename, description=description)
    project_id = await database.execute(project_query)

    try:
        # Read CSV into DataFrame
        df = pd.read_csv(file.file, low_memory=False)
        
        # Check for trivium columns and extract them
        trivium_data = {}  # {row_index: {"flag_ok": 0/1, "flag_question": 0/1, "flag_ng": 0/1, "memo": ""}}
        has_trivium_data = False
        
        # Check for separate flag columns
        if 'trivium-flag-ok' in df.columns:
            has_trivium_data = True
            for idx, flag in enumerate(df['trivium-flag-ok']):
                if pd.notna(flag) and flag != '':
                    if idx not in trivium_data:
                        trivium_data[idx] = {"flag_ok": 0, "flag_question": 0, "flag_ng": 0, "memo": ""}
                    trivium_data[idx]["flag_ok"] = 1
            df = df.drop(columns=['trivium-flag-ok'])
        
        if 'trivium-flag-question' in df.columns:
            has_trivium_data = True
            for idx, flag in enumerate(df['trivium-flag-question']):
                if pd.notna(flag) and flag != '':
                    if idx not in trivium_data:
                        trivium_data[idx] = {"flag_ok": 0, "flag_question": 0, "flag_ng": 0, "memo": ""}
                    trivium_data[idx]["flag_question"] = 1
            df = df.drop(columns=['trivium-flag-question'])
        
        if 'trivium-flag-ng' in df.columns:
            has_trivium_data = True
            for idx, flag in enumerate(df['trivium-flag-ng']):
                if pd.notna(flag) and flag != '':
                    if idx not in trivium_data:
                        trivium_data[idx] = {"flag_ok": 0, "flag_question": 0, "flag_ng": 0, "memo": ""}
                    trivium_data[idx]["flag_ng"] = 1
            df = df.drop(columns=['trivium-flag-ng'])
        
        if 'trivium-memo' in df.columns:
            has_trivium_data = True
            for idx, memo in enumerate(df['trivium-memo']):
                if pd.notna(memo) and memo != '':
                    if idx not in trivium_data:
                        trivium_data[idx] = {"flag_ok": 0, "flag_question": 0, "flag_ng": 0, "memo": ""}
                    trivium_data[idx]["memo"] = str(memo)
            df = df.drop(columns=['trivium-memo'])
        
        # Remove trivium-id if present (we use our own row indices)
        if 'trivium-id' in df.columns:
            df = df.drop(columns=['trivium-id'])
        
        # Detect hidden columns (columns starting with -)
        hidden_columns = []
        columns_to_rename = {}
        for col in df.columns:
            if col.startswith('-'):
                original_name = col[1:]  # Remove the - prefix
                hidden_columns.append(original_name)
                columns_to_rename[col] = original_name
        
        # Rename hidden columns (remove - prefix)
        if columns_to_rename:
            df = df.rename(columns=columns_to_rename)
        
        # Try to detect and parse datetime columns automatically
        for col in df.columns:
            # Skip if already numeric
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            # Try to parse as datetime if column name suggests it or values look like dates
            sample = df[col].dropna().head(100)
            if len(sample) > 0:
                # Check if column name suggests datetime
                is_time_col = any(keyword in col.lower() for keyword in ['time', 'date', 'timestamp', 'created', 'updated', 'at'])
                
                # Or check if first few values look like dates
                first_val = str(sample.iloc[0])
                looks_like_date = any(char in first_val for char in ['/', '-', ':']) and any(char.isdigit() for char in first_val)
                
                if is_time_col or looks_like_date:
                    try:
                        # Try to parse as datetime with UTC, handling various formats including timezone info
                        # Use format='mixed' to suppress warnings about multiple formats
                        df[col] = pd.to_datetime(df[col], errors='coerce', utc=True, format='mixed')
                    except:
                        pass
        
        # Replace NaN and Infinity with None
        df = df.replace([float('inf'), float('-inf')], pd.NA)
        
        # Save as Parquet (preserves data types including datetime)
        parquet_path = get_parquet_path(project_id)
        df.to_parquet(parquet_path, index=False)
        
        # Cache the DataFrame
        df_cache[project_id] = df
        
        # Import trivium flags and memos if present
        if has_trivium_data and trivium_data:
            flag_records = []
            for row_index, data in trivium_data.items():
                flag_records.append({
                    "project_id": project_id,
                    "row_index": row_index,
                    "flag_ok": data.get("flag_ok", 0),
                    "flag_question": data.get("flag_question", 0),
                    "flag_ng": data.get("flag_ng", 0),
                    "memo": data.get("memo", "")
                })
            
            if flag_records:
                insert_query = flags_table.insert()
                await database.execute_many(insert_query, flag_records)
        
        return {
            "status": "success", 
            "project_id": project_id, 
            "records_uploaded": len(df),
            "hidden_columns": hidden_columns
        }
    except Exception as e:
        # Rollback: delete project and parquet file
        rollback_query = projects_table.delete().where(projects_table.c.id == project_id)
        await database.execute(rollback_query)
        parquet_path = get_parquet_path(project_id)
        if parquet_path.exists():
            parquet_path.unlink()
        raise HTTPException(status_code=500, detail=f"An error occurred during CSV processing: {e}")

@app.delete("/api/projects/{project_id}")
async def delete_project(project_id: int):
    # Delete parquet file
    parquet_path = get_parquet_path(project_id)
    if parquet_path.exists():
        parquet_path.unlink()
    
    # Remove from cache
    if project_id in df_cache:
        del df_cache[project_id]
    
    # Explicitly delete flags first (for safety, even though CASCADE should handle it)
    flags_query = flags_table.delete().where(flags_table.c.project_id == project_id)
    await database.execute(flags_query)
    
    # Delete project from database
    project_query = projects_table.delete().where(projects_table.c.id == project_id)
    await database.execute(project_query)
    
    return {"status": "success", "deleted_project_id": project_id}

@app.get("/api/projects/{project_id}/logs")
async def get_project_logs(
    project_id: int, 
    offset: int = 0, 
    limit: int = 100, 
    sort_column: str = None, 
    sort_direction: str = "asc",
    search: str = None,
    flag_filter: List[str] = Query(default=None)
):
    # Load DataFrame
    df = load_dataframe(project_id)
    
    # Get flags
    flags_dict = await get_flags_dict(project_id)
    
    # Add row_index as a column (this will be our ID)
    df = df.reset_index(drop=True)
    df['_row_index'] = df.index
    
    # Apply search filter (before pagination, after adding row_index)
    if search and search.strip():
        search_lower = search.lower()
        # Search across all string columns
        mask = pd.Series([False] * len(df))
        for col in df.columns:
            if col != '_row_index':
                try:
                    mask |= df[col].astype(str).str.lower().str.contains(search_lower, na=False, regex=False)
                except:
                    pass
        df = df[mask]
    
    # Apply flag filter (before pagination)
    # Get flag data as DataFrame for efficient filtering
    if flag_filter and len(flag_filter) > 0:
        # Get raw flag data from database
        flags_query = flags_table.select().where(flags_table.c.project_id == project_id)
        flags_rows = await database.fetch_all(flags_query)
        
        # Create flag mask
        flag_mask = pd.Series([False] * len(df), index=df.index)
        
        # Check if "No Flag" is selected
        if "No Flag" in flag_filter:
            # Rows with no flag entry or all flags are 0
            flagged_indices = {row["row_index"] for row in flags_rows if row["flag_ok"] or row["flag_question"] or row["flag_ng"]}
            flag_mask |= ~df['_row_index'].isin(flagged_indices)
        
        # Check for specific flags
        if "◯" in flag_filter:
            ok_indices = {row["row_index"] for row in flags_rows if row["flag_ok"]}
            flag_mask |= df['_row_index'].isin(ok_indices)
        
        if "?" in flag_filter:
            question_indices = {row["row_index"] for row in flags_rows if row["flag_question"]}
            flag_mask |= df['_row_index'].isin(question_indices)
        
        if "✗" in flag_filter:
            ng_indices = {row["row_index"] for row in flags_rows if row["flag_ng"]}
            flag_mask |= df['_row_index'].isin(ng_indices)
        
        df = df[flag_mask]
    
    # Sort if specified
    if sort_column:
        if sort_column == 'id':
            sort_column = '_row_index'
        
        # Check if column exists
        if sort_column in df.columns:
            ascending = (sort_direction == 'asc')
            df = df.sort_values(by=sort_column, ascending=ascending, na_position='last')
    
    # Get total count after filtering
    total_count = len(df)
    
    # Apply pagination
    df_page = df.iloc[offset:offset + limit]
    
    # Get column types from the original DataFrame (before pagination)
    col_types = {}
    for col in df.columns:
        if col != '_row_index':
            dtype = str(df[col].dtype)
            if 'datetime' in dtype:
                col_types[col] = 'datetime'
            elif 'int' in dtype:
                col_types[col] = 'int'
            elif 'float' in dtype:
                col_types[col] = 'float'
            elif 'bool' in dtype:
                col_types[col] = 'bool'
            else:
                col_types[col] = 'string'
    
    # Convert to list of dicts with flags
    result = []
    for idx, row in df_page.iterrows():
        row_index = row['_row_index']
        row_data = row.drop('_row_index')
        
        # Clean up NaN and Infinity values for JSON compliance, convert timestamps to ISO
        row_dict = {}
        for col, val in row_data.items():
            if pd.isna(val):
                row_dict[col] = None
            elif isinstance(val, float) and (val == float('inf') or val == float('-inf')):
                row_dict[col] = None
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                # Convert datetime to ISO format string
                row_dict[col] = pd.Timestamp(val).isoformat() if not pd.isna(val) else None
            else:
                row_dict[col] = val
        
        flag_data = flags_dict.get(int(row_index), {"flag": "", "memo": ""})
        result.append({
            "id": int(row_index),
            "project_id": project_id,
            "data": row_dict,
            "flag": flag_data["flag"],
            "memo": flag_data["memo"]
        })
    
    return {"logs": result, "total": total_count, "offset": offset, "limit": limit, "column_types": col_types}

@app.get("/api/projects/{project_id}/logs/export")
async def export_project_logs_csv(
    project_id: int,
    sort_column: str = None,
    sort_direction: str = "asc",
    search: str = None,
    flag_filter: List[str] = Query(default=None),
    hidden_columns: List[str] = Query(default=None)
):
    """Export filtered and sorted logs as CSV, including flags and memos. Hidden columns are prefixed with -."""
    # Load DataFrame
    df = load_dataframe(project_id)
    
    # Get flags and memos
    flags_dict = await get_flags_dict(project_id)
    
    # Add row index for ID
    df['_row_index'] = df.index
    
    # Apply search filter
    if search and search.strip():
        search_lower = search.lower()
        mask = pd.Series([False] * len(df))
        for col in df.columns:
            if col != '_row_index':
                try:
                    mask |= df[col].astype(str).str.lower().str.contains(search_lower, na=False, regex=False)
                except:
                    pass
        df = df[mask]
    
    # Apply flag filter
    if flag_filter and len(flag_filter) > 0:
        # Get raw flag data from database for efficient filtering
        flags_query = flags_table.select().where(flags_table.c.project_id == project_id)
        flags_rows = await database.fetch_all(flags_query)
        
        # Create flag mask
        flag_mask = pd.Series([False] * len(df), index=df.index)
        
        # Check if "No Flag" is selected
        if "No Flag" in flag_filter:
            flagged_indices = {row["row_index"] for row in flags_rows if row["flag_ok"] or row["flag_question"] or row["flag_ng"]}
            flag_mask |= ~df['_row_index'].isin(flagged_indices)
        
        # Check for specific flags
        if "◯" in flag_filter:
            ok_indices = {row["row_index"] for row in flags_rows if row["flag_ok"]}
            flag_mask |= df['_row_index'].isin(ok_indices)
        
        if "?" in flag_filter:
            question_indices = {row["row_index"] for row in flags_rows if row["flag_question"]}
            flag_mask |= df['_row_index'].isin(question_indices)
        
        if "✗" in flag_filter:
            ng_indices = {row["row_index"] for row in flags_rows if row["flag_ng"]}
            flag_mask |= df['_row_index'].isin(ng_indices)
        
        df = df[flag_mask]
    
    # Sort if specified
    if sort_column:
        if sort_column == 'id':
            sort_column = '_row_index'
        
        ascending = (sort_direction == 'asc')
        df = df.sort_values(by=sort_column, ascending=ascending, na_position='last')
    else:
        # Default sort by row_index
        df = df.sort_values(by='_row_index', ascending=True)
    
    # Add flag columns (one per flag type) and memo column with trivium- prefix
    # Get all flags for this project for efficient merging
    flags_query = flags_table.select().where(flags_table.c.project_id == project_id)
    all_flags_rows = await database.fetch_all(flags_query)
    
    # Create a mapping from row_index to flags (convert Row to dict)
    flags_map = {row["row_index"]: dict(row) for row in all_flags_rows}
    
    # Efficiently add flag columns using apply
    df['trivium-flag-ok'] = df['_row_index'].apply(lambda x: '1' if flags_map.get(x, {}).get("flag_ok", 0) else '')
    df['trivium-flag-question'] = df['_row_index'].apply(lambda x: '1' if flags_map.get(x, {}).get("flag_question", 0) else '')
    df['trivium-flag-ng'] = df['_row_index'].apply(lambda x: '1' if flags_map.get(x, {}).get("flag_ng", 0) else '')
    df['trivium-memo'] = df['_row_index'].apply(lambda x: flags_map.get(x, {}).get("memo", ""))
    
    # Rename _row_index to trivium-id
    df = df.rename(columns={'_row_index': 'trivium-id'})
    
    # Convert datetime columns to ISO format for CSV
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(lambda x: pd.Timestamp(x).isoformat() if not pd.isna(x) else None)
    
    # Move trivium columns to the beginning
    trivium_cols = ['trivium-id', 'trivium-flag-ok', 'trivium-flag-question', 'trivium-flag-ng', 'trivium-memo']
    other_cols = [col for col in df.columns if col not in trivium_cols]
    df = df[trivium_cols + other_cols]
    
    # Rename hidden columns (add - prefix)
    if hidden_columns and len(hidden_columns) > 0:
        rename_dict = {}
        for col in hidden_columns:
            if col in df.columns and col not in trivium_cols:
                rename_dict[col] = f"-{col}"
        if rename_dict:
            df = df.rename(columns=rename_dict)
    
    # Convert to CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Get project name for filename with timestamp
    project_query = projects_table.select().where(projects_table.c.id == project_id)
    project = await database.fetch_one(project_query)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    original_name = project['name'].replace('.csv', '') if project else f"project_{project_id}"
    filename = f"{timestamp}_{original_name}.csv"
    
    return StreamingResponse(
        iter([csv_buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.put("/api/logs/{log_id}")
async def update_log_flag(log_id: int, flag_update: FlagUpdate):
    """Update flag for a specific row. log_id is the row_index."""
    # We need to know which project this belongs to
    # First, try to find existing flag
    query = flags_table.select().where(flags_table.c.row_index == log_id)
    existing = await database.fetch_one(query)
    
    if existing:
        # Update existing flag
        update_query = flags_table.update().where(
            flags_table.c.row_index == log_id
        ).values(flag=flag_update.flag)
        await database.execute(update_query)
        project_id = existing["project_id"]
    else:
        # This is a problem - we don't know which project this belongs to
        # We need to pass project_id in the request
        raise HTTPException(status_code=400, detail="Cannot update flag without knowing project_id")
    
    return {"status": "success", "log_id": log_id, "new_flag": flag_update.flag}

@app.put("/api/projects/{project_id}/logs/{row_index}")
async def update_log_flag_v2(project_id: int, row_index: int, flag_update: FlagUpdate):
    """Update flag and/or memo for a specific row in a project."""
    # Check if flag exists
    query = flags_table.select().where(
        (flags_table.c.project_id == project_id) & 
        (flags_table.c.row_index == row_index)
    )
    existing = await database.fetch_one(query)
    
    # Convert flag string to bool columns
    flag_ok, flag_question, flag_ng = flag_to_bools(flag_update.flag)
    
    # Prepare update values
    update_values = {
        "flag_ok": flag_ok,
        "flag_question": flag_question,
        "flag_ng": flag_ng
    }
    if flag_update.memo is not None:
        update_values["memo"] = flag_update.memo
    
    if existing:
        # Update existing flag/memo
        update_query = flags_table.update().where(
            (flags_table.c.project_id == project_id) & 
            (flags_table.c.row_index == row_index)
        ).values(**update_values)
        await database.execute(update_query)
        new_memo = flag_update.memo if flag_update.memo is not None else existing["memo"]
    else:
        # Insert new flag/memo
        insert_values = {
            "project_id": project_id,
            "row_index": row_index,
            "flag_ok": flag_ok,
            "flag_question": flag_question,
            "flag_ng": flag_ng,
            "memo": flag_update.memo or ""
        }
        insert_query = flags_table.insert().values(**insert_values)
        await database.execute(insert_query)
        new_memo = flag_update.memo or ""
    
    return {"status": "success", "project_id": project_id, "row_index": row_index, "new_flag": flag_update.flag, "new_memo": new_memo}
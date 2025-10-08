import React, { useState, useEffect, MouseEvent, ChangeEvent, useCallback, useMemo } from 'react';
import { BrowserRouter as Router, Routes, Route, Link as RouterLink, useParams } from 'react-router-dom';
import {
  AppBar, Toolbar, Typography, Container, Table, TableBody, TableCell, TableContainer,
  TableHead, TableRow, Paper, Select, MenuItem, TextField, FormControl, InputLabel,
  IconButton, createTheme, ThemeProvider, CssBaseline, Button, Menu, Checkbox,
  ListItemIcon, ListItemText, CircularProgress, Snackbar, Alert, Grid, Card, CardContent,
  CardActions, Box, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle,
  Pagination, Tooltip
} from '@mui/material';
import { Link as MuiLink } from '@mui/material';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';
import HighlightOffIcon from '@mui/icons-material/HighlightOff';
import DeleteIcon from '@mui/icons-material/Delete';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import CommentIcon from '@mui/icons-material/Comment';
import DownloadIcon from '@mui/icons-material/Download';

// (Theme definition remains the same)
const catppuccinTheme = createTheme({
  palette: {
    mode: 'dark',
    primary: { main: '#8caaee' }, // Blue
    secondary: { main: '#f4b8e4' }, // Pink
    background: { default: '#303446', paper: '#292c3c' }, // Base, Mantle
    text: { primary: '#c6d0f5', secondary: '#b5bfe2' }, // Text, Subtext1
    success: { main: '#a6d189' }, // Green
    warning: { main: '#e5c890' }, // Yellow
    error: { main: '#e78284' }, // Red
  },
});

// --- Type Definitions ---
interface Log {
  id: number;
  flag: string;
  memo: string;
  data: { [key: string]: any };
}
interface Project {
  id: number;
  name: string;
  description: string | null;
  created_at: string;
  total_records?: number;
  flagged_records?: number;
}

const flagIcons: { [key: string]: React.ReactElement } = {
  '◯': <CheckCircleOutlineIcon color="success" />,
  '?': <HelpOutlineIcon color="warning" />,
  '✗': <HighlightOffIcon color="error" />,
};
const flagOptions = ['◯', '?', '✗'];


// --- Snackbar Hook ---
type SnackbarInfo = { open: boolean, message: string, severity: 'success' | 'error' };
const useSnackbar = () => {
    const [snackbar, setSnackbar] = useState<SnackbarInfo | null>(null);
    const closeSnackbar = useCallback(() => setSnackbar(null), []);
    const openSnackbar = useCallback((message: string, severity: 'success' | 'error' = 'success') => {
        setSnackbar({ open: true, message, severity });
    }, []);
    return { snackbar, openSnackbar, closeSnackbar };
};


// --- ProjectPage Component ---
function ProjectPage() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [description, setDescription] = useState('');
  const [file, setFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState<{ open: boolean, projectId: number | null }>({ open: false, projectId: null });
  const { snackbar, openSnackbar, closeSnackbar } = useSnackbar();

  const fetchProjects = async () => {
    setIsLoading(true);
    try {
      const response = await fetch('http://localhost:8000/api/projects');
      if (!response.ok) throw new Error('Network response was not ok');
      const data = await response.json();
      setProjects(data);
    } catch (error) {
      console.error('Failed to load projects:', error);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchProjects();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  
  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    if (event.target.files) setFile(event.target.files[0]);
  };

  const handleUpload = async () => {
    if (!file) {
      openSnackbar('Please select a file.', 'error');
      return;
    }
    setIsUploading(true);
    const formData = new FormData();
    formData.append('file', file);
    formData.append('description', description);

    try {
      const response = await fetch('http://localhost:8000/api/projects', { method: 'POST', body: formData });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to upload.');
      }
      const data = await response.json();
      
      // If hidden_columns are present, save them to localStorage
      if (data.hidden_columns && data.hidden_columns.length > 0 && data.project_id) {
        const storageKey = `trivium_column_visibility_${data.project_id}`;
        const columnVisibility: Record<string, boolean> = { id: true };
        
        // Set all hidden columns to false, others will be true by default
        data.hidden_columns.forEach((col: string) => {
          columnVisibility[col] = false;
        });
        
        localStorage.setItem(storageKey, JSON.stringify(columnVisibility));
      }
      
      openSnackbar('Project created successfully!');
      setFile(null);
      setDescription('');
      await fetchProjects();
    } catch (error: any) {
      openSnackbar(error.message, 'error');
    } finally {
      setIsUploading(false);
    }
  };

  const handleDelete = async () => {
    if (deleteConfirm.projectId === null) return;
    try {
      const response = await fetch(`http://localhost:8000/api/projects/${deleteConfirm.projectId}`, { method: 'DELETE' });
      if (!response.ok) throw new Error('Failed to delete project.');
      openSnackbar('Project deleted successfully!');
      setProjects(projects.filter(p => p.id !== deleteConfirm.projectId));
    } catch (error: any) {
      openSnackbar(error.message, 'error');
    } finally {
      setDeleteConfirm({ open: false, projectId: null });
    }
  };

  return (
    <Box>
      <AppBar position="static" sx={{ py: 0.5 }}>
        <Toolbar variant="dense" sx={{ minHeight: 48 }}>
          <img src="/logo.svg" alt="Trivium Logo" style={{ height: '32px', marginRight: '16px', filter: 'brightness(0) invert(1)' }} />
          <Typography variant="body1">Projects</Typography>
        </Toolbar>
      </AppBar>
      <Container maxWidth={false} sx={{ mt: 4, mb: 4, px: 3 }}>
        <Typography variant="h4" gutterBottom>Projects</Typography>
      
      <Card sx={{ mb: 4 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>Create New Project</Typography>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} sm={6}>
              <TextField fullWidth label="Description (Optional)" value={description} onChange={(e) => setDescription(e.target.value)} variant="outlined" />
            </Grid>
            <Grid item xs={12} sm={6}>
              <Button fullWidth variant="outlined" component="label" sx={{ height: '56px' }}>
                {file ? file.name : 'Select CSV File'}
                <input type="file" hidden accept=".csv" onChange={handleFileChange} />
              </Button>
            </Grid>
          </Grid>
        </CardContent>
        <CardActions>
          <Button fullWidth onClick={handleUpload} disabled={!file || isUploading} variant="contained" color="primary">
            {isUploading ? <CircularProgress size={24} /> : 'Upload and Create'}
          </Button>
        </CardActions>
      </Card>
      
      <Typography variant="h5" gutterBottom>Existing Projects</Typography>
      {isLoading ? <CircularProgress /> : (
        projects.length === 0 ? (
          <Typography sx={{ mt: 2, color: 'text.secondary' }}>
            No existing projects. Upload a new CSV file to get started.
          </Typography>
        ) : (
          <Grid container spacing={2}>
            {projects.map(project => (
              <Grid item xs={12} sm={6} md={4} key={project.id}>
                <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column', position: 'relative' }}>
                  <IconButton 
                    onClick={() => setDeleteConfirm({ open: true, projectId: project.id })} 
                    size="small" 
                    sx={{ 
                      position: 'absolute', 
                      top: 8, 
                      right: 8,
                      zIndex: 1
                    }}
                  >
                    <DeleteIcon />
                  </IconButton>
                  <CardContent sx={{ flexGrow: 1, pr: 5 }}>
                    <MuiLink
                      component={RouterLink}
                      to={`/projects/${project.id}`}
                      sx={{
                        textDecoration: 'none',
                        color: 'primary.main',
                        '&:hover': {
                          textDecoration: 'underline',
                        }
                      }}
                    >
                      <Typography variant="h6">{project.name}</Typography>
                    </MuiLink>
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                      {project.description || 'No description'}
                    </Typography>
                    <Box sx={{ mt: 2, display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                      <Typography variant="body2" color="text.secondary">
                        Total: <strong>{project.total_records || 0}</strong> records
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Flagged: <strong>{project.flagged_records || 0}</strong>
                      </Typography>
                    </Box>
                    <Typography variant="caption" display="block" sx={{ mt: 1 }}>
                      Created: {new Date(project.created_at).toLocaleString()}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        )
      )}

      <Dialog open={deleteConfirm.open} onClose={() => setDeleteConfirm({ open: false, projectId: null })}>
        <DialogTitle>Delete Project?</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to delete this project and all its logs? This action cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteConfirm({ open: false, projectId: null })}>Cancel</Button>
          <Button onClick={handleDelete} color="error">Delete</Button>
        </DialogActions>
      </Dialog>
      <Snackbar open={snackbar?.open} autoHideDuration={6000} onClose={closeSnackbar} anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert onClose={closeSnackbar} severity={snackbar?.severity} sx={{ width: '100%' }}>{snackbar?.message}</Alert>
      </Snackbar>
      </Container>
    </Box>
  );
}


// --- SearchField Component ---
const SearchField = React.memo(({ onSearchChange }: { onSearchChange: (value: string) => void }) => {
  const [localSearch, setLocalSearch] = useState('');
  const [isSearching, setIsSearching] = useState(false);

  useEffect(() => {
    setIsSearching(true);
    const timer = setTimeout(() => {
      onSearchChange(localSearch);
      setIsSearching(false);
    }, 300);

    return () => clearTimeout(timer);
  }, [localSearch, onSearchChange]);

  return (
    <TextField 
      label="Search in message" 
      variant="outlined" 
      value={localSearch} 
      onChange={(e) => setLocalSearch(e.target.value)} 
      sx={{ flexGrow: 1 }}
      InputProps={{
        endAdornment: isSearching ? <CircularProgress size={20} /> : null
      }}
    />
  );
});

// --- MemoDialog Component ---
const MemoDialog = React.memo(({ 
  open, 
  initialMemo, 
  onClose, 
  onSave 
}: { 
  open: boolean; 
  initialMemo: string; 
  onClose: () => void; 
  onSave: (memo: string) => void;
}) => {
  const [memoText, setMemoText] = useState(initialMemo);

  useEffect(() => {
    if (open) {
      setMemoText(initialMemo);
    }
  }, [open, initialMemo]);

  const handleSave = () => {
    onSave(memoText);
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>Edit Memo</DialogTitle>
      <DialogContent>
        <TextField
          autoFocus
          margin="dense"
          label="Memo"
          type="text"
          fullWidth
          multiline
          rows={4}
          value={memoText}
          onChange={(e) => setMemoText(e.target.value)}
          variant="outlined"
        />
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleSave} variant="contained">Save</Button>
      </DialogActions>
    </Dialog>
  );
});

// --- ColumnMenuItem Component ---
const ColumnMenuItem = React.memo(({
  column,
  checked,
  onChange
}: {
  column: string;
  checked: boolean;
  onChange: (column: string) => void;
}) => {
  const handleClick = useCallback(() => {
    onChange(column);
  }, [column, onChange]);

  return (
    <MenuItem onClick={handleClick}>
      <ListItemIcon><Checkbox checked={checked} size="small" /></ListItemIcon>
      <ListItemText>{column}</ListItemText>
    </MenuItem>
  );
});

// --- LogTableRow Component ---
const LogTableRow = React.memo(({
  log,
  visibleColumns,
  isIdVisible,
  onFlagChange,
  onMemoClick,
  flagOptions,
  flagIcons
}: {
  log: Log;
  visibleColumns: string[];
  isIdVisible: boolean;
  onFlagChange: (id: number, flag: string) => void;
  onMemoClick: (id: number, memo: string) => void;
  flagOptions: string[];
  flagIcons: Record<string, React.ReactElement>;
}) => {
  return (
    <TableRow>
      <TableCell sx={{ 
        whiteSpace: 'nowrap', 
        padding: '8px',
        width: '160px',
        minWidth: '160px',
        maxWidth: '160px',
        position: 'sticky',
        left: 0,
        backgroundColor: 'background.paper',
        zIndex: 1,
        borderRight: '1px solid rgba(81, 81, 81, 1)'
      }}>
        <Box sx={{ display: 'flex', gap: '2px', alignItems: 'center' }}>
          {flagOptions.map(option => (
            <IconButton
              key={option}
              onClick={() => onFlagChange(log.id, option)}
              size="small"
              sx={{
                padding: '2px',
                backgroundColor: log.flag === option ? 'rgba(0, 0, 0, 0.08)' : 'transparent',
                opacity: log.flag === '' ? 0.5 : (log.flag === option ? 1 : 0.3),
                transition: 'all 0.1s ease-in-out',
                '&:active': {
                  transform: 'scale(0.95)',
                },
              }}
            >
              {flagIcons[option]}
            </IconButton>
          ))}
          <Tooltip title={log.memo || 'Add memo'} arrow>
            <IconButton
              size="small"
              onClick={() => onMemoClick(log.id, log.memo)}
              sx={{
                padding: '2px',
                marginLeft: '4px',
                opacity: log.memo ? 1 : 0.3,
                color: log.memo ? 'primary.main' : 'inherit',
              }}
            >
              <CommentIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
      </TableCell>
      {isIdVisible && (
        <TableCell sx={{
          width: '80px',
          minWidth: '80px',
          position: 'sticky',
          left: '160px',
          backgroundColor: 'background.paper',
          zIndex: 1,
          borderRight: '1px solid rgba(81, 81, 81, 1)'
        }}>
          {log.id}
        </TableCell>
      )}
      {visibleColumns.map((col, index) => (
        <TableCell 
          key={col}
          sx={{
            position: index === 0 ? 'sticky' : undefined,
            left: index === 0 ? (isIdVisible ? '240px' : '160px') : undefined,
            backgroundColor: index === 0 ? 'background.paper' : undefined,
            zIndex: index === 0 ? 1 : undefined,
            borderRight: index === 0 ? '1px solid rgba(81, 81, 81, 1)' : undefined,
            minWidth: index === 0 ? '200px' : undefined
          }}
        >
          {log.data[col]}
        </TableCell>
      ))}
    </TableRow>
  );
});

// --- LogTable Component ---
const LogTable = React.memo(({
  logs,
  visibleColumns,
  columnVisibility,
  columnTypes,
  sortColumn,
  sortDirection,
  onSort,
  onFlagChange,
  onMemoClick,
  flagOptions,
  flagIcons
}: {
  logs: Log[];
  visibleColumns: string[];
  columnVisibility: Record<string, boolean>;
  columnTypes: Record<string, string>;
  sortColumn: string | null;
  sortDirection: 'asc' | 'desc';
  onSort: (column: string) => void;
  onFlagChange: (id: number, flag: string) => void;
  onMemoClick: (id: number, memo: string) => void;
  flagOptions: string[];
  flagIcons: Record<string, React.ReactElement>;
}) => {
  return (
    <Table stickyHeader>
      <TableHead>
        <TableRow>
          <TableCell sx={{ 
            width: '160px',
            minWidth: '160px',
            maxWidth: '160px',
            position: 'sticky', 
            top: 0, 
            left: 0,
            backgroundColor: 'background.paper', 
            zIndex: 101,
            borderRight: '1px solid rgba(81, 81, 81, 1)'
          }}>
            <Tooltip title="Log flags for marking records" arrow>
              <span>Flag</span>
            </Tooltip>
          </TableCell>
          {columnVisibility.id && (
            <TableCell 
              sx={{ 
                cursor: 'pointer', 
                userSelect: 'none',
                width: '80px',
                minWidth: '80px',
                position: 'sticky', 
                top: 0,
                left: '160px',
                backgroundColor: 'background.paper', 
                zIndex: 101,
                borderRight: '1px solid rgba(81, 81, 81, 1)'
              }} 
              onClick={() => onSort('id')}
            >
              <Tooltip title="Type: int" arrow>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                  ID
                  {sortColumn === 'id' && (
                    sortDirection === 'asc' ? <ArrowUpwardIcon fontSize="small" /> : <ArrowDownwardIcon fontSize="small" />
                  )}
                </Box>
              </Tooltip>
            </TableCell>
          )}
          {visibleColumns.map((col, index) => (
            <TableCell 
              key={col}
              sx={{ 
                cursor: 'pointer', 
                userSelect: 'none', 
                position: 'sticky', 
                top: 0, 
                left: index === 0 ? (columnVisibility.id ? '240px' : '160px') : undefined,
                backgroundColor: 'background.paper', 
                zIndex: index === 0 ? 101 : 100,
                borderRight: index === 0 ? '1px solid rgba(81, 81, 81, 1)' : undefined,
                minWidth: index === 0 ? '200px' : undefined
              }} 
              onClick={() => onSort(col)}
            >
              <Tooltip title={`Type: ${columnTypes[col] || 'unknown'}`} arrow>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                  {col}
                  {sortColumn === col && (
                    sortDirection === 'asc' ? <ArrowUpwardIcon fontSize="small" /> : <ArrowDownwardIcon fontSize="small" />
                  )}
                </Box>
              </Tooltip>
            </TableCell>
          ))}
        </TableRow>
      </TableHead>
      <TableBody>
        {logs.map(log => (
          <LogTableRow
            key={log.id}
            log={log}
            visibleColumns={visibleColumns}
            isIdVisible={columnVisibility.id}
            onFlagChange={onFlagChange}
            onMemoClick={onMemoClick}
            flagOptions={flagOptions}
            flagIcons={flagIcons}
          />
        ))}
      </TableBody>
    </Table>
  );
});

// --- LogViewerPage Component --- (Refactored from AppContent)
function LogViewerPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const [logs, setLogs] = useState<Log[]>([]);
  const [projectName, setProjectName] = useState<string>('');
  const [filterFlags, setFilterFlags] = useState<string[]>([]);
  const [debouncedSearchKeyword, setDebouncedSearchKeyword] = useState<string>('');
  const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>({ id: true });
  const [columns, setColumns] = useState<string[]>([]);
  const [columnTypes, setColumnTypes] = useState<Record<string, string>>({});
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [page, setPage] = useState<number>(1);
  const [totalLogs, setTotalLogs] = useState<number>(0);
  const [logsPerPage, setLogsPerPage] = useState<number>(100);
  const [sortColumn, setSortColumn] = useState<string | null>('id');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');
  const [memoDialogOpen, setMemoDialogOpen] = useState<boolean>(false);
  const [currentMemoLogId, setCurrentMemoLogId] = useState<number | null>(null);
  const [currentMemoInitial, setCurrentMemoInitial] = useState<string>('');
  const { snackbar, openSnackbar, closeSnackbar } = useSnackbar();

  // Fetch project name
  useEffect(() => {
    const fetchProjectName = async () => {
      try {
        const response = await fetch('http://localhost:8000/api/projects');
        if (!response.ok) throw new Error('Failed to fetch projects');
        const projects = await response.json();
        const project = projects.find((p: Project) => p.id === Number(projectId));
        if (project) setProjectName(project.name);
      } catch (error) {
        console.error('Failed to load project name:', error);
      }
    };
    if (projectId) fetchProjectName();
  }, [projectId]);

  const open = Boolean(anchorEl);
  const [flagMenuAnchorEl, setFlagMenuAnchorEl] = useState<null | HTMLElement>(null);
  const flagMenuOpen = Boolean(flagMenuAnchorEl);
  
  // Memoize visible columns to prevent unnecessary re-renders
  const visibleColumns = useMemo(() => {
    return columns.filter(col => columnVisibility[col]);
  }, [columns, columnVisibility]);
  
  const handleColumnMenuClick = useCallback((event: MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  }, []);
  const handleColumnMenuClose = useCallback(() => setAnchorEl(null), []);
  const handleColumnVisibilityChange = useCallback((column: string) => {
    setColumnVisibility(prev => ({ ...prev, [column]: !prev[column] }));
  }, []);
  
  const handleFlagMenuClick = useCallback((event: MouseEvent<HTMLButtonElement>) => {
    setFlagMenuAnchorEl(event.currentTarget);
  }, []);
  const handleFlagMenuClose = useCallback(() => setFlagMenuAnchorEl(null), []);
  const handleFlagFilterToggle = useCallback((flag: string) => {
    setFilterFlags(prev => 
      prev.includes(flag) ? prev.filter(f => f !== flag) : [...prev, flag]
    );
  }, []);
  
  const handleMemoClick = useCallback((logId: number, currentMemo: string) => {
    setCurrentMemoLogId(logId);
    setCurrentMemoInitial(currentMemo);
    setMemoDialogOpen(true);
  }, []);
  
  const handleMemoClose = useCallback(() => {
    setMemoDialogOpen(false);
    setCurrentMemoLogId(null);
    setCurrentMemoInitial('');
  }, []);
  
  const handleMemoSave = async (memoText: string) => {
    if (currentMemoLogId === null) return;
    
    try {
      const response = await fetch(`http://localhost:8000/api/projects/${projectId}/logs/${currentMemoLogId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ flag: logs.find(l => l.id === currentMemoLogId)?.flag || '', memo: memoText }),
      });
      if (!response.ok) throw new Error('Failed to update memo');
      
      const result = await response.json();
      
      // Update local state
      setLogs(prevLogs => 
        prevLogs.map(log => log.id === currentMemoLogId ? { ...log, memo: result.new_memo } : log)
      );
      
      handleMemoClose();
      openSnackbar('Memo saved successfully.', 'success');
    } catch (error) {
      openSnackbar('Failed to save memo.', 'error');
    }
  };
  
  const handleExportCSV = useCallback(async () => {
    try {
      let url = `http://localhost:8000/api/projects/${projectId}/logs/export?`;
      
      // Add sorting parameters
      if (sortColumn) {
        url += `sort_column=${encodeURIComponent(sortColumn)}&sort_direction=${sortDirection}&`;
      }
      
      // Add search parameter
      if (debouncedSearchKeyword) {
        url += `search=${encodeURIComponent(debouncedSearchKeyword)}&`;
      }
      
      // Add flag filter parameters
      if (filterFlags.length > 0) {
        filterFlags.forEach(flag => {
          url += `flag_filter=${encodeURIComponent(flag)}&`;
        });
      }
      
      // Add hidden columns parameters
      const hiddenCols = Object.entries(columnVisibility)
        .filter(([key, visible]) => !visible && key !== 'id')
        .map(([key]) => key);
      if (hiddenCols.length > 0) {
        hiddenCols.forEach(col => {
          url += `hidden_columns=${encodeURIComponent(col)}&`;
        });
      }
      
      const response = await fetch(url);
      if (!response.ok) throw new Error('Failed to export CSV');
      
      const blob = await response.blob();
      const downloadUrl = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = downloadUrl;
      
      // Extract filename from Content-Disposition header
      const contentDisposition = response.headers.get('content-disposition');
      let filename = 'export.csv';
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?(.+)"?/i);
        if (filenameMatch) {
          filename = filenameMatch[1];
        }
      }
      
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(downloadUrl);
      document.body.removeChild(a);
      
      openSnackbar('CSV exported successfully.', 'success');
    } catch (error) {
      openSnackbar('Failed to export CSV.', 'error');
    }
  }, [projectId, sortColumn, sortDirection, debouncedSearchKeyword, filterFlags, columnVisibility, openSnackbar]);
  
  const handleSearchChange = useCallback((value: string) => {
    setDebouncedSearchKeyword(value);
  }, []);

  const handleSort = useCallback((column: string) => {
    if (sortColumn === column) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('asc');
    }
  }, [sortColumn]);

  const fetchLogs = useCallback(async (pageNum: number) => {
    setIsLoading(true);
    try {
      const offset = (pageNum - 1) * logsPerPage;
      let url = `http://localhost:8000/api/projects/${projectId}/logs?offset=${offset}&limit=${logsPerPage}`;
      
      // Add sorting parameters if a column is selected for sorting
      if (sortColumn) {
        url += `&sort_column=${encodeURIComponent(sortColumn)}&sort_direction=${sortDirection}`;
      }
      
      // Add search parameter
      if (debouncedSearchKeyword) {
        url += `&search=${encodeURIComponent(debouncedSearchKeyword)}`;
      }
      
      // Add flag filter parameters (multiple)
      if (filterFlags.length > 0) {
        filterFlags.forEach(flag => {
          url += `&flag_filter=${encodeURIComponent(flag)}`;
        });
      }
      
      const response = await fetch(url);
      if (!response.ok) throw new Error('Network response was not ok');
      const data = await response.json();
      setLogs(data.logs);
      setTotalLogs(data.total);
      
      // Set column types from backend
      if (data.column_types) {
        setColumnTypes(data.column_types);
      }

      if (data.logs.length > 0 && columns.length === 0) {
        const logColumns = Object.keys(data.logs[0].data);
        setColumns(logColumns);
        
        // Try to restore column visibility from localStorage
        const storageKey = `trivium_column_visibility_${projectId}`;
        const savedVisibility = localStorage.getItem(storageKey);
        
        if (savedVisibility) {
          try {
            const parsed = JSON.parse(savedVisibility);
            // Ensure all current columns exist in the saved visibility
            const restoredVisibility: Record<string, boolean> = { id: parsed.id ?? true };
            for (const col of logColumns) {
              restoredVisibility[col] = parsed[col] ?? true;
            }
            setColumnVisibility(restoredVisibility);
          } catch {
            // If parsing fails, use default visibility
            const initialVisibility: Record<string, boolean> = { id: true };
            for (const col of logColumns) {
              initialVisibility[col] = true;
            }
            setColumnVisibility(initialVisibility);
          }
        } else {
          // Default visibility if no saved state
          const initialVisibility: Record<string, boolean> = { id: true };
          for (const col of logColumns) {
            initialVisibility[col] = true;
          }
          setColumnVisibility(initialVisibility);
        }
      }

    } catch (error) {
      console.error('Failed to load logs:', error);
    } finally {
      setIsLoading(false);
    }
  }, [projectId, logsPerPage, columns.length, sortColumn, sortDirection, debouncedSearchKeyword, filterFlags]);

  useEffect(() => {
    if (projectId) fetchLogs(page);
  }, [projectId, page, fetchLogs]);

  const handleFlagChange = useCallback(async (id: number, newFlag: string) => {
    let originalLogs: Log[] = [];
    let newFlagValue = '';
    
    // Immediate optimistic UI update with functional setState
    setLogs(prevLogs => {
      originalLogs = [...prevLogs];
      const currentLog = prevLogs.find(log => log.id === id);
      if (!currentLog) return prevLogs;
      
      newFlagValue = currentLog.flag === newFlag ? '' : newFlag;
      
      // If filters are active, check if the new flag matches any filter
      if (filterFlags.length > 0) {
        const matchesFilter = (flag: string) => {
          if (filterFlags.includes('No Flag') && flag === '') return true;
          return filterFlags.includes(flag);
        };
        
        if (!matchesFilter(newFlagValue)) {
          // Remove from view if doesn't match any filter
          return prevLogs.filter(log => log.id !== id);
        }
      }
      
      return prevLogs.map(log => log.id === id ? { ...log, flag: newFlagValue } : log);
    });

    try {
      const response = await fetch(`http://localhost:8000/api/projects/${projectId}/logs/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ flag: newFlagValue }),
      });
      if (!response.ok) throw new Error('Failed to update flag');
      
      const result = await response.json();
      
      // Update with server response to ensure consistency
      setLogs(prevLogs => {
        if (filterFlags.length > 0) {
          const matchesFilter = (flag: string) => {
            if (filterFlags.includes('No Flag') && flag === '') return true;
            return filterFlags.includes(flag);
          };
          
          if (!matchesFilter(result.new_flag)) {
            return prevLogs.filter(log => log.id !== id);
          }
        }
        return prevLogs.map(log => log.id === id ? { ...log, flag: result.new_flag } : log);
      });
    } catch (error) {
      openSnackbar('Failed to update flag.', 'error');
      setLogs(originalLogs);
    }
  }, [projectId, filterFlags, openSnackbar]);
  
  // Reset to page 1 when search or filter changes
  useEffect(() => {
    setPage(1);
  }, [debouncedSearchKeyword, filterFlags]);

  // Save column visibility to localStorage (debounced)
  useEffect(() => {
    if (projectId && columns.length > 0) {
      const timer = setTimeout(() => {
        const storageKey = `trivium_column_visibility_${projectId}`;
        localStorage.setItem(storageKey, JSON.stringify(columnVisibility));
      }, 500);
      
      return () => clearTimeout(timer);
    }
  }, [columnVisibility, projectId, columns.length]);
  
  return (
    <Box>
      <AppBar position="static" sx={{ py: 0.5 }}>
        <Toolbar variant="dense" sx={{ minHeight: 48 }}>
          <img src="/logo.svg" alt="Trivium Logo" style={{ height: '32px', marginRight: '16px', filter: 'brightness(0) invert(1)' }} />
          <MuiLink 
            component={RouterLink} 
            to="/" 
            color="inherit" 
            sx={{ 
              textDecoration: 'none',
              '&:hover': {
                textDecoration: 'underline',
                opacity: 0.8
              }
            }}
          >
            <Typography variant="body1">Projects</Typography>
          </MuiLink>
          <Typography variant="body1" sx={{ mx: 1 }}>/</Typography>
          <Typography variant="body1" sx={{ color: 'text.secondary' }}>{projectName || 'Loading...'}</Typography>
        </Toolbar>
      </AppBar>
      <Container maxWidth={false} sx={{ mt: 4, mb: 4, px: 3 }}>
        <Paper sx={{ p: 2, display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
            <Button 
              onClick={handleFlagMenuClick} 
              variant="outlined"
              sx={{ minWidth: 120 }}
            >
              Flag Filter {filterFlags.length > 0 && `(${filterFlags.length})`}
            </Button>
            <Menu 
              anchorEl={flagMenuAnchorEl} 
              open={flagMenuOpen} 
              onClose={handleFlagMenuClose}
              keepMounted
            >
              <MenuItem onClick={() => handleFlagFilterToggle('No Flag')}>
                <ListItemIcon><Checkbox checked={filterFlags.includes('No Flag')} size="small" /></ListItemIcon>
                <ListItemText>No Flag</ListItemText>
              </MenuItem>
              {flagOptions.map(option => (
                <MenuItem key={option} onClick={() => handleFlagFilterToggle(option)}>
                  <ListItemIcon><Checkbox checked={filterFlags.includes(option)} size="small" /></ListItemIcon>
                  <ListItemIcon>{flagIcons[option]}</ListItemIcon>
                </MenuItem>
              ))}
            </Menu>
            <FormControl sx={{ minWidth: 120 }}>
              <InputLabel id="page-size-label">Per Page</InputLabel>
              <Select labelId="page-size-label" label="Per Page" value={logsPerPage} onChange={(e) => { setLogsPerPage(Number(e.target.value)); setPage(1); }}>
                <MenuItem value={100}>100</MenuItem>
                <MenuItem value={500}>500</MenuItem>
                <MenuItem value={1000}>1000</MenuItem>
                <MenuItem value={5000}>5000</MenuItem>
              </Select>
            </FormControl>
            <SearchField onSearchChange={handleSearchChange} />
            <Button id="column-visibility-button" aria-haspopup="true" onClick={handleColumnMenuClick} variant="outlined">
              Columns
            </Button>
            <Menu 
              id="column-visibility-menu" 
              anchorEl={anchorEl} 
              open={open} 
              onClose={handleColumnMenuClose}
              keepMounted
            >
              {Object.keys(columnVisibility).map((column) => (
                <ColumnMenuItem
                  key={column}
                  column={column}
                  checked={columnVisibility[column]}
                  onChange={handleColumnVisibilityChange}
                />
              ))}
            </Menu>
        </Paper>
        {isLoading ? ( <CircularProgress sx={{ display: 'block', margin: '100px auto' }} /> ) : (
          <TableContainer 
            component={Paper} 
            sx={{ 
              mt: 2, 
              maxHeight: 'calc(100vh - 300px)',
              // Custom scrollbar styling
              '&::-webkit-scrollbar': {
                width: '12px',
                height: '12px',
              },
              '&::-webkit-scrollbar-track': {
                background: '#292c3c',
                borderRadius: '10px',
              },
              '&::-webkit-scrollbar-thumb': {
                background: '#626880',
                borderRadius: '10px',
                border: '2px solid #292c3c',
                '&:hover': {
                  background: '#737994',
                },
              },
              '&::-webkit-scrollbar-corner': {
                background: '#292c3c',
              },
              // Firefox scrollbar styling
              scrollbarWidth: 'thin',
              scrollbarColor: '#626880 #292c3c',
            }}
          >
            <LogTable
              logs={logs}
              visibleColumns={visibleColumns}
              columnVisibility={columnVisibility}
              columnTypes={columnTypes}
              sortColumn={sortColumn}
              sortDirection={sortDirection}
              onSort={handleSort}
              onFlagChange={handleFlagChange}
              onMemoClick={handleMemoClick}
              flagOptions={flagOptions}
              flagIcons={flagIcons}
            />
          </TableContainer>
        )}
        {!isLoading && totalLogs > 0 && (
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 3, gap: 2 }}>
            <Typography variant="body2" color="text.secondary">
              Total: {totalLogs} logs | Showing {((page - 1) * logsPerPage) + 1}-{Math.min(page * logsPerPage, totalLogs)}
            </Typography>
            <Pagination 
              count={Math.ceil(totalLogs / logsPerPage)} 
              page={page} 
              onChange={(_, value) => setPage(value)}
              color="primary"
              showFirstButton
              showLastButton
            />
            <Button
              variant="contained"
              startIcon={<DownloadIcon />}
              onClick={handleExportCSV}
              size="small"
            >
              Export
            </Button>
          </Box>
        )}
        <Snackbar open={snackbar?.open} autoHideDuration={6000} onClose={closeSnackbar} anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
          <Alert onClose={closeSnackbar} severity={snackbar?.severity} sx={{ width: '100%' }}>{snackbar?.message}</Alert>
        </Snackbar>
        <MemoDialog
          open={memoDialogOpen}
          initialMemo={currentMemoInitial}
          onClose={handleMemoClose}
          onSave={handleMemoSave}
        />
      </Container>
    </Box>
  );
}

// --- Main App Component with Router ---
function App() {
  return (
    <ThemeProvider theme={catppuccinTheme}>
      <CssBaseline />
      <Router>
        <Routes>
          <Route path="/" element={<ProjectPage />} />
          <Route path="/projects/:projectId" element={<LogViewerPage />} />
        </Routes>
      </Router>
    </ThemeProvider>
  );
}

export default App;

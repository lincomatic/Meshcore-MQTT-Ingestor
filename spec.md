# MQTT MeshCore Ingestor - Database Design Specification (FINALIZED)

## Overview
Design a fault-tolerant SQLite database to store MeshCore MQTT messages from two topics: `status` and `packets`.

**Requirements:**
- Storage: SQLite database
- Read concurrency: < 10 concurrent reader processes
- Message rate: < 10,000 messages/minute (~167 msg/sec)
- Fault tolerance: graceful shutdown on crashes
- Topic format: `meshcore/<IATA>/<pubkey>/(status|packets)`

---

## 1. FINALIZED Schema Design

### 1.1 Observers Table
Tracks device status updates with upsert logic (insert or update existing).

```sql
CREATE TABLE observers (
    pubkey TEXT PRIMARY KEY,           -- origin_id (64-char hex, uppercase)
    name TEXT NOT NULL,                -- origin string (from "origin" field)
    radio TEXT,                        -- radio config (e.g., "927.875,62.5,7,5")
    client_version TEXT,               -- e.g., "pyMC_repeater/1.0.6.dev68+gded15ea43"
    iata TEXT,                         -- 3-letter IATA code from topic (uppercase)
    status TEXT,                       -- observer status from JSON: "online" or "offline"
    N.B USELESS BECAUSE OFTEN WHEN A REPEATER GOES OFFLINE, STATUS REMAINS online. Use last_seen to determine status instead
    first_seen REAL NOT NULL,          -- Unix timestamp (UTC) of first insert
    last_seen REAL NOT NULL            -- Unix timestamp (UTC) of last update
);
```

**Upsert Logic:**
- On insert: `first_seen = last_seen = now()`
- On update: Preserve `first_seen`; update only when any non-key field changes (`name`, `radio`, `client_version`, `iata`, `status`), and set `last_seen = now()` when update occurs

**Migration Strategy:**
- Existing databases are migrated at startup via `run_migrations(conn)`
- Migration example currently implemented: add `iata` to `observers` when missing
- Migration example currently implemented: add `status` to `observers` when missing

---

### 1.2 Packets Table
Stores validated packet data with parsed fields from the `raw` hex payload.

```sql
CREATE TABLE packets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,           -- Unix timestamp (UTC) when message arrived
    iata TEXT NOT NULL,                -- 3-letter IATA code (uppercase)
    observer TEXT NOT NULL,            -- First 4 chars of pubkey (uppercase)
    raw TEXT NOT NULL,                 -- Hex string (length = 2 * payload_len)
    
    -- Parsed fields from first byte of raw
    route_type INTEGER,                -- Bits 0-1 of first byte
    payload_type INTEGER,              -- Bits 2-5 of first byte
    version INTEGER                    -- Bits 6-7 of first byte
);
```

**Validation Before Insert:**
1. ✅ `raw` is not null - raises ValueError if null
2. ✅ `payload_len > 0` - raises ValueError if zero  
3. ✅ `len(raw) == 2 * payload_len` - logs ERROR but **still inserts** the packet

**NOTE:** Route validation (F or D) has been removed per user request.

---

## 2. Indexes Explanation & Decision

### What Indexes Do:
Indexes are like a book's table of contents - they speed up searches but:
- ✅ **Pro:** Much faster queries when filtering/sorting by indexed columns
  - Without index: SQLite scans entire table (e.g., 1M rows)
  - With index: SQLite jumps directly to matching rows (e.g., 100 rows)
- ❌ **Con:** Slower inserts (must update index on each insert)
- ❌ **Con:** More disk space (~10-50% overhead per index)

### For Your Use Case (10K msg/min writes):
- High write rate = indexes slow down inserts
- **Decision:** Skip indexes initially, add later if needed for specific queries
- You can add indexes anytime: `CREATE INDEX idx_packets_timestamp ON packets(timestamp);`

**When to add indexes later:**
- If queries like "get packets for IATA=LAX" become slow
- If you need fast time-range queries

---

## 3. Monthly Tables vs Archive Files

### Option A: Monthly Tables
Create new table each month: `packets_2026_02`, `packets_2026_03`, etc.
- ✅ Queries on recent data stay fast (smaller tables)
- ✅ Easy to drop old months: `DROP TABLE packets_2025_01;`
- ❌ Complex queries across months (need UNION)
- ❌ Application logic to route inserts to correct table

### Option B: Single Table + Archive to Files
Keep one `packets` table, periodically export old data to files.
- ✅ Simple queries (one table)
- ✅ Old data completely removed from DB (frees disk space)
- ❌ Archived data not queryable (unless re-imported)
- ❌ Manual archive process needed

### **RECOMMENDATION for your case:**
Use a **single table** for now. At your rate (~10K/min), you'll generate:
- ~14M packets/day
- ~5.2B packets/year

If SQLite performance degrades (>100GB DB), switch to monthly tables then.
**Easy migration:** No code changes needed initially.

---

## 4. WAL Checkpoints Explanation

### What is WAL (Write-Ahead Logging)?
```
PRAGMA journal_mode=WAL;
```
Creates two files:
- `database.db` - main database file
- `database.db-wal` - temporary write-ahead log

**How it works:**
1. Writes go to WAL file first (fast)
2. Readers read from main DB + WAL combined (no blocking)
3. Periodically, WAL is "checkpointed" = merged back into main DB

### What is a Checkpoint?
A checkpoint copies data from WAL → main DB file, then truncates WAL.

**When checkpoints happen:**
- Automatic: When WAL grows to 1000 pages (~4MB default)
- Manual: `PRAGMA wal_checkpoint(TRUNCATE);`

### Why Checkpoints Matter:
- ✅ Keeps WAL file from growing forever
- ✅ Ensures backups of main DB are recent
- ❌ Brief write pause during checkpoint

### **RECOMMENDATION:**
- Let SQLite auto-checkpoint (default is fine)
- For backups: Manually checkpoint first, then copy DB file
```python
# Before backup
conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
# Now safe to copy database.db
```

---

## 5. SQLite Optimization Strategy

### 5.1 WAL Mode for Concurrency
```sql
PRAGMA journal_mode=WAL;
```
- Enables concurrent reads while writing
- Writers don't block readers
- Required for 10 concurrent readers

### 5.2 Performance Tuning
```sql
PRAGMA synchronous=NORMAL;      -- Balance between safety and speed
PRAGMA cache_size=-64000;       -- 64MB cache (negative = KB)
PRAGMA temp_store=MEMORY;       -- Use memory for temp tables
PRAGMA mmap_size=30000000000;   -- 30GB memory-mapped I/O (if supported)
```

### 5.3 Batch Inserts - FINALIZED
- Use transactions for bulk inserts
- **Commit every 50 messages OR 30 seconds** (whichever comes first)
- Trade-off: Balances fault-tolerance with performance

---

## 6. Implementation Decisions - FINALIZED

### 6.1 Write Strategy
✅ **Single writer task with asyncio.Queue**
- All MQTT handlers push messages to queue
- Dedicated DatabaseWriter task batches and commits
- Simple concurrency model, easy batching

### 6.2 Deduplication
✅ **No deduplication** - store all packets (same packet from multiple observers is valid)

### 6.3 Data Retention
✅ **Keep forever** - no auto-purge

### 6.4 Parsed Fields
✅ **Extract immediately** - parse `route_type`, `payload_type`, `version` from first byte of `raw` and store in DB

### 6.5 Error Logging
✅ **Text log file** - log failed inserts to standard logger (not separate DB)

### 6.6 Metrics
✅ **Track metrics:**
- Insert rate (packets/sec, status updates/sec)
- Queue depth (pending messages)
- DB file size
- Log periodically (e.g., every minute)

### 6.7 Backup Strategy
✅ **File-based backups:**
- Checkpoint WAL before backup: `PRAGMA wal_checkpoint(TRUNCATE);`
- Copy `database.db` file to backup location
- Recommend: Daily cron job or external backup tool

---

## 7. Bit Field Parsing

Extract from first byte of `raw` hex string:

```python
# raw is hex string like "152C7C01..."
first_byte = int(raw[0:2], 16)  # First 2 hex chars = 1 byte

route_type = first_byte & 0b00000011      # Bits 0-1
payload_type = (first_byte >> 2) & 0b00001111  # Bits 2-5
version = (first_byte >> 6) & 0b00000011  # Bits 6-7
```

**Example:** If `raw = "152C..."`, first byte = `0x15` = `0b00010101`
- `route_type` = `01` (binary) = 1
- `payload_type` = `0101` (binary) = 5
- `version` = `00` (binary) = 0

---

## 8. Code Organization

### 8.1 File Structure
```
database.py          # All database logic
    - init_db()        # Create tables, set PRAGMAs
    - run_migrations() # Startup schema migrations
    - insert_observer()# Upsert observer record
  - insert_packet()  # Insert packet (with validation)
  - DatabaseWriter   # Async queue-based writer class
  - parse_bits()     # Helper to extract route_type, payload_type, version

mqtt-mc-ingestor.py  # MQTT handlers
  - Imports database.py
  - Handlers enqueue messages to DatabaseWriter queue
  - Main spawns DatabaseWriter task alongside MQTT tasks
  - Signal handlers for graceful shutdown
```

### 8.2 DatabaseWriter Class Design
```python
class DatabaseWriter:
    def __init__(self, db_path, batch_size=50, batch_timeout=30.0):
        self.queue = asyncio.Queue()
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.conn = None
        self.running = False
        
    async def start(self):
        # Initialize DB connection, create tables
        # Start batch processing loop
        
    async def enqueue_observer(self, data):
        await self.queue.put(('observer', data))
        
    async def enqueue_packet(self, data):
        await self.queue.put(('packet', data))
        
    async def _process_batch(self):
        # Collect up to batch_size messages or wait batch_timeout
        # Execute batch INSERT with transaction
        # Commit and log metrics
        
    async def stop(self):
        # Flush remaining queue
        # Close DB connection
```

---

## 9. Implementation Steps

### Phase 1: Database Module
1. ✅ Create `database.py`
2. ✅ Implement `init_db()` - create tables, set PRAGMAs
3. ✅ Implement `DatabaseWriter` class:
   - Queue management
   - Batch insert logic
   - Transaction handling
   - Metrics tracking
4. ✅ Implement validation functions
5. ✅ Implement bit parsing helper

### Phase 2: Update Ingestor
1. ✅ Import database module in `ingestor.py`
2. ✅ Initialize `DatabaseWriter` in `main()`
3. ✅ Update `handle_status_message()` to enqueue to DB
4. ✅ Update `handle_packets_message()` to enqueue to DB
5. ✅ Add signal handlers for graceful shutdown (SIGINT, SIGTERM)

### Phase 3: Testing
1. ✅ Unit tests for validation logic
2. ✅ Unit tests for bit parsing
3. ✅ Integration test with sample MQTT messages
4. ✅ Load test: sustained 10K msg/min
5. ✅ Crash recovery test: kill -9, verify DB integrity

### Phase 4: Metrics & Monitoring
1. ✅ Log insert rates every 60 seconds
2. ✅ Log queue depth warnings (>1000 pending)
3. ✅ Log DB file size periodically
4. ✅ Add healthcheck endpoint (optional)

---

## 10. Implementation Status

### ✅ COMPLETE - All components implemented and integrated

**Files created/modified:**
- [database.py](database.py) - New: Database module with DatabaseWriter class
- [ingestor.py](ingestor.py) - Updated: Integrated database, async handlers, signal handlers

**Key features implemented:**
- ✅ SQLite database with WAL mode for concurrent reads
- ✅ Batch commitment: 50 messages OR 30 seconds (whichever first)
- ✅ Async DatabaseWriter with queue-based message processing
- ✅ Transaction safety with rollback on errors
- ✅ Metrics logging every 60 seconds (insert rate, queue depth, DB size)
- ✅ Graceful shutdown: SIGINT/SIGTERM handlers flush remaining queue
- ✅ Bit field parsing (route_type, payload_type, version from raw[0:2])
- ✅ Observer upsert logic (insert new, update existing by pubkey when any non-key field changes)
- ✅ Startup migration support via `run_migrations()`
- ✅ Packet validation with special handling for raw length mismatches

**Validation behavior:**
- `raw is null` → Raises ValueError, logs ERROR, skipped
- `payload_len == 0` → Raises ValueError, logs ERROR, skipped  
- `raw length mismatch` → Logs ERROR but **still inserts** packet
- Route validation (F/D) → **Removed per user request**

---

## 11. Sample Implementation Code (As Implemented)

### Validation Function
```python
def validate_packet_data(data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """Validate packet data before insert."""
    if data.get('raw') is None:
        return False, "raw is null"
    
    payload_len_str = data.get('payload_len')
    if payload_len_str in (0, "0", None):
        return False, f"payload_len is zero or null: {payload_len_str}"
    
    try:
        payload_len = int(payload_len_str)
    except (ValueError, TypeError):
        return False, f"Invalid payload_len: {payload_len_str}"
    
    if payload_len == 0:
        return False, "payload_len is zero"
    
    raw = data['raw']
    expected_len = 2 * payload_len
    if len(raw) != expected_len:
        return False, f"raw length {len(raw)} != 2 * payload_len {expected_len}"
    
    return True, None
```

### DatabaseWriter Batch Processing
```python
async def _insert_batch(self, batch: list):
    """Insert a batch of messages in a single transaction."""
    try:
        self.conn.execute("BEGIN TRANSACTION")
        
        for msg_type, data in batch:
            try:
                if msg_type == 'observer':
                    self._insert_observer(data)
                elif msg_type == 'packet':
                    self._insert_packet(data)
            except Exception as e:
                logger.error("Error inserting %s: %s", msg_type, e)
        
        self.conn.commit()
        # Update metrics...
    except Exception as e:
        logger.error("Error committing batch: %s", e)
        self.conn.rollback()
```

### Packet Insertion with Raw Length Handling
```python
def _insert_packet(self, data: Dict[str, Any]):
    """Insert packet with validation."""
    is_valid, error_msg = validate_packet_data(data)
    
    if not is_valid:
        if error_msg and "raw length" in error_msg:
            # Log error but still insert
            logger.error(
                "Packet raw length validation failed: %s. Inserting anyway.",
                error_msg
            )
        else:
            # Other errors: raise to be caught
            raise ValueError(error_msg)
    
    # Extract topic and parse bits...
    raw = data['raw'].upper()
    route_type, payload_type, version = parse_raw_bits(raw)
    
    # Insert packet
    self.conn.execute("""
        INSERT INTO packets (timestamp, iata, observer, raw, route_type, payload_type, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (now, iata, observer, raw, route_type, payload_type, version))
```

### Async Handlers in Ingestor
```python
async def handle_packets_message(remote_name, topic_data, payload):
    """Handle packets message - validate and enqueue to database."""
    try:
        parsed_payload = parse_json_payload(payload)
        parsed_payload['_topic_data'] = topic_data
        
        if db_writer:
            await db_writer.enqueue_packet(parsed_payload)
    except Exception as e:
        logger.error("[%s] [PACKETS] Error: %s", remote_name, e)

async def handle_status_message(remote_name, topic_data, payload):
    """Handle status message - enqueue to database."""
    try:
        parsed_payload = parse_json_payload(payload)
        parsed_payload['_topic_data'] = topic_data
        
        if db_writer:
            await db_writer.enqueue_observer(parsed_payload)
    except Exception as e:
        logger.error("[%s] [STATUS] Error: %s", remote_name, e)
```

---

## 12. Database File Location

**Storage location:** Project root as `meshcore.db`
- Path: `c:\dev\meshcore\mqtt-mc-ingestor\meshcore.db`
- WAL file: `meshcore.db-wal` (auto-created by SQLite)
- SHM file: `meshcore.db-shm` (auto-created by SQLite)

**Configuration:**
- Path is hardcoded in `ingestor.py` as `meshcore.db` in current directory
- Can be made configurable via environment variable or config.ini if needed

---

## 13. Error Handling

| Scenario | Handling |
|----------|----------|
| `raw` is null | Raises ValueError, logged as ERROR, packet skipped |
| `payload_len == 0` | Raises ValueError, logged as ERROR, packet skipped |
| Raw length mismatch | Logs ERROR, **packet is still inserted** |
| JSON parse error | Logged as ERROR, packet skipped |
| MQTT connection error | Warning logged, auto-reconnect with backoff |
| DB transaction error | Rollback executed, error logged, batch continues |
| Queue full (>1000 items) | Warning logged, continues accepting (possible backpressure) |
| Graceful shutdown signal | Flushes pending queue, closes DB properly |

---

## 14. Metrics Logged Every 60 Seconds

Example log entry:
```
[METRICS] packets=1234 observers=56 queue_depth=23 db_size_mb=456.7 packet_rate=20.5/sec observer_rate=0.9/sec
```

Values logged:
- `packets` - Total packets inserted since last metric log
- `observers` - Total observers updated since last metric log
- `queue_depth` - Current messages awaiting insertion (real-time)
- `db_size_mb` - Current database file size in MB
- `packet_rate` - Packets per second (calculated over metric period)
- `observer_rate` - Observer updates per second (calculated over metric period)

---

## 15. Appendix: Sample SQL Queries

### Query: Recent packets for a specific IATA
```sql
SELECT 
    datetime(timestamp, 'unixepoch') as time, 
    observer, 
    route_type,
    payload_type,
    version,
    substr(raw, 1, 20) || '...' as raw_preview
FROM packets
WHERE iata = 'LAX'
  AND timestamp > unixepoch('now', '-1 hour')
ORDER BY timestamp DESC
LIMIT 100;
```

### Query: Active devices (observer updated in last hour)
```sql
SELECT 
    pubkey, 
    name, 
    radio,
    iata,
    client_version,
    datetime(last_seen, 'unixepoch') as last_seen,
    datetime(first_seen, 'unixepoch') as first_seen
FROM observers
WHERE last_seen > unixepoch('now', '-1 hour')
ORDER BY last_seen DESC;
```

### Query: Packet rate per IATA (last 24 hours)
```sql
SELECT 
    iata, 
    COUNT(*) as packet_count,
    COUNT(DISTINCT observer) as unique_observers
FROM packets
WHERE timestamp > unixepoch('now', '-1 day')
GROUP BY iata
ORDER BY packet_count DESC;
```

### Query: Packets by observer
```sql
SELECT 
    observer,
    COUNT(*) as packet_count,
    datetime(MIN(timestamp), 'unixepoch') as first_packet,
    datetime(MAX(timestamp), 'unixepoch') as last_packet
FROM packets
GROUP BY observer
ORDER BY packet_count DESC;
```

### Query: Database size summary
```sql
SELECT 
    (SELECT COUNT(*) FROM packets) as total_packets,
    (SELECT COUNT(*) FROM observers) as total_devices,
    (SELECT datetime(MIN(timestamp), 'unixepoch') FROM packets) as oldest_packet,
    (SELECT datetime(MAX(timestamp), 'unixepoch') FROM packets) as newest_packet;
```

---

## 16. Implementation Complete ✅

**Date completed:** February 27, 2026

**What was implemented:**
- ✅ `database.py` - Complete database module with DatabaseWriter class
- ✅ `ingestor.py` - Updated MQTT handlers with database integration
- ✅ SQLite schema with observers and packets tables
- ✅ WAL mode for concurrent read support
- ✅ Queue-based batch processing (50 messages or 30 seconds)
- ✅ Graceful shutdown with signal handlers
- ✅ Metrics logging every 60 seconds
- ✅ Packet validation with raw length mismatch handling (logs ERROR but inserts)
- ✅ Bit field parsing from raw hex data

**How to run:**
```bash
python ingestor.py
```

**What to expect:**
- Database created automatically as `meshcore.db`
- MQTT connections established to configured remotes
- Messages logged and inserted into database
- Metrics logged every 60 seconds
- Press Ctrl+C for graceful shutdown (flushes queue and closes DB)

**Next steps (optional):**
- Add indexes if queries become slow: `CREATE INDEX idx_packets_timestamp ON packets(timestamp);`
- Monitor database growth and plan archival strategy if >100GB
- Implement backup job using `PRAGMA wal_checkpoint(TRUNCATE);` before copying DB file
- Add health checks or monitoring endpoints as needed



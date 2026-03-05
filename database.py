"""
Database module for MeshCore MQTT ingestor.
Handles SQLite storage with batched writes and fault tolerance.
"""

import asyncio
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# Database Initialization
# ============================================================================

def run_migrations(conn: sqlite3.Connection):
    """
    Run database schema migrations.
    
    Args:
        conn: SQLite database connection
    """
    logger.info("Running database migrations...")
    
    # Inspect current observers columns once
    try:
        cursor = conn.execute("PRAGMA table_info(observers)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Migration 1: Add iata column to observers table
        if 'iata' not in columns:
            logger.info("Adding iata column to observers table")
            conn.execute("ALTER TABLE observers ADD COLUMN iata TEXT")
            conn.commit()
            logger.info("Migration complete: iata column added")
        else:
            logger.debug("iata column already exists in observers table")

        # Migration 2: Add status column to observers table
        if 'status' not in columns:
            logger.info("Adding status column to observers table")
            conn.execute("ALTER TABLE observers ADD COLUMN status TEXT")
            conn.commit()
            logger.info("Migration complete: status column added")
        else:
            logger.debug("status column already exists in observers table")
    except Exception as e:
        logger.error("Error in migration: %s", e, exc_info=True)
        raise
    
    logger.info("All migrations complete")


def init_db(db_path: str) -> sqlite3.Connection:
    """
    Initialize database, create tables if needed, and set PRAGMAs.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        sqlite3.Connection object
    """
    logger.info("Initializing database at: %s", db_path)
    
    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Connect with check_same_thread=False for async usage
    conn = sqlite3.connect(db_path, check_same_thread=False)
    
    # Set PRAGMAs for performance and concurrency
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=-64000;")  # 64MB cache
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA mmap_size=30000000000;")  # 30GB if supported
    
    logger.info("Database PRAGMAs set: WAL mode, NORMAL sync, 64MB cache")
    
    # Create observers table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS observers (
            pubkey TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            radio TEXT,
            client_version TEXT,
            iata TEXT,
            status TEXT,
            first_seen REAL NOT NULL,
            last_seen REAL NOT NULL
        )
    """)
    
    # Create packets table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS packets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            iata TEXT NOT NULL,
            observer TEXT NOT NULL,
            raw TEXT NOT NULL,
            route_type INTEGER,
            payload_type INTEGER,
            version INTEGER
        )
    """)
    
    # Create iata table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS iatas (
            iata TEXT PRIMARY KEY,
            added_at REAL NOT NULL
        )
    """)
    
    conn.commit()
    logger.info("Database tables created/verified")
    
    # Run migrations for existing databases
    run_migrations(conn)
    
    return conn


# ============================================================================
# Validation Functions
# ============================================================================

def validate_packet_data(data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate packet data before insert.
    
    Args:
        data: Packet data dictionary from JSON
        
    Returns:
        Tuple of (is_valid, error_message) where error_message is None if valid
    """
    # Check raw is not null
    if data.get('raw') is None:
        return False, "raw is null"
    
#    # Check payload_len is not zero
#    payload_len_str = data.get('payload_len')
#    if payload_len_str in (0, "0", None):
#        return False, f"payload_len is zero or null: {payload_len_str}"
#    
#    try:
#        payload_len = int(payload_len_str)
#    except (ValueError, TypeError):
#        return False, f"Invalid payload_len: {payload_len_str}"
#    
#    if payload_len == 0:
#        return False, "payload_len is zero"
#    
    # Check raw length matches payload_len
#    raw = data['raw']
#    expected_len = 2 * payload_len
#    if len(raw) != expected_len:
#        return False, f"raw length {len(raw)} != 2 * payload_len {expected_len}"
    
    return True, None
# Bit Parsing Helper
# ============================================================================

def parse_raw_bits(raw_hex: str) -> tuple[int, int, int]:
    """
    Parse first byte of raw hex string to extract bit fields.
    
    Args:
        raw_hex: Hex string (e.g., "152C7C01...")
        
    Returns:
        Tuple of (route_type, payload_type, version)
        - route_type: bits 0-1
        - payload_type: bits 2-5
        - version: bits 6-7
    """
    if len(raw_hex) < 2:
        raise ValueError(f"raw hex too short: {raw_hex}")
    
    # First byte is first 2 hex characters
    first_byte = int(raw_hex[0:2], 16)
    
    route_type = first_byte & 0b00000011           # Bits 0-1
    payload_type = (first_byte >> 2) & 0b00001111  # Bits 2-5
    version = (first_byte >> 6) & 0b00000011       # Bits 6-7
    
    return route_type, payload_type, version


# ============================================================================
# DatabaseWriter Class
# ============================================================================

class DatabaseWriter:
    """
    Async database writer with batched inserts and metrics tracking.
    """
    
    def __init__(
        self,
        db_path: str = "meshcore.db",
        batch_size: int = 50,
        batch_timeout: float = 30.0
    ):
        """
        Initialize DatabaseWriter.
        
        Args:
            db_path: Path to SQLite database file
            batch_size: Max messages per batch before commit (default from config [database].batch_size)
            batch_timeout: Max seconds to wait before commit if batch not full (default from config [database].batch_timeout)
        """
        self.db_path = db_path
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        self.queue: asyncio.Queue = asyncio.Queue()
        self.conn: Optional[sqlite3.Connection] = None
        self.running = False
        
        # Metrics
        self.packets_inserted = 0
        self.observers_updated = 0
        self.last_metric_time = time.time()
        self.last_metric_log_time = time.time()
        
        logger.info(
            "DatabaseWriter initialized: batch_size=%d, batch_timeout=%.1fs",
            batch_size,
            batch_timeout
        )
    
    async def start(self):
        """Initialize database and start processing loop."""
        logger.info("Starting DatabaseWriter...")
        self.conn = init_db(self.db_path)
        self.running = True
        self.last_metric_time = time.time()
        self.last_metric_log_time = time.time()
        
        # Start batch processing task
        asyncio.create_task(self._process_loop())
        logger.info("DatabaseWriter started")
    
    async def stop(self):
        """Stop processing and flush remaining queue."""
        logger.info("Stopping DatabaseWriter...")
        self.running = False
        
        # Process any remaining items in queue
        await self._flush_queue()
        
        # Close database connection
        if self.conn:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                self.conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error("Error closing database: %s", e, exc_info=True)
        
        logger.info("DatabaseWriter stopped")
    
    async def enqueue_observer(self, data: Dict[str, Any]):
        """Enqueue an observer message for insertion."""
        await self.queue.put(('observer', data))
    
    async def enqueue_packet(self, data: Dict[str, Any]):
        """Enqueue a packet message for insertion."""
        await self.queue.put(('packet', data))
    
    async def populate_iatas(self, iata_codes: list[str]):
        """
        Populate the iata table with IATA codes extracted from subscribed topics.
        
        Args:
            iata_codes: List of IATA codes to insert
        """
        if not self.conn:
            logger.error("Database connection not initialized")
            return
        
        if not iata_codes:
            logger.debug("No IATA codes to populate")
            return
        
        try:
            now = time.time()
            inserted_count = 0
            
            for iata in iata_codes:
                iata_upper = iata.upper()
                try:
                    self.conn.execute("""
                        INSERT OR IGNORE INTO iatas (iata, added_at)
                        VALUES (?, ?)
                    """, (iata_upper, now))
                    inserted_count += 1
                except Exception as e:
                    logger.error("Error inserting IATA %s: %s", iata, e)
            
            self.conn.commit()
            logger.info("Populated iata table with %d codes", inserted_count)
        except Exception as e:
            logger.error("Error populating IATA table: %s", e, exc_info=True)
            try:
                self.conn.rollback()
            except Exception as rollback_error:
                logger.error("Error rolling back: %s", rollback_error)
    
    async def _process_loop(self):
        """Main processing loop - collects and batches messages."""
        logger.info("Database processing loop started")
        
        while self.running:
            try:
                await self._process_batch()
                await self._log_metrics_if_needed()
            except Exception as e:
                logger.error(
                    "Error in database processing loop: %s",
                    e,
                    exc_info=True
                )
                await asyncio.sleep(1)  # Brief pause before retry
        
        logger.info("Database processing loop stopped")
    
    async def _process_batch(self):
        """Collect and process a batch of messages."""
        batch = []
        deadline = time.time() + self.batch_timeout
        
        # Collect messages until batch_size or timeout
        while len(batch) < self.batch_size:
            timeout = max(0.1, deadline - time.time())
            
            try:
                msg = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=timeout
                )
                batch.append(msg)
            except asyncio.TimeoutError:
                # Timeout reached, process what we have
                break
        
        # Process the batch if we have any messages
        if batch:
            await self._insert_batch(batch)
    
    async def _insert_batch(self, batch: list):
        """
        Insert a batch of messages in a single transaction.
        
        Args:
            batch: List of (msg_type, data) tuples
        """
        if not self.conn:
            logger.error("Database connection not initialized")
            return
        
        start_time = time.time()
        packets_count = 0
        observers_count = 0
        
        try:
            # Begin transaction
            self.conn.execute("BEGIN TRANSACTION")
            
            for msg_type, data in batch:
                try:
                    if msg_type == 'observer':
                        self._insert_observer(data)
                        observers_count += 1
                    elif msg_type == 'packet':
                        self._insert_packet(data)
                        packets_count += 1
                except Exception as e:
                    logger.error(
                        "Error inserting %s: %s (data: %s)",
                        msg_type,
                        e,
                        data,
                        exc_info=True
                    )
                    # Continue processing other messages
            
            # Commit transaction
            self.conn.commit()
            
            # Update metrics
            self.packets_inserted += packets_count
            self.observers_updated += observers_count
            
            duration = time.time() - start_time
            logger.debug(
                "Batch inserted: %d packets, %d observer updates in %.3fs",
                packets_count,
                observers_count,
                duration
            )
            
        except Exception as e:
            logger.error("Error committing batch: %s", e, exc_info=True)
            try:
                self.conn.rollback()
            except Exception as rollback_error:
                logger.error("Error rolling back: %s", rollback_error)
    
    def _insert_observer(self, data: Dict[str, Any]):
        """
        Insert or update observer record.
        Updates if any field (name, radio, client_version, iata, status) changes.
        
        Args:
            data: Observer data from JSON
        """
        if not self.conn:
            raise RuntimeError("Database connection not initialized")
        
        now = time.time()
        pubkey = data.get('origin_id', '').upper()
        
        if not pubkey:
            raise ValueError("Missing origin_id in observer data")
        
        # Extract topic data
        topic_data = data.get('_topic_data', {})
        iata = topic_data.get('iata', '').upper()

        # Parse observer status from status JSON
        raw_status = data.get('status')
        normalized_status = str(raw_status).lower() if raw_status is not None else None
        status = normalized_status if normalized_status in ('online', 'offline') else None
        
        # Upsert: insert if new, update if any field changed
        self.conn.execute("""
            INSERT INTO observers (pubkey, name, radio, client_version, iata, status, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pubkey) DO UPDATE SET
                name = excluded.name,
                radio = excluded.radio,
                client_version = excluded.client_version,
                iata = excluded.iata,
                status = excluded.status,
                last_seen = excluded.last_seen
            WHERE name IS NOT excluded.name
               OR radio IS NOT excluded.radio
               OR client_version IS NOT excluded.client_version
               OR iata IS NOT excluded.iata
               OR status IS NOT excluded.status
               OR (name IS NULL AND excluded.name IS NOT NULL)
               OR (radio IS NULL AND excluded.radio IS NOT NULL)
               OR (client_version IS NULL AND excluded.client_version IS NOT NULL)
               OR (iata IS NULL AND excluded.iata IS NOT NULL)
               OR (status IS NULL AND excluded.status IS NOT NULL)
               OR (name IS NOT NULL AND excluded.name IS NULL)
               OR (radio IS NOT NULL AND excluded.radio IS NULL)
               OR (client_version IS NOT NULL AND excluded.client_version IS NULL)
               OR (iata IS NOT NULL AND excluded.iata IS NULL)
               OR (status IS NOT NULL AND excluded.status IS NULL)
        """, (
            pubkey,
            data.get('origin', ''),
            data.get('radio'),
            data.get('client_version'),
            iata,
            status,
            now,
            now
        ))
    
    def _insert_packet(self, data: Dict[str, Any]):
        """
        Insert packet record with validation.
        
        Args:
            data: Packet data from JSON plus topic_data
        """
        if not self.conn:
            raise RuntimeError("Database connection not initialized")
        
        # Validate first
        is_valid, error_msg = validate_packet_data(data)
        
        if not is_valid:
            # Special handling for raw length mismatch - log error but still insert
            if error_msg and "raw length" in error_msg:
                logger.error(
                    "Packet raw length validation failed: %s. Inserting anyway. Data: %s",
                    error_msg,
                    data
                )
                # Continue with insertion
            else:
                # Other validation errors - raise to be caught and logged as error
                raise ValueError(error_msg)
        
        # Extract topic data
        topic_data = data.get('_topic_data', {})
        iata = topic_data.get('iata', '').upper()
        pubkey = topic_data.get('pubkey', '').upper()
        observer = pubkey[:4] if pubkey else ''
        
        # Parse bit fields from raw
        raw = data['raw'].upper()
        route_type, payload_type, version = parse_raw_bits(raw)
        
        # Insert packet
        now = time.time()
        self.conn.execute("""
            INSERT INTO packets (timestamp, iata, observer, raw, route_type, payload_type, version)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            now,
            iata,
            observer,
            raw,
            route_type,
            payload_type,
            version
        ))
    
    async def _flush_queue(self):
        """Flush all remaining items in queue."""
        logger.info("Flushing remaining queue items...")
        batch = []
        
        # Drain queue
        while not self.queue.empty():
            try:
                msg = self.queue.get_nowait()
                batch.append(msg)
            except asyncio.QueueEmpty:
                break
        
        # Insert final batch
        if batch:
            await self._insert_batch(batch)
            logger.info("Flushed %d remaining items", len(batch))
    
    async def _log_metrics_if_needed(self):
        """Log metrics every 60 seconds."""
        now = time.time()
        elapsed = now - self.last_metric_log_time
        
        if elapsed >= 60.0:
            await self._log_metrics()
            self.last_metric_log_time = now
    
    async def _log_metrics(self):
        """Log current metrics."""
        now = time.time()
        elapsed = now - self.last_metric_time
        
        if elapsed == 0:
            elapsed = 0.001  # Avoid division by zero
        
        # Calculate rates
        packet_rate = self.packets_inserted / elapsed
        observer_rate = self.observers_updated / elapsed
        queue_depth = self.queue.qsize()
        
        # Get database file size
        db_size_mb = 0.0
        try:
            if os.path.exists(self.db_path):
                db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024)
        except Exception as e:
            logger.error("Error getting DB size: %s", e)
        
        logger.info(
            "[METRICS] packets=%d observers=%d queue_depth=%d db_size_mb=%.1f "
            "packet_rate=%.1f/sec observer_rate=%.1f/sec",
            self.packets_inserted,
            self.observers_updated,
            queue_depth,
            db_size_mb,
            packet_rate,
            observer_rate
        )
        
        # Warn if queue is backing up
        if queue_depth > 1000:
            logger.warning(
                "Queue depth is high: %d items pending (possible backpressure)",
                queue_depth
            )
        
        # Reset counters
        self.packets_inserted = 0
        self.observers_updated = 0
        self.last_metric_time = now

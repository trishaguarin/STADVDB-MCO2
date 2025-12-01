from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error, pooling
import logging
from datetime import datetime
from contextlib import contextmanager
import time
from datetime import timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ========================================================================
# DATABASE CONNECTION
# ========================================================================

POOL_SIZE = 5
POOL_RESET_SESSION = True

def create_connection_pool(host, user, password, database, port=3306, pool_name=None):
    """Create a connection pool for a database node"""
    try:
        pool = pooling.MySQLConnectionPool(
            pool_name=pool_name or f"{host}_{port}",
            pool_size=POOL_SIZE,
            pool_reset_session=POOL_RESET_SESSION,
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            autocommit=False,
            buffered=True,
            consume_results=True
        )
        logger.info(f"Created connection pool for {host}:{port}")
        return pool
    except Error as e:
        logger.error(f"Cannot create connection pool for {host}:{port} - {e}")
        return None

central_pool = create_connection_pool(
    host="ccscloud.dlsu.edu.ph",
    user="stadvdb",
    password="Password123!",
    database="stadvdb_node1",
    port=60820,
    pool_name="central_pool"
)

node2_pool = create_connection_pool(
    host="ccscloud.dlsu.edu.ph",
    user="stadvdb",
    password="Password123!",
    database="stadvdb_node2",
    port=60821,
    pool_name="node2_pool"
)

node3_pool = create_connection_pool(
    host="ccscloud.dlsu.edu.ph",
    user="stadvdb",
    password="Password123!",
    database="stadvdb_node3",
    port=60822,
    pool_name="node3_pool"
)

@contextmanager
def get_connection(pool):
    """Context manager for getting a connection from the pool"""
    conn = None
    try:
        if pool:
            conn = pool.get_connection()
            yield conn
        else:
            raise Error("Connection pool is not available")
    except Error as e:
        logger.error(f"Failed to get connection from pool: {e}")
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()

def check_pool_health(pool):
    """Check if a connection pool is healthy"""
    if not pool:
        return False
    try:
        with get_connection(pool) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
    except Error as e:
        logger.error(f"Connection pool health check failed: {e}")
        return False

# ========================================================================
# TWO-PHASE LOCKING (2PL) MANAGER
# ========================================================================

class TwoPhaseLockManager:
    """
    Manages Two-Phase Locking (2PL) protocol:
    - Growing Phase: Acquire locks, cannot release
    - Shrinking Phase: Release locks, cannot acquire
    """
    
    def __init__(self):
        self.connections = []  # List of (connection, pool_name) tuples
        self.locks_acquired = []  # Track acquired locks
        self.phase = 'GROWING'  # GROWING or SHRINKING
        self.start_time = None
        
    def acquire_connection(self, pool):
        """Acquire a connection during GROWING phase"""
        if self.phase == 'SHRINKING':
            raise Exception("Cannot acquire locks in SHRINKING phase - 2PL violation!")
        
        if self.start_time is None:
            self.start_time = time.time()
            
        conn = pool.get_connection()
        self.connections.append((conn, pool.pool_name))
        logger.info(f"[2PL GROWING] Acquired connection from {pool.pool_name}")
        return conn
    
    def acquire_row_lock(self, conn, order_id, lock_type='FOR UPDATE'):
        """Acquire row-level lock during GROWING phase"""
        if self.phase == 'SHRINKING':
            raise Exception("Cannot acquire locks in SHRINKING phase - 2PL violation!")
        
        cursor = conn.cursor()
        if lock_type == 'FOR UPDATE':
            cursor.execute(
                "SELECT orderID FROM FactOrders WHERE orderID = %s FOR UPDATE",
                (order_id,)
            )
            logger.info(f"[2PL GROWING] Acquired EXCLUSIVE lock on order {order_id}")
        else:
            cursor.execute(
                "SELECT orderID FROM FactOrders WHERE orderID = %s LOCK IN SHARE MODE",
                (order_id,)
            )
            logger.info(f"[2PL GROWING] Acquired SHARED lock on order {order_id}")
        
        result = cursor.fetchone()
        cursor.close()
        
        self.locks_acquired.append({
            'order_id': order_id,
            'lock_type': lock_type,
            'connection': conn.pool_name
        })
        
        return result
    
    def set_isolation_level(self, conn, level):
        """Set isolation level (allowed during GROWING phase)"""
        cursor = conn.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
        cursor.close()
        logger.debug(f"Isolation level set to {level}")
    
    def begin_shrinking_phase(self):
        """Transition from GROWING to SHRINKING phase"""
        if self.phase == 'SHRINKING':
            logger.warning("Already in SHRINKING phase")
            return
        
        self.phase = 'SHRINKING'
        elapsed = time.time() - self.start_time if self.start_time else 0
        logger.info(f"[2PL] Entering SHRINKING phase (locks held for {elapsed:.3f}s)")
        logger.info(f"[2PL] Total locks acquired: {len(self.locks_acquired)}")
    
    def commit_all(self):
        """Commit all transactions during SHRINKING phase"""
        if self.phase != 'SHRINKING':
            self.begin_shrinking_phase()
        
        for conn, pool_name in self.connections:
            try:
                if conn.is_connected():
                    conn.commit()
                    logger.info(f"[2PL SHRINKING] Committed transaction on {pool_name}")
            except Error as e:
                logger.error(f"Failed to commit on {pool_name}: {e}")
    
    def rollback_all(self):
        """Rollback all transactions during SHRINKING phase"""
        if self.phase != 'SHRINKING':
            self.begin_shrinking_phase()
        
        for conn, pool_name in self.connections:
            try:
                if conn.is_connected():
                    conn.rollback()
                    logger.info(f"[2PL SHRINKING] Rolled back transaction on {pool_name}")
            except Error as e:
                logger.error(f"Failed to rollback on {pool_name}: {e}")
    
    def release_all(self):
        """Release all connections and locks during SHRINKING phase"""
        if self.phase != 'SHRINKING':
            self.begin_shrinking_phase()
        
        for conn, pool_name in self.connections:
            try:
                if conn.is_connected():
                    conn.close()
                    logger.info(f"[2PL SHRINKING] Released connection to {pool_name}")
            except Error as e:
                logger.error(f"Failed to close connection to {pool_name}: {e}")
        
        elapsed = time.time() - self.start_time if self.start_time else 0
        logger.info(f"[2PL] Transaction complete. Total time: {elapsed:.3f}s")
        
        self.connections.clear()
        self.locks_acquired.clear()
    
    def get_lock_summary(self):
        """Get summary of acquired locks"""
        return {
            'phase': self.phase,
            'locks_count': len(self.locks_acquired),
            'locks': self.locks_acquired,
            'connections': [pool_name for _, pool_name in self.connections]
        }

# ========================================================================
# ISOLATION LEVEL HANDLING
# ========================================================================

def set_isolation_level(conn, level):
    """Set transaction isolation level for a connection"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
        cursor.close()
        logger.debug(f"Isolation level set to {level}")
        return True
    except Error as e:
        logger.error(f"Failed to set isolation level: {e}")
        return False

# ========================================================================
# CONCURRENCY WARNING FUNCTIONS
# ========================================================================

def get_isolation_warnings(level, operation_type):
    """Get warnings about possible concurrency problems for current isolation level"""
    warnings = {
        "READ UNCOMMITTED": {
            "read": ["DIRTY READS possible"],
            "update": ["NON-REPEATABLE READS possible", "DIRTY READS possible"],
            "insert": ["PHANTOM READS possible"]
        },
        "READ COMMITTED": {
            "read": ["NON-REPEATABLE READS possible"],
            "update": ["NON-REPEATABLE READS possible"],
            "insert": ["PHANTOM READS possible"]
        },
        "REPEATABLE READ": {
            "read": ["DIRTY READS prevented", "NON-REPEATABLE READS prevented"],
            "update": ["Data consistency maintained within transaction"],
            "insert": ["PHANTOM READS possible"]
        },
        "SERIALIZABLE": {
            "read": ["All concurrency problems prevented"],
            "update": ["Complete isolation from other transactions"],
            "insert": ["Full serializability guaranteed"]
        }
    }
    
    if level in warnings and operation_type in warnings[level]:
        return {
            "isolation_level": level,
            "warnings": warnings[level][operation_type]
        }
    return None

def check_for_phantom_insert(year, level):
    """Check if phantom reads are possible after insert"""
    if level in ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ"]:
        return {
            "type": "PHANTOM READ WARNING",
            "message": f"New order in year {year} could appear as 'phantom row' in concurrent transactions at {level} isolation level"
        }
    return None

def check_for_non_repeatable_read_update(order_id, old_date, new_date, level):
    """Check if non-repeatable reads are possible after update"""
    if level in ["READ UNCOMMITTED", "READ COMMITTED"]:
        return {
            "type": "NON-REPEATABLE READ WARNING",
            "message": f"Order {order_id} changed from {old_date} to {new_date}. Concurrent transactions at {level} may see inconsistent values"
        }
    return None

# ========================================================================
# PARTITIONING LOGIC
# ========================================================================

def determine_partition_node(delivery_date):
    """Determine which partition node based on year"""
    try:
        if isinstance(delivery_date, str):
            year = int(delivery_date[:4])
        else:
            year = delivery_date.year
        
        if year == 2024:
            return node2_pool
        elif year == 2025:
            return node3_pool
        else:
            return None
    except Exception as e:
        logger.error(f"Invalid date format: {delivery_date} - {e}")
        return None

# ========================================================================
# API ENDPOINTS WITH 2PL
# ========================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "nodes": {
            "central": check_pool_health(central_pool),
            "node2": check_pool_health(node2_pool),
            "node3": check_pool_health(node3_pool)
        }
    })

@app.route('/api/insert', methods=['POST'])
def insert_order():
    """Insert a new order with 2PL"""
    start_time = datetime.now()
    lock_mgr = TwoPhaseLockManager()
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        delivery_date = data.get('delivery_date')
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        use_locking = data.get('use_locking', True)
        
        if not order_id or not delivery_date:
            return jsonify({"error": "Missing required fields"}), 400
        
        warnings_info = get_isolation_warnings(isolation_level, "insert")
        
        # ============ GROWING PHASE ============
        logger.info(f"[2PL] Starting GROWING phase for INSERT order {order_id}")
        
        # 1. Acquire connection to central node
        central_conn = lock_mgr.acquire_connection(central_pool)
        lock_mgr.set_isolation_level(central_conn, isolation_level)
        
        # 2. Acquire lock on central node (check for duplicate)
        if use_locking:
            result = lock_mgr.acquire_row_lock(central_conn, order_id, 'FOR UPDATE')
            if result:
                return jsonify({"error": "OrderID already exists"}), 400
        else:
            cursor = central_conn.cursor()
            cursor.execute("SELECT 1 FROM FactOrders WHERE orderID = %s", (order_id,))
            if cursor.fetchone():
                cursor.close()
                return jsonify({"error": "OrderID already exists"}), 400
            cursor.close()
        
        # 3. Determine partition and acquire connection
        partition_pool = determine_partition_node(delivery_date)
        partition_conn = None
        if partition_pool:
            partition_conn = lock_mgr.acquire_connection(partition_pool)
            lock_mgr.set_isolation_level(partition_conn, isolation_level)
            
            # 4. Acquire lock on partition node
            if use_locking:
                lock_mgr.acquire_row_lock(partition_conn, order_id, 'FOR UPDATE')
        
        # All locks acquired - perform operations
        logger.info(f"[2PL] All locks acquired for order {order_id}")
        
        # 5. Insert into central node
        cursor = central_conn.cursor()
        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
        """
        cursor.execute(sql, (order_id, delivery_date))
        cursor.close()
        
        # 6. Insert into partition node
        if partition_conn:
            cursor = partition_conn.cursor()
            cursor.execute(sql, (order_id, delivery_date))
            cursor.close()
        
        # ============ SHRINKING PHASE ============
        lock_mgr.begin_shrinking_phase()
        
        # 7. Commit all transactions
        lock_mgr.commit_all()
        
        # 8. Release all locks
        lock_mgr.release_all()
        
        year = int(delivery_date[:4])
        phantom_warning = check_for_phantom_insert(year, isolation_level)
        
        end_time = datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        response = {
            "success": True,
            "message": "Order inserted successfully",
            "order_id": order_id,
            "locking_protocol": "2PL (Two-Phase Locking)",
            "locking_used": use_locking,
            "lock_summary": lock_mgr.get_lock_summary(),
            "transaction_time_ms": duration_ms,
            "concurrency_info": {
                "isolation_warnings": warnings_info,
                "phantom_warning": phantom_warning
            }
        }
        
        return jsonify(response), 201
        
    except Exception as e:
        logger.exception(f"Insert failed: {e}")
        lock_mgr.rollback_all()
        lock_mgr.release_all()
        return jsonify({"error": str(e)}), 500

@app.route('/api/read', methods=['POST'])
def read_order():
    """Read an order from all nodes with 2PL"""
    start_time = datetime.now()
    lock_mgr = TwoPhaseLockManager()
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        use_locking = data.get('use_locking', False)
        
        if not order_id:
            return jsonify({"error": "Missing order_id"}), 400
        
        warnings_info = get_isolation_warnings(isolation_level, "read")
        results = {}
        
        # ============ GROWING PHASE ============
        logger.info(f"[2PL] Starting GROWING phase for READ order {order_id}")
        
        nodes = [
            ("central", central_pool),
            ("node2", node2_pool),
            ("node3", node3_pool)
        ]
        
        # 1. Acquire all connections first
        connections = {}
        for label, pool in nodes:
            if pool:
                conn = lock_mgr.acquire_connection(pool)
                lock_mgr.set_isolation_level(conn, isolation_level)
                connections[label] = conn
        
        # 2. Acquire all locks
        if use_locking:
            for label, conn in connections.items():
                lock_mgr.acquire_row_lock(conn, order_id, 'LOCK IN SHARE MODE')
        
        # 3. Perform reads with locks held
        found = False
        for label, conn in connections.items():
            cursor = conn.cursor(dictionary=True)
            
            if label == "central":
                cursor.execute(
                    "SELECT orderID, deliveryDate FROM FactOrders WHERE orderID = %s",
                    (order_id,)
                )
            else:
                cursor.execute(
                    "SELECT orderID, deliveryDate, createdAt, updatedAt FROM FactOrders WHERE orderID = %s",
                    (order_id,)
                )
            
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                for key, value in row.items():
                    if hasattr(value, 'isoformat'):
                        row[key] = value.isoformat()
                
                results[label] = {"status": "found", "data": row}
                found = True
            else:
                results[label] = {"status": "not_found"}
        
        # ============ SHRINKING PHASE ============
        lock_mgr.begin_shrinking_phase()
        
        # 4. Commit (releases shared locks)
        lock_mgr.commit_all()
        
        # 5. Release connections
        lock_mgr.release_all()
        
        if not found:
            return jsonify({
                "success": False,
                "error": f"Order {order_id} not found in any node",
                "results": results
            }), 404
        
        end_time = datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        return jsonify({
            "success": True,
            "message": "Order retrieved successfully",
            "order_id": order_id,
            "results": results,
            "locking_protocol": "2PL (Two-Phase Locking)",
            "locking_used": use_locking,
            "lock_summary": lock_mgr.get_lock_summary(),
            "transaction_time_ms": duration_ms,
            "concurrency_info": {
                "isolation_warnings": warnings_info
            }
        }), 200
        
    except Exception as e:
        logger.exception(f"Read failed: {e}")
        lock_mgr.rollback_all()
        lock_mgr.release_all()
        return jsonify({"error": str(e)}), 500

@app.route('/api/update', methods=['POST'])
def update_order():
    """Update an order with 2PL"""
    start_time = datetime.now()
    lock_mgr = TwoPhaseLockManager()
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        new_delivery_date = data.get('delivery_date')
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        use_locking = data.get('use_locking', True)
        
        if not order_id or not new_delivery_date:
            return jsonify({"error": "Missing required fields"}), 400
        
        warnings_info = get_isolation_warnings(isolation_level, "update")
        
        # ============ GROWING PHASE ============
        logger.info(f"[2PL] Starting GROWING phase for UPDATE order {order_id}")
        
        # 1. First, acquire central connection to check existence
        central_conn = lock_mgr.acquire_connection(central_pool)
        lock_mgr.set_isolation_level(central_conn, isolation_level)
        
        # 2. Acquire lock and get current data
        if use_locking:
            result = lock_mgr.acquire_row_lock(central_conn, order_id, 'FOR UPDATE')
        else:
            cursor = central_conn.cursor()
            cursor.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = %s", (order_id,))
            result = cursor.fetchone()
            cursor.close()
        
        if not result:
            return jsonify({"error": "OrderID does not exist"}), 404
        
        # Get old delivery date
        cursor = central_conn.cursor()
        cursor.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = %s", (order_id,))
        old_delivery_date = cursor.fetchone()[0]
        cursor.close()
        
        old_year = int(str(old_delivery_date)[:4])
        new_year = int(new_delivery_date[:4])
        
        # 3. Acquire partition connections
        old_partition = determine_partition_node(old_delivery_date)
        new_partition = determine_partition_node(new_delivery_date)
        
        partition_connections = {}
        
        if old_partition:
            old_conn = lock_mgr.acquire_connection(old_partition)
            lock_mgr.set_isolation_level(old_conn, isolation_level)
            if use_locking:
                lock_mgr.acquire_row_lock(old_conn, order_id, 'FOR UPDATE')
            partition_connections['old'] = old_conn
        
        if new_partition and new_partition != old_partition:
            new_conn = lock_mgr.acquire_connection(new_partition)
            lock_mgr.set_isolation_level(new_conn, isolation_level)
            if use_locking:
                lock_mgr.acquire_row_lock(new_conn, order_id, 'FOR UPDATE')
            partition_connections['new'] = new_conn
        
        logger.info(f"[2PL] All locks acquired for UPDATE order {order_id}")
        
        # 4. Perform updates with all locks held
        
        # Handle year change (move between partitions)
        if old_year != new_year and old_partition and new_partition:
            logger.info(f"Year changed from {old_year} to {new_year} - moving partitions")
            
            # Insert into new partition
            if 'new' in partition_connections:
                cursor = partition_connections['new'].cursor()
                sql = """
                INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
                """
                cursor.execute(sql, (order_id, new_delivery_date))
                cursor.close()
            
            # Delete from old partition
            if 'old' in partition_connections:
                cursor = partition_connections['old'].cursor()
                cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
                cursor.close()
        
        # Update central node
        cursor = central_conn.cursor()
        sql = """
        UPDATE FactOrders
        SET deliveryDate = %s, updatedAt = NOW()
        WHERE orderID = %s
        """
        cursor.execute(sql, (new_delivery_date, order_id))
        cursor.close()
        
        # Update partition if same year
        if old_year == new_year and 'old' in partition_connections:
            cursor = partition_connections['old'].cursor()
            cursor.execute(sql, (new_delivery_date, order_id))
            cursor.close()
        
        # ============ SHRINKING PHASE ============
        lock_mgr.begin_shrinking_phase()
        
        # 5. Commit all
        lock_mgr.commit_all()
        
        # 6. Release all locks
        lock_mgr.release_all()
        
        non_repeatable_warning = check_for_non_repeatable_read_update(
            order_id, old_delivery_date, new_delivery_date, isolation_level
        )
        
        end_time = datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        return jsonify({
            "success": True,
            "message": "Order updated successfully",
            "order_id": order_id,
            "old_date": str(old_delivery_date),
            "new_date": new_delivery_date,
            "locking_protocol": "2PL (Two-Phase Locking)",
            "locking_used": use_locking,
            "lock_summary": lock_mgr.get_lock_summary(),
            "transaction_time_ms": duration_ms,
            "concurrency_info": {
                "isolation_warnings": warnings_info,
                "non_repeatable_warning": non_repeatable_warning
            }
        }), 200
        
    except Exception as e:
        logger.exception(f"Update failed: {e}")
        lock_mgr.rollback_all()
        lock_mgr.release_all()
        return jsonify({"error": str(e)}), 500

@app.route('/api/delete', methods=['POST'])
def delete_order():
    """Delete an order from all nodes with 2PL"""
    start_time = datetime.now()
    lock_mgr = TwoPhaseLockManager()
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        use_locking = data.get('use_locking', True)
        
        if not order_id:
            return jsonify({"error": "Missing order_id"}), 400
        
        # ============ GROWING PHASE ============
        logger.info(f"[2PL] Starting GROWING phase for DELETE order {order_id}")
        
        # 1. Acquire all connections
        nodes = [
            ("central", central_pool),
            ("node2", node2_pool),
            ("node3", node3_pool)
        ]
        
        connections = {}
        for label, pool in nodes:
            if pool:
                conn = lock_mgr.acquire_connection(pool)
                lock_mgr.set_isolation_level(conn, isolation_level)
                connections[label] = conn
        
        # 2. Acquire all locks
        if use_locking:
            for label, conn in connections.items():
                lock_mgr.acquire_row_lock(conn, order_id, 'FOR UPDATE')
        
        # 3. Check existence
        cursor = connections['central'].cursor()
        cursor.execute("SELECT 1 FROM FactOrders WHERE orderID = %s", (order_id,))
        exists = cursor.fetchone() is not None
        cursor.close()
        
        if not exists:
            lock_mgr.release_all()
            return jsonify({"error": "OrderID does not exist"}), 404
        
        logger.info(f"[2PL] All locks acquired for DELETE order {order_id}")
        
        # 4. Perform deletes with all locks held
        deletion_results = {}
        for label, conn in connections.items():
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
                cursor.close()
                deletion_results[label] = "success"
            except Error as e:
                deletion_results[label] = f"error: {str(e)}"
        
        # ============ SHRINKING PHASE ============
        lock_mgr.begin_shrinking_phase()
        
        # 5. Commit all
        lock_mgr.commit_all()
        
        # 6. Release all locks
        lock_mgr.release_all()
        
        end_time = datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        
        return jsonify({
            "success": True,
            "message": "Order deleted successfully",
            "order_id": order_id,
            "deletion_results": deletion_results,
            "locking_protocol": "2PL (Two-Phase Locking)",
            "locking_used": use_locking,
            "lock_summary": lock_mgr.get_lock_summary(),
            "transaction_time_ms": duration_ms
        }), 200
        
    except Exception as e:
        logger.exception(f"Delete failed: {e}")
        lock_mgr.rollback_all()
        lock_mgr.release_all()
        return jsonify({"error": str(e)}), 500

# ========================================================================
# ERROR HANDLERS
# ========================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

def startup_recovery_check():
    """
    Run recovery check on startup using database time
    """
    logger.info("Running startup recovery check...")
    
    # Get current time from database
    try:
        with get_connection(central_pool) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT NOW()")
            db_now = cursor.fetchone()[0]
            cursor.close()
    except Exception as e:
        logger.error(f"Failed to get database time: {e}")
        db_now = datetime.now()
    
    # Calculate window based on database time
    end_time = db_now
    start_time = end_time - timedelta(hours=24)
    
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Using database time window: {start_str} to {end_str}")
    
    results = {}
    
    # Try to recover central if healthy
    if check_pool_health(central_pool):
        logger.info("Central is online - checking for missing data...")
        try:
            result = recover_missing_to_central(start_str, end_str)
            if result['synced'] > 0:
                results['central'] = result
        except Exception as e:
            logger.error(f"Failed to recover central: {e}")
    
    # Try to recover node2 if healthy
    if check_pool_health(node2_pool):
        logger.info("Node2 is online - checking for missing data...")
        try:
            result = recover_missing_to_partition(start_str, end_str, 2024, node2_pool, 'node2')
            if result['synced'] > 0:
                results['node2'] = result
        except Exception as e:
            logger.error(f"Failed to recover node2: {e}")
    
    # Try to recover node3 if healthy
    if check_pool_health(node3_pool):
        logger.info("Node3 is online - checking for missing data...")
        try:
            result = recover_missing_to_partition(start_str, end_str, 2025, node3_pool, 'node3')
            if result['synced'] > 0:
                results['node3'] = result
        except Exception as e:
            logger.error(f"Failed to recover node3: {e}")
    
    return results

# ========================================================================
# MAIN
# ========================================================================

if __name__ == '__main__':
    logger.info("Starting Flask API server with 2PL...")
    logger.info(f"Central Node: {'Connected' if check_pool_health(central_pool) else 'Disconnected'}")
    logger.info(f"Node 2: {'Connected' if check_pool_health(node2_pool) else 'Disconnected'}")
    logger.info(f"Node 3: {'Connected' if check_pool_health(node3_pool) else 'Disconnected'}")
    
    logger.info("Checking for pending recoveries...")
    try:
        results = startup_recovery_check()
        if results:
            logger.info(f"Startup recovery COMPLETED: {results}")
        else:
            logger.info("No data needed recovery")
    except Exception as e:
        logger.error(f"Startup recovery check FAILED: {e}")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
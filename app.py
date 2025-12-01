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
# LOCKING MECHANISMS
# ========================================================================

def acquire_row_lock(conn, order_id, lock_type='FOR UPDATE'):
    """
    Acquire a row-level lock on a specific order
    lock_type: 'FOR UPDATE' (exclusive) or 'LOCK IN SHARE MODE' (shared)
    """
    try:
        cursor = conn.cursor()
        if lock_type == 'FOR UPDATE':
            # Exclusive lock - blocks both reads and writes
            cursor.execute(
                "SELECT orderID FROM FactOrders WHERE orderID = %s FOR UPDATE",
                (order_id,)
            )
            logger.info(f"Acquired exclusive lock on order {order_id}")
        else:
            # Shared lock - allows other shared locks but blocks exclusive locks
            cursor.execute(
                "SELECT orderID FROM FactOrders WHERE orderID = %s LOCK IN SHARE MODE",
                (order_id,)
            )
            logger.info(f"Acquired shared lock on order {order_id}")
        
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except Error as e:
        logger.error(f"Failed to acquire lock on order {order_id}: {e}")
        raise

def acquire_table_lock(conn, lock_type='WRITE'):
    """
    Acquire a table-level lock
    lock_type: 'READ' or 'WRITE'
    Note: Table locks should be released with release_table_lock()
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"LOCK TABLES FactOrders {lock_type}")
        cursor.close()
        logger.info(f"Acquired {lock_type} table lock")
        return True
    except Error as e:
        logger.error(f"Failed to acquire table lock: {e}")
        raise

def release_table_lock(conn):
    """Release all table locks"""
    try:
        cursor = conn.cursor()
        cursor.execute("UNLOCK TABLES")
        cursor.close()
        logger.info("Released table locks")
        return True
    except Error as e:
        logger.error(f"Failed to release table locks: {e}")
        raise

def acquire_distributed_lock(order_id, pools, lock_type='FOR UPDATE', timeout=10):
    """
    Acquire locks across multiple nodes in a specific order to prevent deadlocks
    Returns list of connections with acquired locks
    """
    locked_connections = []
    start_time = time.time()
    
    try:
        # Always acquire locks in the same order: central -> node2 -> node3
        # This prevents circular wait conditions (deadlock prevention)
        for pool in pools:
            if pool is None:
                continue
                
            if time.time() - start_time > timeout:
                raise Exception(f"Lock acquisition timeout after {timeout}s")
            
            conn = pool.get_connection()
            locked_connections.append(conn)
            
            # Acquire lock on this node
            acquire_row_lock(conn, order_id, lock_type)
        
        logger.info(f"Successfully acquired distributed locks for order {order_id}")
        return locked_connections
        
    except Exception as e:
        # If we fail to acquire all locks, release what we have
        logger.error(f"Failed to acquire distributed locks: {e}")
        for conn in locked_connections:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
        raise

def release_distributed_locks(connections):
    """Release all distributed locks by committing/rolling back and closing connections"""
    for conn in connections:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception as e:
            logger.error(f"Error releasing lock: {e}")

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

def check_for_dirty_read_uncommitted(order_id, level):
    """Warning about dirty reads during uncommitted transactions"""
    if level == "READ UNCOMMITTED":
        return {
            "type": "DIRTY READ WARNING",
            "message": f"Other transactions at READ UNCOMMITTED can see uncommitted changes to order {order_id}"
        }
    return None

def get_read_concurrency_note(level):
    """Get concurrency note for read operations"""
    if level in ["READ UNCOMMITTED", "READ COMMITTED"]:
        return {
            "type": "CONCURRENCY NOTE",
            "message": f"At {level}, this data could change if you read it again (Non-repeatable read possible)"
        }
    return None

def get_delete_concurrency_note(level):
    """Get concurrency note for delete operations"""
    if level in ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ"]:
        return {
            "type": "CONCURRENCY NOTE",
            "message": f"At {level}, this deletion might affect concurrent transactions reading counts (potential phantom read impact)"
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
# REPLICATION FUNCTIONS (WITH LOCKING)
# ========================================================================

def replicate_insert_to_central(order_id, delivery_date, level, use_locking=True):
    """Replicate insert from partition node to central node with optional locking"""
    conn = None
    try:
        conn = central_pool.get_connection()
        if not set_isolation_level(conn, level):
            logger.error("Failed to set isolation level for central node")
            return False
        
        # Start transaction
        cursor = conn.cursor()
        
        # Optional: Acquire lock before insert
        if use_locking:
            # Check if order exists and lock if it does
            cursor.execute(
                "SELECT orderID FROM FactOrders WHERE orderID = %s FOR UPDATE",
                (order_id,)
            )
            if cursor.fetchone():
                logger.warning(f"Order {order_id} already exists in central, skipping insert")
                cursor.close()
                return True
        
        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
        """
        
        cursor.execute(sql, (order_id, delivery_date))
        conn.commit()
        cursor.close()
        logger.info(f"Replicated order {order_id} to central node")
        return True
    except Exception as e:
        logger.error(f"Failed to replicate to central node: {e}")
        try:
            if conn and conn.is_connected():
                conn.rollback()
        except:
            pass
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()

def replicate_insert_to_partition(order_id, delivery_date, level, use_locking=True):
    """Replicate insert from central node to partition node with optional locking"""
    conn = None
    node_name = None
    
    try:
        if not delivery_date:
            logger.error("No delivery date provided for partition determination")
            return False
            
        try:
            year = int(delivery_date[:4]) if isinstance(delivery_date, str) else delivery_date.year
        except (ValueError, AttributeError) as e:
            logger.error(f"Invalid delivery date format: {delivery_date} - {e}")
            return False
            
        pool = None
        if year == 2024:
            pool = node2_pool
            node_name = 'node2'
        elif year == 2025:
            pool = node3_pool
            node_name = 'node3'
        else:
            logger.warning(f"No partition node for date {delivery_date}")
            return True
            
        if not pool:
            logger.error("No valid pool found for replication")
            
            if node_name:
                log_node_failure(node_name)
            return False
            
        conn = pool.get_connection()
        
        if not set_isolation_level(conn, level):
            logger.error("Failed to set isolation level for partition node")
            return False
        
        cursor = conn.cursor()
        
        # Optional: Acquire lock before insert
        if use_locking:
            cursor.execute(
                "SELECT orderID FROM FactOrders WHERE orderID = %s FOR UPDATE",
                (order_id,)
            )
            if cursor.fetchone():
                logger.warning(f"Order {order_id} already exists in partition, skipping insert")
                cursor.close()
                return True
        
        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
        """
        
        cursor.execute(sql, (order_id, delivery_date))
        conn.commit()
        cursor.close()
        logger.info(f"Replicated order {order_id} to partition node")
        return True
    except Exception as e:
        logger.error(f"Failed to replicate to partition node: {e}")
        
        if node_name:
            log_node_failure(node_name)
            
        try:
            if conn and conn.is_connected():
                conn.rollback()
        except:
            pass
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()

# ========================================================================
# API ENDPOINTS
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
    """Insert a new order with optional locking"""
    start_time = datetime.now()
    conn = None
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        delivery_date = data.get('delivery_date')
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        use_locking = data.get('use_locking', True)  # New parameter
        
        if not order_id or not delivery_date:
            return jsonify({"error": "Missing required fields"}), 400
        
        warnings_info = get_isolation_warnings(isolation_level, "insert")

        try:
            conn = central_pool.get_connection()
            
            # Set isolation level
            if not set_isolation_level(conn, isolation_level):
                return jsonify({"error": "Failed to set isolation level"}), 500
            
            cursor = conn.cursor()
            
            # Check for existing order (with optional lock)
            if use_locking:
                # Use FOR UPDATE to prevent concurrent inserts
                cursor.execute(
                    "SELECT 1 FROM FactOrders WHERE orderID = %s FOR UPDATE",
                    (order_id,)
                )
            else:
                cursor.execute(
                    "SELECT 1 FROM FactOrders WHERE orderID = %s",
                    (order_id,)
                )
            
            if cursor.fetchone():
                cursor.close()
                return jsonify({"error": "OrderID already exists"}), 400
            
            # Insert into central node
            sql = """
            INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
            VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
            """
            
            cursor.execute(sql, (order_id, delivery_date))
            conn.commit()
            cursor.close()
            logger.info(f"Inserted order {order_id} into central node")
            
            # Replicate to partition
            replication_success = replicate_insert_to_partition(
                order_id, delivery_date, isolation_level, use_locking
            )
            
            # Check for concurrency issues
            year = int(delivery_date[:4])
            phantom_warning = check_for_phantom_insert(year, isolation_level)
            
            end_time = datetime.now()
            duration_ms = (end_time - start_time).total_seconds() * 1000
            
            response = {
                "success": True,
                "message": "Order inserted successfully",
                "order_id": order_id,
                "replication_status": "success" if replication_success else "partial",
                "locking_used": use_locking,
                "transaction_time_ms": duration_ms,
                "concurrency_info": {
                    "isolation_warnings": warnings_info,
                    "phantom_warning": phantom_warning,
                    "locking_info": "Row-level locks used" if use_locking else "No explicit locking"
                }
            }
            
            return jsonify(response), 201
            
        except Error as e:
            logger.error(f"Database error: {e}")
            if conn and conn.is_connected():
                conn.rollback()
            return jsonify({"error": "Database operation failed"}), 500
            
    except ValueError:
        return jsonify({"error": "Invalid input format"}), 400
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500
    finally:
        if conn and conn.is_connected():
            conn.close()

@app.route('/api/read', methods=['POST'])
def read_order():
    """Read an order from all nodes with optional shared locking"""
    start_time = datetime.now()
    results = {}
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        use_locking = data.get('use_locking', False)  # Shared lock for consistent reads
        
        if not order_id:
            return jsonify({"error": "Missing order_id"}), 400
        
        warnings_info = get_isolation_warnings(isolation_level, "read")
        found = False
        
        nodes = [
            ("central", central_pool),
            ("node2", node2_pool),
            ("node3", node3_pool)
        ]
        
        for label, pool in nodes:
            conn = None
            try:
                conn = pool.get_connection()
                if not set_isolation_level(conn, isolation_level):
                    results[label] = {"status": "error", "message": "Failed to set isolation level"}
                    continue
                            
                cursor = conn.cursor(dictionary=True)
                
                # Use LOCK IN SHARE MODE for consistent reads if locking is enabled
                if use_locking:
                    if label == "central":
                        cursor.execute(
                            "SELECT orderID, deliveryDate FROM FactOrders WHERE orderID = %s LOCK IN SHARE MODE",
                            (order_id,)
                        )
                    else:
                        cursor.execute(
                            "SELECT orderID, deliveryDate, createdAt, updatedAt FROM FactOrders WHERE orderID = %s LOCK IN SHARE MODE",
                            (order_id,)
                        )
                else:
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
                conn.commit()  # Release shared lock
                cursor.close()
                
                if row:
                    for key, value in row.items():
                        if hasattr(value, 'isoformat'): 
                            row[key] = value.isoformat()
                    
                    results[label] = {
                        "status": "found",
                        "data": row
                    }
                    found = True
                else:
                    results[label] = {
                        "status": "not_found",
                        "message": f"Order {order_id} not found in {label}"
                    }
                    
            except Error as e:
                logger.error(f"Read from {label} failed: {e}")
                results[label] = {
                    "status": "error",
                    "message": str(e),
                    "node": label
                }
            finally:
                if conn and conn.is_connected():
                    conn.close()
        
        if not found:
            return jsonify({
                "success": False,
                "error": f"Order {order_id} not found in any node",
                "results": results
            }), 404
        
        concurrency_note = get_read_concurrency_note(isolation_level)
        end_time = datetime.now()  
        duration_ms = (end_time - start_time).total_seconds() * 1000

        return jsonify({
            "success": True,
            "message": "Order retrieved successfully",
            "order_id": order_id,
            "results": results,
            "locking_used": use_locking,
            "transaction_time_ms": duration_ms,
            "concurrency_info": {
                "isolation_warnings": warnings_info,
                "concurrency_note": concurrency_note,
                "locking_info": "Shared locks used" if use_locking else "No explicit locking"
            }
        }), 200
        
    except ValueError:
        return jsonify({"error": "Invalid order_id format"}), 400
    except Exception as e:
        logger.exception(f"Read failed: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500

@app.route('/api/update', methods=['POST'])
def update_order():
    """Update an order with distributed locking for consistency"""
    start_time = datetime.now()
    locked_connections = []
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        new_delivery_date = data.get('delivery_date')
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        use_locking = data.get('use_locking', True)
        
        if not order_id or not new_delivery_date:
            return jsonify({"error": "Missing required fields"}), 400
        
        warnings_info = get_isolation_warnings(isolation_level, "update")
        
        # Determine which nodes need to be locked
        nodes_to_lock = [central_pool]
        
        # Get old delivery date first (without lock for now)
        conn_check = central_pool.get_connection()
        cursor = conn_check.cursor()
        cursor.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = %s", (order_id,))
        result = cursor.fetchone()
        cursor.close()
        conn_check.close()
        
        if not result:
            return jsonify({"error": "OrderID does not exist"}), 404
        
        old_delivery_date = result[0]
        old_year = int(str(old_delivery_date)[:4])
        new_year = int(new_delivery_date[:4])
        
        old_partition = determine_partition_node(old_delivery_date)
        new_partition = determine_partition_node(new_delivery_date)
        
        # Add partition nodes to lock list
        if old_partition and old_partition not in nodes_to_lock:
            nodes_to_lock.append(old_partition)
        if new_partition and new_partition not in nodes_to_lock and new_partition != old_partition:
            nodes_to_lock.append(new_partition)
        
        if use_locking:
            # Acquire distributed locks across all relevant nodes
            try:
                locked_connections = acquire_distributed_lock(
                    order_id, 
                    nodes_to_lock, 
                    'FOR UPDATE',
                    timeout=10
                )
                
                # Set isolation level on all connections
                for conn in locked_connections:
                    set_isolation_level(conn, isolation_level)
                
            except Exception as e:
                logger.error(f"Failed to acquire distributed locks: {e}")
                return jsonify({"error": "Failed to acquire locks for update"}), 500
        else:
            # No locking - just get connections
            for pool in nodes_to_lock:
                conn = pool.get_connection()
                set_isolation_level(conn, isolation_level)
                locked_connections.append(conn)
        
        # Now perform the updates with locks held
        dirty_read_warning = check_for_dirty_read_uncommitted(order_id, isolation_level)
        
        # Handle year change (move between partitions)
        if old_year != new_year and old_partition and new_partition:
            logger.info(f"Year changed from {old_year} to {new_year} - moving partitions")
            
            # Find the new partition connection
            new_part_conn = None
            for conn in locked_connections:
                if conn.pool_name == new_partition.pool_name:
                    new_part_conn = conn
                    break
            
            if new_part_conn:
                cursor = new_part_conn.cursor()
                sql = """
                INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
                """
                cursor.execute(sql, (order_id, new_delivery_date))
                new_part_conn.commit()
                cursor.close()
            
            # Delete from old partition
            old_part_conn = None
            for conn in locked_connections:
                if conn.pool_name == old_partition.pool_name:
                    old_part_conn = conn
                    break
            
            if old_part_conn:
                cursor = old_part_conn.cursor()
                cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
                old_part_conn.commit()
                cursor.close()
        
        # Update central node
        central_conn = locked_connections[0]  # Central is always first
        cursor = central_conn.cursor()
        sql = """
        UPDATE FactOrders
        SET deliveryDate = %s, updatedAt = NOW()
        WHERE orderID = %s
        """
        cursor.execute(sql, (new_delivery_date, order_id))
        central_conn.commit()
        cursor.close()
        
        # Update partition if same year
        if old_year == new_year and new_partition:
            for conn in locked_connections:
                if conn.pool_name == new_partition.pool_name:
                    cursor = conn.cursor()
                    sql = """
                    UPDATE FactOrders
                    SET deliveryDate = %s, updatedAt = NOW()
                    WHERE orderID = %s
                    """
                    cursor.execute(sql, (new_delivery_date, order_id))
                    conn.commit()
                    cursor.close()
                    break
        
        # Release all locks
        release_distributed_locks(locked_connections)
        
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
            "locking_used": use_locking,
            "nodes_locked": len(locked_connections) if use_locking else 0,
            "transaction_time_ms": duration_ms,
            "concurrency_info": {
                "isolation_warnings": warnings_info,
                "dirty_read_warning": dirty_read_warning,
                "non_repeatable_warning": non_repeatable_warning,
                "locking_info": f"Distributed locks across {len(locked_connections)} nodes" if use_locking else "No explicit locking"
            }
        }), 200
        
    except ValueError:
        return jsonify({"error": "Invalid input format"}), 400
    except Exception as e:
        logger.exception(f"Update failed: {e}")
        # Ensure locks are released on error
        release_distributed_locks(locked_connections)
        return jsonify({"error": "An unexpected error occurred"}), 500

@app.route('/api/delete', methods=['POST'])
def delete_order():
    """Delete an order from all nodes with distributed locking"""
    start_time = datetime.now()
    locked_connections = []
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        use_locking = data.get('use_locking', True)
        
        if not order_id:
            return jsonify({"error": "Missing order_id"}), 400
        
        # Check existence
        exists = False
        try:
            with get_connection(central_pool) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM FactOrders WHERE orderID = %s", (order_id,))
                exists = cursor.fetchone() is not None
                cursor.close()
        except Error as e:
            logger.error(f"Failed to check order existence: {e}")
            return jsonify({"error": "Database error"}), 500
        
        if not exists:
            return jsonify({"error": "OrderID does not exist"}), 404
        
        deletion_results = {}
        nodes = [
            ("central", central_pool),
            ("node2", node2_pool),
            ("node3", node3_pool)
        ]
        
        if use_locking:
            # Acquire distributed locks
            try:
                pools_to_lock = [pool for _, pool in nodes if pool is not None]
                locked_connections = acquire_distributed_lock(
                    order_id,
                    pools_to_lock,
                    'FOR UPDATE',
                    timeout=10
                )
                
                # Map connections back to node labels
                conn_map = {}
                for i, (label, pool) in enumerate(nodes):
                    if pool is not None:
                        for conn in locked_connections:
                            if conn.pool_name == pool.pool_name:
                                conn_map[label] = conn
                                break
                
                # Delete from each node with lock held
                for label, conn in conn_map.items():
                    try:
                        set_isolation_level(conn, isolation_level)
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
                        conn.commit()
                        cursor.close()
                        deletion_results[label] = "success"
                        logger.info(f"Deleted order {order_id} from {label}")
                    except Error as e:
                        logger.error(f"Delete from {label} failed: {e}")
                        deletion_results[label] = f"error: {str(e)}"
                        conn.rollback()
                
                # Release locks
                release_distributed_locks(locked_connections)
                
            except Exception as e:
                logger.error(f"Failed to acquire distributed locks for delete: {e}")
                release_distributed_locks(locked_connections)
                return jsonify({"error": "Failed to acquire locks for deletion"}), 500
        else:
            # Delete without locking
            for label, pool in nodes:
                conn = None
                try:
                    conn = pool.get_connection()
                    if not set_isolation_level(conn, isolation_level):
                        deletion_results[label] = "failed to set isolation level"
                        continue
                    
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
                    conn.commit()
                    cursor.close()
                    deletion_results[label] = "success"
                    logger.info(f"Deleted order {order_id} from {label}")
                except Error as e:
                    logger.error(f"Delete from {label} failed: {e}")
                    deletion_results[label] = f"error: {str(e)}"
                    if conn and conn.is_connected():
                        try:
                            conn.rollback()
                        except:
                            pass
                finally:
                    if conn and conn.is_connected():
                        conn.close()
        
        concurrency_note = get_delete_concurrency_note(isolation_level)
        end_time = datetime.now()
        duration_ms = (end_time - start_time).total_seconds() * 1000

        return jsonify({
            "success": True,
            "message": "Order deleted successfully",
            "order_id": order_id,
            "deletion_results": deletion_results,
            "locking_used": use_locking,
            "transaction_time_ms": duration_ms,
            "concurrency_info": {
                "concurrency_note": concurrency_note,
                "locking_info": "Distributed locks used" if use_locking else "No explicit locking"
            }
        }), 200
        
    except ValueError:
        return jsonify({"error": "Invalid order_id format"}), 400
    except Exception as e:
        logger.exception(f"Delete failed: {e}")
        release_distributed_locks(locked_connections)
        return jsonify({"error": "An unexpected error occurred"}), 500
    
# ========================================================================
# GLOBAL FAILURE RECOVERY
# ========================================================================

downtime_tracker = {
    'central': None,
    'node2': None,
    'node3': None
}

def log_node_failure(node_name):
    # log when a node fails during replication
    downtime_tracker[node_name] = datetime.now()
    logger.warning(f"Node {node_name} failure detected at {downtime_tracker[node_name]}")

def replicate_to_central(order_data):
    """
    Replicate order to central node with failure detection
    Returns: True if successful, False if failed
    """
    
    try:
        with get_connection(central_pool) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO FactOrders 
                (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_data['orderID'],
                order_data['userID'],
                order_data['deliveryDate'],
                order_data['riderID'],
                order_data['createdAt'],
                order_data['updatedAt'],
                order_data['productID'],
                order_data['quantity']
            ))
            conn.commit()
            cursor.close()
            logger.info(f"Replicated order {order_data['orderID']} to central")
            return True
        
    except Error as e:
        logger.error(f"Failed to replicate to central: {e}")
        log_node_failure('central')
        return False
    
def replicate_to_partition(order_data, partition_pool, node_name):
    """
    Replicate order to partition node with failure detection
    Returns: True if successful, False if failed
    """
    
    try:
        with get_connection(partition_pool) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO FactOrders 
                (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order_data['orderID'],
                order_data['userID'],
                order_data['deliveryDate'],
                order_data['riderID'],
                order_data['createdAt'],
                order_data['updatedAt'],
                order_data['productID'],
                order_data['quantity']
            ))
            conn.commit()
            cursor.close()
            logger.info(f"Replicated order {order_data['orderID']} to {node_name}")
            return True
    except Error as e:
        logger.error(f"Failed to replicate to {node_name}: {e}")
        log_node_failure(node_name)
        return False
                            
def recover_missing_to_central(start_time, end_time):
    """
    Sync missing transactions from partition nodes to central
    *Uses LAST-WRITER-WINS
    """
    logger.info(f"Starting recovery to central from {start_time} to {end_time}")
    
    count_synced = 0
    count_skipped = 0
    
    for node_name, node_pool in [('node2', node2_pool), ('node3', node3_pool)]:
        try:
            with get_connection(node_pool) as conn_partition:
                cursor_partition = conn_partition.cursor()
                
                # get orders from partition node during downtime
                cursor_partition.execute("""
                    SELECT orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity
                    FROM FactOrders 
                    WHERE updatedAt BETWEEN %s AND %s
                """, (start_time, end_time))
                
                orders = cursor_partition.fetchall()
                logger.info(f"Found {len(orders)} orders in {node_name} during downtime")
                
                cursor_partition.close()
                
                # process each order with last-writer-wins
                for order in orders:
                    orderID = order[0]
                    partition_updated_at = order[5]  # updatedAt timestamp
                    
                    try:
                        with get_connection(central_pool) as conn_central:
                            cursor_central = conn_central.cursor()
                            
                            # if order exists in central, get timestamp
                            cursor_central.execute("""
                                SELECT updatedAt FROM FactOrders WHERE orderID = %s
                            """, (orderID,))
                            
                            result = cursor_central.fetchone()
                            
                            if result is None:
                                # if order doesn't exist in central -> INSERT
                                logger.info(f"Inserting order {orderID} to central")
                                cursor_central.execute("""
                                    INSERT INTO FactOrders 
                                    (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """, order)
                                conn_central.commit()
                                count_synced += 1
                            else:
                                # order exists -> check which version is newer (last-writer-wins)
                                central_updated_at = result[0]
                                
                                if partition_updated_at > central_updated_at:
                                    # if partition version is newer -> update central
                                    logger.info(f"Updating order {orderID} in central (partition version is newer)")
                                    cursor_central.execute("""
                                        UPDATE FactOrders 
                                        SET userID=%s, deliveryDate=%s, riderID=%s, 
                                            createdAt=%s, updatedAt=%s, productID=%s, quantity=%s
                                        WHERE orderID=%s
                                    """, (order[1], order[2], order[3], order[4], order[5], order[6], order[7], orderID))
                                    conn_central.commit()
                                    count_synced += 1
                                else:
                                    # if central version is newer or equal -> skip
                                    logger.info(f"Skipping order {orderID} (central version is newer or equal)")
                                    count_skipped += 1
                            
                            cursor_central.close()
                    except Error as e:
                        logger.error(f"Failed to sync order {orderID} to central: {e}")
        
        except Error as e:
            logger.error(f"Failed to recover from {node_name}: {e}")
    
    logger.info(f"Recovery to central complete: {count_synced} synced, {count_skipped} skipped")
    return {'synced': count_synced, 'skipped': count_skipped}

def recover_missing_to_partition(start_time, end_time, year, partition_pool, node_name):
    """
    Sync missing transactions from central to partition node
    *Uses LAST-WRITER-WINS
    Only syncs orders for the specific year partition
    """
    logger.info(f"Starting recovery to {node_name} for year {year} from {start_time} to {end_time}")
    
    count_synced = 0
    count_skipped = 0
    
    try:
        with get_connection(central_pool) as conn_central:
            cursor_central = conn_central.cursor()
            
            cursor_central.execute("""
                SELECT orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity
                FROM FactOrders 
                WHERE updatedAt BETWEEN %s AND %s 
                AND YEAR(deliveryDate) = %s
            """, (start_time, end_time, year))
            
            orders = cursor_central.fetchall()
            logger.info(f"Found {len(orders)} orders for year {year} in central during downtime")
            
            cursor_central.close()
            
            # process each order with last-writer-wins
            for order in orders:
                orderID = order[0]
                central_updated_at = order[5]  # updatedAt timestamp
                
                try:
                    with get_connection(partition_pool) as conn_partition:
                        cursor_partition = conn_partition.cursor()
                        
                        # if order exists in partition, get timestamp
                        cursor_partition.execute("""
                            SELECT updatedAt FROM FactOrders WHERE orderID = %s
                        """, (orderID,))
                        
                        result = cursor_partition.fetchone()
                        
                        if result is None:
                            # if order doesn't exist in partition -> INSERT
                            logger.info(f"Inserting order {orderID} to {node_name}")
                            cursor_partition.execute("""
                                INSERT INTO FactOrders 
                                (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, order)
                            conn_partition.commit()
                            count_synced += 1
                        else:
                            # order exists -> check which version is newer (last-writer-wins)
                            partition_updated_at = result[0]
                            
                            if central_updated_at > partition_updated_at:
                                # if central version is newer -> update partition
                                logger.info(f"Updating order {orderID} in {node_name} (central version is newer)")
                                cursor_partition.execute("""
                                    UPDATE FactOrders 
                                    SET userID=%s, deliveryDate=%s, riderID=%s, 
                                        createdAt=%s, updatedAt=%s, productID=%s, quantity=%s
                                    WHERE orderID=%s
                                """, (order[1], order[2], order[3], order[4], order[5], order[6], order[7], orderID))
                                conn_partition.commit()
                                count_synced += 1
                            else:
                                # if partition version is newer or equal -> skip
                                logger.info(f"Skipping order {orderID} ({node_name} version is newer or equal)")
                                count_skipped += 1
                        
                        cursor_partition.close()
                except Error as e:
                    logger.error(f"Failed to sync order {orderID} to {node_name}: {e}")
    
    except Error as e:
        logger.error(f"Failed to recover to {node_name}: {e}")
    
    logger.info(f"Recovery to {node_name} complete: {count_synced} synced, {count_skipped} skipped")
    return {'synced': count_synced, 'skipped': count_skipped}

def check_and_recover_all():
    """
    Check all nodes and recover if needed
    """
    logger.info("Checking for nodes that need recovery...")
    
    recovery_results = {}
    
    # check if central needs recovery
    if downtime_tracker['central'] is not None:
        if check_pool_health(central_pool):
            logger.info("Central is back online. Starting recovery...")
            start_time = downtime_tracker['central']
            end_time = datetime.now()
            
            result = recover_missing_to_central(
                start_time.strftime('%Y-%m-%d %H:%M:%S'),
                end_time.strftime('%Y-%m-%d %H:%M:%S')
            )
            recovery_results['central'] = result
            downtime_tracker['central'] = None  # clear after recovery
    
    # check if node2 needs recovery
    if downtime_tracker['node2'] is not None:
        if check_pool_health(node2_pool):
            logger.info("Node2 is back online. Starting recovery...")
            start_time = downtime_tracker['node2']
            end_time = datetime.now()
            
            result = recover_missing_to_partition(
                start_time.strftime('%Y-%m-%d %H:%M:%S'),
                end_time.strftime('%Y-%m-%d %H:%M:%S'),
                2024,
                node2_pool,
                'node2'
            )
            recovery_results['node2'] = result
            downtime_tracker['node2'] = None  # clear after recovery 
            
    # check if node3 needs recovery
    if downtime_tracker['node3'] is not None:
        if check_pool_health(node3_pool):
            logger.info("Node3 is back online. Starting recovery...")
            start_time = downtime_tracker['node3']
            end_time = datetime.now()
            
            result = recover_missing_to_partition(
                start_time.strftime('%Y-%m-%d %H:%M:%S'),
                end_time.strftime('%Y-%m-%d %H:%M:%S'),
                2025,
                node3_pool,
                'node3'
            )
            recovery_results['node3'] = result
            downtime_tracker['node3'] = None  # clear after recovery
    
    return recovery_results

# ========================================================================
# RECOVERY ENDPOINTS
# ========================================================================

@app.route('/api/recovery/status', methods=['GET'])
def get_recovery_status():
    # Get current recovery status and downtime tracking
    return jsonify({
        "downtime_tracker": {
            node: dt.strftime('%Y-%m-%d %H:%M:%S') if dt else None
            for node, dt in downtime_tracker.items()
        },
        "node_health": {
            "central": check_pool_health(central_pool),
            "node2": check_pool_health(node2_pool),
            "node3": check_pool_health(node3_pool)
        }
    }), 200

@app.route('/api/recovery/trigger', methods=['POST'])
def trigger_recovery():
    # Manually trigger recovery for all nodes
    try:
        results = check_and_recover_all()
        return jsonify({
            "success": True,
            "message": "Recovery completed",
            "results": results
        }), 200
    except Exception as e:
        logger.exception(f"Recovery failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/recovery/clear', methods=['POST'])
def clear_downtime_tracker():
    # Clear downtime tracker (admin function)
    global downtime_tracker
    downtime_tracker = {'central': None, 'node2': None, 'node3': None}
    return jsonify({"success": True, "message": "Downtime tracker cleared"}), 200

"""
TODO:
    - Recovery API endpoints
    - Modify CRUD operations to call the replication functions
    - Startup recovery
"""
# ========================================================================
# DEBUGGING STUFF (temp)
# ========================================================================

@app.route('/api/debug/recent-orders', methods=['GET'])
def debug_recent_orders():
    """Debug endpoint to see recent orders"""
    try:
        with get_connection(central_pool) as conn:
            cursor = conn.cursor()
            
            # Get orders from last 24 hours
            cursor.execute("""
                SELECT orderID, deliveryDate, updatedAt, 
                       TIMESTAMPDIFF(HOUR, updatedAt, NOW()) as hours_ago
                FROM FactOrders 
                WHERE orderID >= 6000000
                ORDER BY updatedAt DESC 
                LIMIT 20
            """)
            
            orders = []
            for row in cursor.fetchall():
                orders.append({
                    'orderID': row[0],
                    'deliveryDate': str(row[1]),
                    'updatedAt': str(row[2]),
                    'hours_ago': row[3]
                })
            
            cursor.close()
            
            return jsonify({
                'orders': orders,
                'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/debug/order-times/<int:order_id>', methods=['GET'])
def debug_order_times(order_id):
    """Check order timestamps in all nodes"""
    results = {}
    
    for name, pool in [('central', central_pool), ('node2', node2_pool), ('node3', node3_pool)]:
        try:
            with get_connection(pool) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT orderID, createdAt, updatedAt,
                           NOW() as server_time,
                           TIMESTAMPDIFF(MINUTE, updatedAt, NOW()) as minutes_ago
                    FROM FactOrders 
                    WHERE orderID = %s
                """, (order_id,))
                
                row = cursor.fetchone()
                cursor.close()
                
                if row:
                    results[name] = {
                        'exists': True,
                        'createdAt': str(row[1]),
                        'updatedAt': str(row[2]),
                        'server_time_now': str(row[3]),
                        'minutes_ago': row[4]
                    }
                else:
                    results[name] = {'exists': False}
        except Exception as e:
            results[name] = {'error': str(e)}
    
    return jsonify(results)

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
    logger.info("Starting Flask API server...")
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
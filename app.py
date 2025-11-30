from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error, pooling
import logging
from datetime import datetime
from contextlib import contextmanager

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
# REPLICATION FUNCTIONS
# ========================================================================

def replicate_insert_to_central(order_id, delivery_date, level):
    """Replicate insert from partition node to central node"""
    conn = None
    try:
        conn = central_pool.get_connection()
        if not set_isolation_level(conn, level):
            logger.error("Failed to set isolation level for central node")
            return False
            
        cursor = conn.cursor()
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

def replicate_insert_to_partition(order_id, delivery_date, level):
    """Replicate insert from central node to partition node"""
    conn = None
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
        elif year == 2025:
            pool = node3_pool
        else:
            logger.warning(f"No partition node for date {delivery_date}")
            return True
            
        if not pool:
            logger.error("No valid pool found for replication")
            return False
            
        conn = pool.get_connection()
        
        if not set_isolation_level(conn, level):
            logger.error("Failed to set isolation level for partition node")
            return False
            
        cursor = conn.cursor()
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
    """Insert a new order with concurrency detection"""
    conn = None
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        delivery_date = data.get('delivery_date')
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        
        if not order_id or not delivery_date:
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get isolation warnings
        warnings_info = get_isolation_warnings(isolation_level, "insert")
        
        try:
            conn = central_pool.get_connection()
            
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM FactOrders WHERE orderID = %s", (order_id,))
            if cursor.fetchone():
                cursor.close()
                return jsonify({"error": "OrderID already exists"}), 400
            cursor.close()
            
            if not set_isolation_level(conn, isolation_level):
                return jsonify({"error": "Failed to set isolation level"}), 500
                
            cursor = conn.cursor()
            sql = """
            INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
            VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
            """
            cursor.execute(sql, (order_id, delivery_date))
            conn.commit()
            cursor.close()
            logger.info(f"Inserted order {order_id} into central node")
            
            replication_success = replicate_insert_to_partition(order_id, delivery_date, isolation_level)
            
            # Check for concurrency issues
            year = int(delivery_date[:4])
            phantom_warning = check_for_phantom_insert(year, isolation_level)
            
            response = {
                "success": True,
                "message": "Order inserted successfully",
                "order_id": order_id,
                "replication_status": "success" if replication_success else "partial",
                "concurrency_info": {
                    "isolation_warnings": warnings_info,
                    "phantom_warning": phantom_warning
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
    """Read an order from all nodes with concurrency detection"""
    results = {}
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        
        if not order_id:
            return jsonify({"error": "Missing order_id"}), 400
        
        # Get isolation warnings
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
        
        # Get concurrency note
        concurrency_note = get_read_concurrency_note(isolation_level)
        
        return jsonify({
            "success": True,
            "message": "Order retrieved successfully",
            "order_id": order_id,
            "results": results,
            "concurrency_info": {
                "isolation_warnings": warnings_info,
                "concurrency_note": concurrency_note
            }
        }), 200
        
    except ValueError:
        return jsonify({"error": "Invalid order_id format"}), 400
    except Exception as e:
        logger.exception(f"Read failed: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500

@app.route('/api/update', methods=['POST'])
def update_order():
    """Update an order with concurrency detection"""
    conn_central = None
    conn_old_part = None
    conn_new_part = None
    
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        new_delivery_date = data.get('delivery_date')
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        
        if not order_id or not new_delivery_date:
            return jsonify({"error": "Missing required fields"}), 400
        
        # Get isolation warnings
        warnings_info = get_isolation_warnings(isolation_level, "update")
        
        try:
            conn_central = central_pool.get_connection()
            if not set_isolation_level(conn_central, isolation_level):
                return jsonify({"error": "Failed to set isolation level"}), 500
        except Error as e:
            logger.error(f"Failed to connect to central node: {e}")
            return jsonify({"error": "Central node unavailable"}), 503
        
        cursor = conn_central.cursor()
        cursor.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = %s", (order_id,))
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            return jsonify({"error": "OrderID does not exist"}), 404
        
        old_delivery_date = result[0]
        old_year = int(str(old_delivery_date)[:4])
        new_year = int(new_delivery_date[:4])
        
        # Check for dirty read warning
        dirty_read_warning = check_for_dirty_read_uncommitted(order_id, isolation_level)
        
        old_partition = determine_partition_node(old_delivery_date)
        new_partition = determine_partition_node(new_delivery_date)
        
        if old_year != new_year and old_partition and new_partition:
            logger.info(f"Year changed from {old_year} to {new_year}")
            
            try:
                conn_new_part = new_partition.get_connection()
                if set_isolation_level(conn_new_part, isolation_level):
                    cursor = conn_new_part.cursor()
                    sql = """
                    INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                    VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
                    """
                    cursor.execute(sql, (order_id, new_delivery_date))
                    conn_new_part.commit()
                    cursor.close()
            except Error as e:
                logger.error(f"Failed to insert into new partition: {e}")
                if conn_new_part and conn_new_part.is_connected():
                    conn_new_part.rollback()
                return jsonify({"error": "Failed to update partition"}), 500
            
            try:
                conn_old_part = old_partition.get_connection()
                if set_isolation_level(conn_old_part, isolation_level):
                    cursor = conn_old_part.cursor()
                    cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
                    conn_old_part.commit()
                    cursor.close()
            except Error as e:
                logger.error(f"Failed to delete from old partition: {e}")
                if conn_old_part and conn_old_part.is_connected():
                    conn_old_part.rollback()
        
        try:
            cursor = conn_central.cursor()
            sql = """
            UPDATE FactOrders
            SET deliveryDate = %s, updatedAt = NOW()
            WHERE orderID = %s
            """
            cursor.execute(sql, (new_delivery_date, order_id))
            conn_central.commit()
            cursor.close()
        except Error as e:
            logger.error(f"Failed to update central node: {e}")
            conn_central.rollback()
            return jsonify({"error": "Failed to update order"}), 500
        
        if old_year == new_year and new_partition:
            try:
                conn_part = new_partition.get_connection()
                if set_isolation_level(conn_part, isolation_level):
                    cursor = conn_part.cursor()
                    sql = """
                    UPDATE FactOrders
                    SET deliveryDate = %s, updatedAt = NOW()
                    WHERE orderID = %s
                    """
                    cursor.execute(sql, (new_delivery_date, order_id))
                    conn_part.commit()
                    cursor.close()
            except Error as e:
                logger.error(f"Failed to update partition node: {e}")
                if conn_part and conn_part.is_connected():
                    conn_part.rollback()
        
        # Check for non-repeatable read warning
        non_repeatable_warning = check_for_non_repeatable_read_update(
            order_id, old_delivery_date, new_delivery_date, isolation_level
        )
        
        return jsonify({
            "success": True,
            "message": "Order updated successfully",
            "order_id": order_id,
            "old_date": str(old_delivery_date),
            "new_date": new_delivery_date,
            "concurrency_info": {
                "isolation_warnings": warnings_info,
                "dirty_read_warning": dirty_read_warning,
                "non_repeatable_warning": non_repeatable_warning
            }
        }), 200
        
    except ValueError:
        return jsonify({"error": "Invalid input format"}), 400
    except Exception as e:
        logger.exception(f"Update failed: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500
    finally:
        for conn in [conn_central, conn_old_part, conn_new_part]:
            if conn and conn.is_connected():
                conn.close()

@app.route('/api/delete', methods=['POST'])
def delete_order():
    """Delete an order from all nodes with concurrency detection"""
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        
        if not order_id:
            return jsonify({"error": "Missing order_id"}), 400
        
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
        
        # Get concurrency note
        concurrency_note = get_delete_concurrency_note(isolation_level)
        
        return jsonify({
            "success": True,
            "message": "Order deleted successfully",
            "order_id": order_id,
            "deletion_results": deletion_results,
            "concurrency_info": {
                "concurrency_note": concurrency_note
            }
        }), 200
        
    except ValueError:
        return jsonify({"error": "Invalid order_id format"}), 400
    except Exception as e:
        logger.exception(f"Delete failed: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500

# ========================================================================
# CONCURRENCY TEST ENDPOINTS
# ========================================================================

@app.route('/api/test/dirty-read', methods=['POST'])
def test_dirty_read():
    """Endpoint for testing dirty reads"""
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        isolation_level = data.get('isolation_level', 'READ UNCOMMITTED')
        step = data.get('step', 'first')  # 'first' or 'second'
        
        if not central_pool:
            return jsonify({"error": "Central node unavailable"}), 503
        
        with get_connection(central_pool) as conn:
            set_isolation_level(conn, isolation_level)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT deliveryDate, updatedAt FROM FactOrders WHERE orderID = %s", (order_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if not result:
                return jsonify({"error": f"Order {order_id} not found"}), 404
            
            for key, value in result.items():
                if hasattr(value, 'isoformat'):
                    result[key] = value.isoformat()
            
            return jsonify({
                "success": True,
                "step": step,
                "order_id": order_id,
                "data": result,
                "isolation_level": isolation_level,
                "instructions": "Update this order in another session WITHOUT committing, then read again"
            }), 200
            
    except Exception as e:
        logger.exception(f"Dirty read test failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/test/non-repeatable-read', methods=['POST'])
def test_non_repeatable_read():
    """Endpoint for testing non-repeatable reads"""
    try:
        data = request.json
        order_id = int(data.get('order_id'))
        isolation_level = data.get('isolation_level', 'READ COMMITTED')
        action = data.get('action', 'start')  # 'start', 'read', or 'end'
        
        if not central_pool:
            return jsonify({"error": "Central node unavailable"}), 503
        
        with get_connection(central_pool) as conn:
            set_isolation_level(conn, isolation_level)
            
            if action == 'start':
                conn.start_transaction()
            
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT deliveryDate, updatedAt FROM FactOrders WHERE orderID = %s", (order_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if action == 'end':
                conn.rollback()
            
            if not result:
                return jsonify({"error": f"Order {order_id} not found"}), 404
            
            for key, value in result.items():
                if hasattr(value, 'isoformat'):
                    result[key] = value.isoformat()
            
            return jsonify({
                "success": True,
                "action": action,
                "order_id": order_id,
                "data": result,
                "isolation_level": isolation_level,
                "instructions": "UPDATE and COMMIT this order in another session between reads"
            }), 200
            
    except Exception as e:
        logger.exception(f"Non-repeatable read test failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/test/phantom-read', methods=['POST'])
def test_phantom_read():
    """Endpoint for testing phantom reads"""
    try:
        data = request.json
        year = int(data.get('year', 2024))
        isolation_level = data.get('isolation_level', 'REPEATABLE READ')
        action = data.get('action', 'start')  # 'start', 'read', or 'end'
        
        if not central_pool:
            return jsonify({"error": "Central node unavailable"}), 503
        
        with get_connection(central_pool) as conn:
            set_isolation_level(conn, isolation_level)
            
            if action == 'start':
                conn.start_transaction()
            
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT COUNT(*) as count FROM FactOrders WHERE YEAR(deliveryDate) = %s",
                (year,)
            )
            count_result = cursor.fetchone()
            
            cursor.execute(
                "SELECT orderID FROM FactOrders WHERE YEAR(deliveryDate) = %s ORDER BY orderID LIMIT 5",
                (year,)
            )
            sample_ids = [row['orderID'] for row in cursor.fetchall()]
            cursor.close()
            
            if action == 'end':
                conn.rollback()
            
            return jsonify({
                "success": True,
                "action": action,
                "year": year,
                "count": count_result['count'],
                "sample_ids": sample_ids,
                "isolation_level": isolation_level,
                "instructions": f"INSERT and COMMIT a new order in year {year} in another session between reads"
            }), 200
            
    except Exception as e:
        logger.exception(f"Phantom read test failed: {e}")
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

# ========================================================================
# MAIN
# ========================================================================

if __name__ == '__main__':
    logger.info("Starting Flask API server with concurrency detection...")
    logger.info(f"Central Node: {'Connected' if check_pool_health(central_pool) else 'Disconnected'}")
    logger.info(f"Node 2: {'Connected' if check_pool_health(node2_pool) else 'Disconnected'}")
    logger.info(f"Node 3: {'Connected' if check_pool_health(node3_pool) else 'Disconnected'}")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
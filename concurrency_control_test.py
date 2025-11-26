import mysql.connector
from mysql.connector import Error
import threading
import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========================================================================
# CONNECTION SETUP
# ========================================================================

def connect_node(host, user, password, database, port=3306):
    """Create a connection to a database node"""
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            autocommit=False  # Important for transaction testing
        )
        return conn
    except Error as e:
        logger.error(f"Cannot connect to {host}: {e}")
        return None


def get_node_connections():
    """Get fresh connections for all three nodes"""
    central = connect_node(
        host="10.2.14.120",
        user="stadvdb",
        password="Password123!",
        database="stadvdb_node1"
    )
    node2 = connect_node(
        host="10.2.14.121",
        user="stadvdb",
        password="Password123!",
        database="stadvdb_node2"
    )
    node3 = connect_node(
        host="10.2.14.122",
        user="stadvdb",
        password="Password123!",
        database="stadvdb_node3"
    )
    return central, node2, node3


def close_connections(connections):
    """Close all connections safely"""
    for conn in connections:
        if conn and conn.is_connected():
            try:
                conn.close()
            except:
                pass


def set_isolation_level(conn, level):
    """Set transaction isolation level"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level};")
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Failed to set isolation level: {e}")
        return False


# ========================================================================
# TEST DATA SETUP
# ========================================================================

def setup_test_data():
    """Initialize test data in the database"""
    central, node2, node3 = get_node_connections()
    
    try:
        # Clear existing test data
        test_order_ids = [1000001, 1000002, 1000003, 1000004, 1000005]
        
        for node in [central, node2, node3]:
            if node:
                cursor = node.cursor()
                for order_id in test_order_ids:
                    cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
                node.commit()
                cursor.close()
        
        # Insert test orders
        test_orders = [
            (1000001, '2024-01-15'),  # Node2 partition
            (1000002, '2024-06-20'),  # Node2 partition
            (1000003, '2025-03-10'),  # Node3 partition
        ]
        
        for order_id, delivery_date in test_orders:
            # Insert into central
            if central:
                cursor = central.cursor()
                sql = """
                INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, 1, %s, 1, NOW(), NOW(), 1, 1)
                """
                cursor.execute(sql, (order_id, delivery_date))
                central.commit()
                cursor.close()
            
            # Insert into appropriate partition
            year = int(delivery_date[:4])
            partition_node = node2 if year == 2024 else node3
            
            if partition_node:
                cursor = partition_node.cursor()
                sql = """
                INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, 1, %s, 1, NOW(), NOW(), 1, 1)
                """
                cursor.execute(sql, (order_id, delivery_date))
                partition_node.commit()
                cursor.close()
        
        logger.info("Test data setup complete")
        return True
        
    except Exception as e:
        logger.error(f"Failed to setup test data: {e}")
        return False
    finally:
        close_connections([central, node2, node3])


def cleanup_test_data():
    """Remove test data from the database"""
    central, node2, node3 = get_node_connections()
    
    try:
        test_order_ids = [1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007, 1000008]
        
        for node in [central, node2, node3]:
            if node:
                cursor = node.cursor()
                for order_id in test_order_ids:
                    cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
                node.commit()
                cursor.close()
        
        logger.info("Test data cleanup complete")
        
    except Exception as e:
        logger.error(f"Failed to cleanup test data: {e}")
    finally:
        close_connections([central, node2, node3])


# ========================================================================
# SCENARIO 1: CONCURRENT READS ON SAME DATA
# ========================================================================

def concurrent_read_transaction(node_host, node_name, order_id, isolation_level, delay=0):
    """Perform a read transaction on a specific node"""
    conn = connect_node(node_host, "stadvdb", "Password123!", 
                        f"stadvdb_node{node_name[-1]}" if node_name != "Central" else "stadvdb_node1")
    
    if not conn:
        logger.error(f"{node_name}: Failed to connect")
        return None
    
    try:
        set_isolation_level(conn, isolation_level)
        cursor = conn.cursor()
        
        # Start transaction
        conn.start_transaction()
        logger.info(f"{node_name}: Transaction started - Reading order {order_id}")
        
        # Simulate processing time
        time.sleep(delay)
        
        # Read data
        cursor.execute("SELECT * FROM FactOrders WHERE orderID = %s", (order_id,))
        result = cursor.fetchone()
        
        logger.info(f"{node_name}: Read result = {result}")
        
        # Simulate more processing
        time.sleep(1)
        
        conn.commit()
        logger.info(f"{node_name}: Transaction committed")
        
        cursor.close()
        return result
        
    except Exception as e:
        logger.error(f"{node_name}: Error during read - {e}")
        try:
            conn.rollback()
        except:
            pass
        return None
    finally:
        if conn:
            conn.close()


def test_scenario_1_concurrent_reads(isolation_level="READ COMMITTED"):
    """
    Test Scenario 1: Concurrent transactions in two or more nodes reading the same data item.
    Expected: All reads should succeed and return consistent data.
    """
    logger.info("="*70)
    logger.info("SCENARIO 1: CONCURRENT READS ON SAME DATA ITEM")
    logger.info(f"Isolation Level: {isolation_level}")
    logger.info("="*70)
    
    order_id = 1000001
    nodes = [
        ("10.2.14.120", "Central", 0),
        ("10.2.14.121", "Node2", 0.5),
        ("10.2.14.122", "Node3", 1.0),
    ]
    
    threads = []
    results = []
    
    # Create threads for concurrent reads
    for host, name, delay in nodes:
        thread = threading.Thread(
            target=lambda h, n, d: results.append(concurrent_read_transaction(h, n, order_id, isolation_level, d)),
            args=(host, name, delay),
            name=f"Read-{name}"
        )
        threads.append(thread)
    
    # Start all threads
    for thread in threads:
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    logger.info("-"*70)
    logger.info(f"Results collected: {len([r for r in results if r is not None])} successful reads")
    logger.info("="*70 + "\n")


# ========================================================================
# SCENARIO 2: ONE WRITE + MULTIPLE READS ON SAME DATA
# ========================================================================

def concurrent_write_transaction(node_host, node_name, order_id, new_date, isolation_level, delay=0):
    """Perform an update transaction on a specific node"""
    conn = connect_node(node_host, "stadvdb", "Password123!", 
                        f"stadvdb_node{node_name[-1]}" if node_name != "Central" else "stadvdb_node1")
    
    if not conn:
        logger.error(f"{node_name}: Failed to connect")
        return False
    
    try:
        set_isolation_level(conn, isolation_level)
        cursor = conn.cursor()
        
        # Start transaction
        conn.start_transaction()
        logger.info(f"{node_name}: Transaction started - Updating order {order_id}")
        
        # Simulate processing time
        time.sleep(delay)
        
        # Update data
        sql = """
        UPDATE FactOrders
        SET deliveryDate = %s, updatedAt = NOW()
        WHERE orderID = %s
        """
        cursor.execute(sql, (new_date, order_id))
        
        logger.info(f"{node_name}: Updated order {order_id} to date {new_date}, rows affected: {cursor.rowcount}")
        
        # Simulate more processing before commit
        time.sleep(2)
        
        conn.commit()
        logger.info(f"{node_name}: Transaction committed")
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"{node_name}: Error during update - {e}")
        try:
            conn.rollback()
        except:
            pass
        return False
    finally:
        if conn:
            conn.close()


def test_scenario_2_write_and_reads(isolation_level="READ COMMITTED"):
    """
    Test Scenario 2: One transaction writing and others reading the same data item.
    Expected: Behavior depends on isolation level (dirty reads, etc.)
    """
    logger.info("="*70)
    logger.info("SCENARIO 2: ONE WRITE + CONCURRENT READS ON SAME DATA ITEM")
    logger.info(f"Isolation Level: {isolation_level}")
    logger.info("="*70)
    
    order_id = 1000002
    new_date = '2024-12-31'
    
    # Writer thread
    writer_thread = threading.Thread(
        target=concurrent_write_transaction,
        args=("10.2.14.120", "Central", order_id, new_date, isolation_level, 0),
        name="Writer-Central"
    )
    
    # Reader threads (starting slightly after writer)
    reader_threads = [
        threading.Thread(
            target=concurrent_read_transaction,
            args=("10.2.14.121", "Node2", order_id, isolation_level, 0.5),
            name="Reader-Node2"
        ),
        threading.Thread(
            target=concurrent_read_transaction,
            args=("10.2.14.122", "Node3", order_id, isolation_level, 1.0),
            name="Reader-Node3"
        )
    ]
    
    # Start all threads
    writer_thread.start()
    
    for thread in reader_threads:
        thread.start()
    
    # Wait for all threads to complete
    writer_thread.join()
    for thread in reader_threads:
        thread.join()
    
    logger.info("-"*70)
    logger.info("="*70 + "\n")


# ========================================================================
# SCENARIO 3: CONCURRENT WRITES ON SAME DATA
# ========================================================================

def concurrent_delete_transaction(node_host, node_name, order_id, isolation_level, delay=0):
    """Perform a delete transaction on a specific node"""
    conn = connect_node(node_host, "stadvdb", "Password123!", 
                        f"stadvdb_node{node_name[-1]}" if node_name != "Central" else "stadvdb_node1")
    
    if not conn:
        logger.error(f"{node_name}: Failed to connect")
        return False
    
    try:
        set_isolation_level(conn, isolation_level)
        cursor = conn.cursor()
        
        # Start transaction
        conn.start_transaction()
        logger.info(f"{node_name}: Transaction started - Deleting order {order_id}")
        
        # Simulate processing time
        time.sleep(delay)
        
        # Delete data
        cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
        
        logger.info(f"{node_name}: Deleted order {order_id}, rows affected: {cursor.rowcount}")
        
        # Simulate more processing before commit
        time.sleep(2)
        
        conn.commit()
        logger.info(f"{node_name}: Transaction committed")
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"{node_name}: Error during delete - {e}")
        try:
            conn.rollback()
        except:
            pass
        return False
    finally:
        if conn:
            conn.close()


def test_scenario_3_concurrent_writes(isolation_level="READ COMMITTED"):
    """
    Test Scenario 3: Concurrent transactions writing to the same data item.
    Expected: Depending on isolation level, may see locks, deadlocks, or lost updates.
    """
    logger.info("="*70)
    logger.info("SCENARIO 3: CONCURRENT WRITES ON SAME DATA ITEM")
    logger.info(f"Isolation Level: {isolation_level}")
    logger.info("="*70)
    
    order_id = 1000003
    
    # Multiple writers updating the same record
    writer_threads = [
        threading.Thread(
            target=concurrent_write_transaction,
            args=("10.2.14.120", "Central", order_id, '2025-06-15', isolation_level, 0),
            name="Writer1-Central"
        ),
        threading.Thread(
            target=concurrent_write_transaction,
            args=("10.2.14.122", "Node3", order_id, '2025-08-20', isolation_level, 0.5),
            name="Writer2-Node3"
        ),
    ]
    
    # Start all threads
    for thread in writer_threads:
        thread.start()
    
    # Wait for all threads to complete
    for thread in writer_threads:
        thread.join()
    
    logger.info("-"*70)
    
    # Check final state
    central, node2, node3 = get_node_connections()
    try:
        for node, name in [(central, "Central"), (node3, "Node3")]:
            if node:
                cursor = node.cursor()
                cursor.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = %s", (order_id,))
                result = cursor.fetchone()
                if result:
                    logger.info(f"{name}: Final deliveryDate = {result[0]}")
                else:
                    logger.info(f"{name}: Record not found")
                cursor.close()
    finally:
        close_connections([central, node2, node3])
    
    logger.info("="*70 + "\n")


# ========================================================================
# MAIN TEST RUNNER
# ========================================================================

def run_all_tests():
    """Run all test scenarios with different isolation levels"""
    
    isolation_levels = [
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE"
    ]
    
    print("\n" + "="*70)
    print("DISTRIBUTED DATABASE CONCURRENCY TEST SUITE")
    print("="*70 + "\n")
    
    # Setup test data
    logger.info("Setting up test data...")
    if not setup_test_data():
        logger.error("Failed to setup test data. Aborting tests.")
        return
    
    time.sleep(2)  # Give database time to stabilize
    
    # Run tests for each isolation level
    for isolation_level in isolation_levels:
        logger.info(f"\n{'#'*70}")
        logger.info(f"TESTING WITH ISOLATION LEVEL: {isolation_level}")
        logger.info(f"{'#'*70}\n")
        
        # Scenario 1: Concurrent Reads
        test_scenario_1_concurrent_reads(isolation_level)
        time.sleep(2)
        
        # Scenario 2: One Write + Multiple Reads
        test_scenario_2_write_and_reads(isolation_level)
        time.sleep(2)
        
        # Scenario 3: Concurrent Writes
        test_scenario_3_concurrent_writes(isolation_level)
        time.sleep(2)
    
    # Cleanup
    logger.info("Cleaning up test data...")
    cleanup_test_data()
    
    print("\n" + "="*70)
    print("ALL TESTS COMPLETED")
    print("="*70 + "\n")


def run_single_test(scenario_number, isolation_level="READ COMMITTED"):
    """Run a single test scenario"""
    
    print("\n" + "="*70)
    print(f"RUNNING SCENARIO {scenario_number} WITH {isolation_level}")
    print("="*70 + "\n")
    
    # Setup test data
    logger.info("Setting up test data...")
    setup_test_data()
    time.sleep(1)
    
    if scenario_number == 1:
        test_scenario_1_concurrent_reads(isolation_level)
    elif scenario_number == 2:
        test_scenario_2_write_and_reads(isolation_level)
    elif scenario_number == 3:
        test_scenario_3_concurrent_writes(isolation_level)
    else:
        logger.error("Invalid scenario number")
    
    print("\n" + "="*70)
    print("TEST COMPLETED")
    print("="*70 + "\n")


# ========================================================================
# INTERACTIVE MENU
# ========================================================================

def menu():
    """Interactive menu for running tests"""
    
    while True:
        print("\n" + "="*70)
        print("DISTRIBUTED DATABASE CONCURRENCY TEST MENU")
        print("="*70)
        print("\n1. Run All Tests (All Scenarios, All Isolation Levels)")
        print("2. Run Scenario 1: Concurrent Reads")
        print("3. Run Scenario 2: One Write + Multiple Reads")
        print("4. Run Scenario 3: Concurrent Writes (Update)")
        print("5. Setup Test Data")
        print("6. Cleanup Test Data")
        print("7. Exit")
        
        choice = input("\nEnter your choice (1-7): ").strip()
        
        if choice == '1':
            run_all_tests()
        
        elif choice in ['2', '3', '4']:
            print("\nSelect Isolation Level:")
            print("1. READ UNCOMMITTED")
            print("2. READ COMMITTED")
            print("3. REPEATABLE READ")
            print("4. SERIALIZABLE")
            
            iso_choice = input("Enter choice (1-4): ").strip()
            iso_map = {
                '1': 'READ UNCOMMITTED',
                '2': 'READ COMMITTED',
                '3': 'REPEATABLE READ',
                '4': 'SERIALIZABLE'
            }
            
            if iso_choice in iso_map:
                isolation_level = iso_map[iso_choice]
                scenario_num = int(choice)
                run_single_test(scenario_num, isolation_level)
            else:
                print("Invalid isolation level choice")
        
        elif choice == '5':
            setup_test_data()
            print("Test data setup complete")
        
        elif choice == '6':
            cleanup_test_data()
            print("Test data cleanup complete")
        
        elif choice == '7':
            print("Exiting...")
            break
        
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    menu()
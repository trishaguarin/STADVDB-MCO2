import mysql.connector
import threading
import time
import logging
from mysql.connector import Error

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_node(host, user, password, database, port=3306):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        return conn
    except Error as e:
        print(f"[ERROR] Cannot connect: {e}")
        return None

def set_isolation_level(conn, level):
    """Set transaction isolation level"""
    cursor = conn.cursor()
    cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
    cursor.close()
    
# set up test data
NODE_HOSTS = [
    ("10.2.14.120","central"),
    ("10.2.14.121","node2"),
    ("10.2.14.122","node3")
]

DB_MAP = {
    "central": "stadvdb_node1",
    "node2": "stadvdb_node2",
    "node3": "stadvdb_node3"
}

def set_up_test_data():
    for host, name in NODE_HOSTS:
        conn = connect_node(node_host, "stadvdb", "Password123!", DB_MAP[node_name])
        if not conn: 
            logger.error(f"{name}: connection failed for setup")
            continue
        cur = conn.cursor()
        cur.execute("DELETE FROM FactOrders WHERE orderID = 999999")
        cur.execute("""
            INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
            VALUES (999999, 1, '2024-09-23', 1, NOW(), NOW(), 1, 1)
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"{name}: test row inserted")
    
def read_transaction(node_host, node_name, orderID, isolation_level, delay=0):
    # read order from a node
    conn = connect_node(node_host, "stadvdb", "Password123!", DB_MAP[node_name])

    if not conn:
        logger.error(f"{node_name}: Connection failed")
        return None

    try:
        set_isolation_level(conn, isolation_level)
        cursor = conn.cursor()
        conn.start_transaction()

        logger.info(f"{node_name}: Reading order {orderID}")
        time.sleep(delay)

        cursor.execute("SELECT * FROM FactOrders WHERE orderID = %s", (orderID,))
        result = cursor.fetchone()

        logger.info(f"{node_name}: Retrieved result")
        time.sleep(1)
        
        conn.commit()
        logger.info(f"{node_name}: Committed")
        
        cursor.close()
        return result
    
    except Exception as e:
        logger.error(f"{node_name}: Error {e}")
        conn.rollback()
        return None
    
    finally:
        conn.close()

def update_transaction(node_host, node_name, orderID, new_date, isolation_level, delay=0):
    #update an order's delivery date

    conn = connect_node(node_host, "stadvdb", "Password123!", DB_MAP[node_name])

    if not conn:
        logger.error(f"{node_name}: Connection failed")
        return False
    
    try:
        set_isolation_level(conn, isolation_level)
        cursor = conn.cursor()
        conn.start_transaction()

        logger.info(f"{node_name}: Starting UPDATE transaction for order {orderID}...")
        time.sleep(delay)

        cursor.execute("""
            UPDATE FactOrders
            SET deliveryDate = %s, updatedAt = NOW()
            WHERE orderID = %s
        """, (new_date, orderID))

        rows_affected = cursor.rowcount
        logger.info(f"{node_name}: Updated to {new_date}, rows affected: {rows_affected}")

        time.sleep(2)

        conn.commit()
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"{node_name}: Error {e}")
        conn.rollback()
        return False

    finally:
        conn.close()

# CASE 1: CONCURRENT TRANSACTIONS IN TWO OR MORE NODES ARE READING THE SAME ITEM
def test_concurrent_reads(isolation_level):
    print("================================================")
    print("TESTING CASE 1: CONCURRENT READS TEST")
    print(f"Isolation Level: {isolation_level}")
    print("================================================")

    order_id = 999999

    # create 3 threads
    threads = [
        threading.Thread(target=read_transaction, args=("10.2.14.120", "Reader1-Central", order_id, isolation_level, 0)),
        threading.Thread(target=read_transaction, args=("10.2.14.121", "Reader2-Node2", order_id, isolation_level, 0.5)),
        threading.Thread(target=read_transaction, args=("10.2.14.122", "Reader3-Node3", order_id, isolation_level, 1.0))
    ]

    # start all threads
    for t in threads:
        t.start()

    # wait for all to finish
    for t in threads:
        t.join()
    
    print("\nAll concurrent reads completed")

def test_write_and_reads(isolation_level):
    print("================================================")
    print("TESTING CASE 2: ONE WRITE + CONCURRENT READS")
    print(f"Isolation Level: {isolation_level}")
    print("================================================")

    orderID = 999999
    new_date = '2024-12-31'
    
    writer = threading.Thread(
        target=update_transaction,
        args=("10.2.14.120", "Writer-Central", orderID, new_date, isolation_level, 0),
        name="Writer-Central"
    )

    readers = [
        threading.Thread(
            target=read_transaction,
            args=("10.2.14.120", "Reader1-Central", orderID, isolation_level, 0.3),  # Added
            name="Reader1-Central"
        ),
        threading.Thread(
            target=read_transaction,
            args=("10.2.14.121", "Reader2-Node2", orderID, isolation_level, 0.6),
            name="Reader2-Node2"
        ),
        threading.Thread(
            target=read_transaction,
            args=("10.2.14.122", "Reader3-Node3", orderID, isolation_level, 1.0),
            name="Reader3-Node3"
        )
    ]

    writer.start()
    for r in readers:
        r.start()

    writer.join()
    for r in readers:
        r.join()

    print("\nWrite and read test completed")

def test_concurrent_writes(isolation_level):
    print("\n================================================")
    print("TESTING CASE 3: CONCURRENT WRITES")
    print(f"Isolation Level: {isolation_level}")
    print("================================================")

    orderID = 999999

    writers = [
        threading.Thread(
            target=update_transaction,
            args=("10.2.14.120", "Writer1-Central", orderID, '2024-06-15', isolation_level, 0),
            name="Writer1-Central"
        ),
        threading.Thread(
            target=update_transaction,
            args=("10.2.14.121", "Writer2-Node2", orderID, '2024-08-20', isolation_level, 0.5),
            name="Writer2-Node2"
        )
    ]

    for w in writers:
        w.start()

    for w in writers:
        w.join()

    print("\nConcurrent writes completed")

    # check final state on both nodes
    print("\nChecking final state on all nodes:")
    for host, name in [("10.2.14.120", "Central"), ("10.2.14.121", "Node2")]:
        conn = connect_node(node_host, "stadvdb", "Password123!", DB_MAP[node_name])
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = 999999")
            result = cursor.fetchone()
            if result:
                print(f"  {name}: Final delivery date = {result[0]}")
            else:
                print(f"  {name}: Record not found")
            cursor.close()
            conn.close()

def menu():
    isolation_levels = {
        '1': 'READ UNCOMMITTED',
        '2': 'READ COMMITTED',
        '3': 'REPEATABLE READ',
        '4': 'SERIALIZABLE'
    }

    while True:
        print("*******************************")
        print("CONCURRENCY TEST MENU")
        print("*******************************")
        print("\nSelect Isolation Level:")
        print("1. READ UNCOMMITTED")
        print("2. READ COMMITTED")
        print("3. REPEATABLE READ")
        print("4. SERIALIZABLE")
        print("5. Exit")

        choice = input("\nChoose option (1-5): ").strip()

        if choice == '5':
            print("Exiting...")
            break

        if choice not in isolation_levels:
            print("Invalid choice. Please try again.")
            continue

        isolation_level = isolation_levels[choice]
        print(f"\nUsing isolation level: {isolation_level}")

        # setup test data
        print("\nSetting up test data...")
        set_up_test_data()
        time.sleep(1)

        # run all tests with selected isolation level
        print("\n*******************************")
        print(f"RUNNING ALL TEST CASES WITH {isolation_level}")
        print("*******************************")

        for run in range(1, 4):
            print(f"[RUN {run}/3]")
            test_concurrent_reads(isolation_level)
            time.sleep(2)
            
            test_write_and_reads(isolation_level)
            time.sleep(2)
            
            test_concurrent_writes(isolation_level)
            time.sleep(2)

        print("\n*******************************")
        print("ALL TESTS COMPLETED")
        print("*******************************")

        again = input("\nRun tests with another isolation level? (y/n): ").strip().lower()
        if again != 'y':
            print("Exiting...")
            break

if __name__ == "__main__":
    menu()

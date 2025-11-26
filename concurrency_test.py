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
    
# set up test data
def set_up_test_data():
    conn = connect_node("10.2.14.121", "stadvdb", "Password123!", "stadvdb_node1")

    if conn:
        cursor = conn.cursor()

        # delete if the order exists
        cursor.execute("DELETE FROM FactOrders where orderId = 999999")

        # insert test order
        cursor.execute("""
            INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
            VALUES (999999, 1, '2024-09-23', 1, NOW(), NOW(), 1, 1) 
        """)

        cursor.commit()

        cursor.execute("SELECT * FROM FactOrders WHERE orderID = 999999")
        test_order = cursor.fetchone()

        print(f"Inserted test order: {test_order}")
        
        cursor.close()
        conn.close()

        return test_order
    
def read_transaction(node_host, node_name, orderID, delay=0):
    # read order from a node
    conn = connect_node("10.2.14.121", "stadvdb", "Password123!", "stadvdb_node1")

    if not conn:
        logger.error("Connection failed.")

    try:
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

# CASE 1: CONCURRENT TRANSACTIONS IN TWO OR MORE NODES ARE READING THE SAME ITEM
def test_concurrent_reads():
    print("================================================")
    print("TESTING CASE 1: CONCURRENT READS TEST")
    print("================================================")

    order_id = 999999

    # create 3 threads
    threads = [
        threading.Thread(target=read_transaction, args=("10.2.14.120", "Reader1", order_id, 0)),
        threading.Thread(target=read_transaction, args=("10.2.14.120", "Reader2", order_id, 0.5)),
        threading.Thread(target=read_transaction, args=("10.2.14.120", "Reader3", order_id, 1.0))
    ]

    # start all threads
    for t in threads:
        t.start()

    # wait for all to finish
    for t in threads:
        t.join()
    
    print("\nAll concurrent reads completed")

if __name__ == "__main__":
    set_up_test_data()
    time.sleep(1)
    test_concurrent_reads()
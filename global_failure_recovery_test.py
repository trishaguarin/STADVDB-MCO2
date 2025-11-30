import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# for tracking when nodes go down
downtime_tracker = {
    'Central': None,
    'Node2': None,
    'Node3': None
}

# to connect the nodes
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
    
def check_connection(conn):
    try:
        if conn and conn.is_connected():
            return True
        return False
    except:
        return False

central_node = connect_node(
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

def cleanup_test_orders():
    """Delete test orders from all nodes"""
    
    # list of order ids to test
    test_order_ids = [999990, 999991, 999992, 999993]
    
    nodes = [
        ("10.2.14.120", "stadvdb", "Password123!", "stadvdb_node1", "Central"),
        ("10.2.14.121", "stadvdb", "Password123!", "stadvdb_node2", "Node2"),
        ("10.2.14.122", "stadvdb", "Password123!", "stadvdb_node3", "Node3")
    ]
    
    print("Cleaning up test orders from all nodes...")
    
    for host, user, password, database, node_name in nodes:
        conn = connect_node(host, user, password, database)
        
        if conn:
            cursor = conn.cursor()
            
            for order_id in test_order_ids:
                cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (order_id,))
            
            conn.commit()
            print(f"Cleaned {node_name}")
            
            cursor.close()
            conn.close()
        else:
            print(f"Could not connect to {node_name}")
    
    print("Cleanup complete!\n")

    
def check_order_location(orderID):
    """Check which nodes contain an order""" 
    print(f"Checking location of order {orderID}...")
    
    nodes = [
        ("10.2.14.120", "stadvdb_node1", "Central"),
        ("10.2.14.121", "stadvdb_node2", "Node2"),
        ("10.2.14.122", "stadvdb_node3", "Node3")
    ]
    
    for host, database, node_name in nodes:
        conn = connect_node(host, "stadvdb", "Password123!", database)
        
        if conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM FactOrders WHERE orderID = %s", (orderID,))
            count = cursor.fetchone()[0]  # get count
            
            if count > 0:
                print(f"  {node_name}: EXISTS")
            else:
                print(f"  {node_name}: MISSING")
            
            cursor.close()
            conn.close()
        else:
            # connection failed - node is down
            print(f"  {node_name}: UNAVAILABLE (node is down)")

def copy_missing_to_central(start_time, end_time):
    """Recovery function: sync missing transactions from node 2 to central"""
    print(f"\nRecovering orders from {start_time} to {end_time}...")
    
    conn_node2 = connect_node("10.2.14.121", "stadvdb", "Password123!", "stadvdb_node2")
    conn_central = connect_node("10.2.14.120", "stadvdb", "Password123!", "stadvdb_node1")
    
    if not conn_node2:
        print("Error: Cannot connect to Node 2")
        return
    
    if not conn_central:
        print("Error: Cannot connect to Central")
        conn_node2.close()
        return
    
    cursor_node2 = conn_node2.cursor()
    cursor_central = conn_central.cursor()
    
    cursor_node2.execute("""
        SELECT * FROM FactOrders 
        WHERE updatedAt BETWEEN %s AND %s
        """, (start_time, end_time))
    
    orders_during_downtime = cursor_node2.fetchall()
    print(f"Found {len(orders_during_downtime)} orders during downtime")
    
    count_synced = 0
    for order in orders_during_downtime:
        orderID = order[0]
        
        # check if it exists in central
        cursor_central.execute("SELECT COUNT(*) FROM FactOrders WHERE orderID = %s", (orderID,))
        count = cursor_central.fetchone()[0]
        
        if count == 0:
            # insert missing order
            print(f"  -> Inserting order {orderID}...")
            cursor_central.execute("""
                INSERT INTO FactOrders 
                (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, order)
            count_synced += 1
        else:
            # order exists but might be outdated -> update it
            print(f"  -> Updating order {orderID}...")
            cursor_central.execute("""
                UPDATE FactOrders 
                SET userID=%s, deliveryDate=%s, riderID=%s, updatedAt=%s, productID=%s, quantity=%s
                WHERE orderID=%s
            """, (order[1], order[2], order[3], order[5], order[6], order[7], orderID))
            count_synced += 1
    
    conn_central.commit()
    
    cursor_node2.close()
    cursor_central.close()
    conn_node2.close()
    conn_central.close()
        
    print(f"\nRecovery complete: Synced {count_synced} orders to Central")

def copy_missing_to_node2(start_time, end_time):
    """Recovery function: sync missing transactions from central to node 2"""
    print(f"\nRecovering orders from {start_time} to {end_time} to Node 2...")
    
    conn_central = connect_node("10.2.14.120", "stadvdb", "Password123!", "stadvdb_node1")
    conn_node2 = connect_node("10.2.14.121", "stadvdb", "Password123!", "stadvdb_node2")
    
    if not conn_central:
        print("Error: Cannot connect to Central")
        return
    
    if not conn_node2:
        print("Error: Cannot connect to Node 2")
        conn_central.close()
        return
    
    cursor_central = conn_central.cursor()
    cursor_node2 = conn_node2.cursor()
    
    # orders from central that should be in node 2
    cursor_central.execute("""
        SELECT * FROM FactOrders 
        WHERE updatedAt BETWEEN %s AND %s 
        AND YEAR(deliveryDate) = 2024
    """, (start_time, end_time))
    
    orders_during_downtime = cursor_central.fetchall()
    print(f"Found {len(orders_during_downtime)} 2024 orders during downtime")
    
    count_synced = 0
    for order in orders_during_downtime:
        orderID = order[0]
        delivery_date = order[2]
        
        # only process 2024 orders
        if str(delivery_date).startswith('2024'):
            cursor_node2.execute("SELECT COUNT(*) FROM FactOrders WHERE orderID = %s", (orderID,))
            count = cursor_node2.fetchone()[0]
            
            if count == 0:
                # insert the missing order
                print(f"  -> Inserting order {orderID} to Node 2...")
                cursor_node2.execute("""
                    INSERT INTO FactOrders 
                    (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, order)
                count_synced += 1
            else:
                # update if outdated
                print(f"  -> Updating order {orderID} in Node 2...")
                cursor_node2.execute("""
                    UPDATE FactOrders 
                    SET userID=%s, deliveryDate=%s, riderID=%s, updatedAt=%s, productID=%s, quantity=%s
                    WHERE orderID=%s
                """, (order[1], order[2], order[3], order[5], order[6], order[7], orderID))
                count_synced += 1
    
    conn_node2.commit()
    
    cursor_central.close()
    cursor_node2.close()
    conn_central.close()
    conn_node2.close()
        
    print(f"\nRecovery complete: Synced {count_synced} orders to Node 2")
    
def test_case1():
    """
    Case 1: When attempting to replicate the transaction from Node 2 or Node 3 to the central node, 
    the transaction fails in writing (insert / update) to the central node.
    """
    
    orderID = 999990
    delivery_date = '2024-12-31'
    
    print("===========================================")
    print("CASE 1: REPLICATION FROM NODE 2 TO CENTRAL")
    print("===========================================")
    
    print("\nStep 1: Inserting order on Node 2...")
    conn = connect_node("10.2.14.121", "stadvdb", "Password123!", "stadvdb_node2")
    
    if not conn:
        print("Error: Cannot connect to Node 2")
        return
    
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1) 
    """, (orderID, delivery_date))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Order {orderID} inserted on Node 2")
    
    # replication logic... copy to central
    print("\nStep 2: Attempting to replicate to Central...")
    
    try:
        conn_central = connect_node("10.2.14.120", "stadvdb", "Password123!", "stadvdb_node1")
        
        if conn_central:
            cursor_central = conn_central.cursor()
            cursor_central.execute("""
                INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
            """, (orderID, delivery_date))
            
            conn_central.commit()
            cursor_central.close()
            conn_central.close()
            
            print("Replication to Central SUCCESSFUL")
            
        else:
            # record time when central is down
            downtime_tracker['Central'] = datetime.now()
            print("Replication FAILED - Central unavailable")
            print(f"[DETECTED] Central went down at: {downtime_tracker['Central'].strftime('%Y-%m-%d %H:%M:%S')}")
            
    except Exception as e:
        # record time when central is down
        downtime_tracker['Central'] = datetime.now()
        print(f"Replication FAILED - Error: {e}")
        print(f"[DETECTED] Central went down at: {downtime_tracker['Central'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    # verify the replicated data exists
    print("\nStep 3: Verifying data location...")
    check_order_location(orderID)
        
def test_case2():
    """
    Case 2: The central node eventually recovers from failure (i.e., comes back online) 
    and missed certain write (insert / update) transactions.
    """
    print("===========================================")
    print("CASE 2: Central Node recovers missing data")
    print("===========================================")
    
    orderID = 999990
    
    # check state before recovery
    print("\nBefore recovery:")
    check_order_location(orderID)
    
    # check for recorded downtime
    if downtime_tracker['Central'] is None:
        print("\nNo recorded downtime for Central.")
        print("Using fallback: last 1 hour")
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
    else:
        start_time = downtime_tracker['Central']
        end_time = datetime.now()
        
        print(f"\nUsing detected downtime window:")
        print(f"  Central went DOWN at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Central came UP at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    copy_missing_to_central(start_str, end_str)
    
    # clear after successful recovery
    downtime_tracker['Central'] = None
    
    # check state after recovery
    print("\nAfter recovery:")
    check_order_location(orderID)

def test_case3():
    """
    Case 3: When attempting to replicate the transaction from Central to Node 2 or Node 3, 
    the transaction fails in writing (insert / update) to the partition node.
    """
    orderID = 999991
    delivery_date = '2024-06-15'
    
    print("===========================================")
    print("CASE 3: REPLICATION FROM CENTRAL TO NODE 2")
    print("===========================================")
    
    print("\nStep 1: Inserting order on Central...")
    conn = connect_node("10.2.14.120", "stadvdb", "Password123!", "stadvdb_node1")
    
    if not conn:
        print("Error: Cannot connect to Central")
        return
    
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1) 
    """, (orderID, delivery_date))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Order {orderID} inserted on Central")
    
    # replication logic... copy to node 2
    print("\nStep 2: Attempting to replicate to Node 2...")
    
    try:
        conn_node2 = connect_node("10.2.14.121", "stadvdb", "Password123!", "stadvdb_node2")
        
        if conn_node2:
            cursor_node2 = conn_node2.cursor()
            cursor_node2.execute("""
                INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
            """, (orderID, delivery_date))
            
            conn_node2.commit()
            cursor_node2.close()
            conn_node2.close()
            
            print("Replication to Node 2 SUCCESSFUL")
            
        else:
            # record when node 2 down
            downtime_tracker['Node2'] = datetime.now()
            print("Replication FAILED - Node 2 unavailable")
            print(f"[DETECTED] Node 2 went down at: {downtime_tracker['Node2'].strftime('%Y-%m-%d %H:%M:%S')}")
            
    except Exception as e:
        # record when node 2 down
        downtime_tracker['Node2'] = datetime.now()
        print(f"Replication FAILED - Error: {e}")
        print(f"[DETECTED] Node 2 went down at: {downtime_tracker['Node2'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    # verify data location
    print("\nStep 3: Verifying data location...")
    check_order_location(orderID)
    
if __name__ == "__main__":
    #cleanup_test_orders()
    # TODO: measure time elapsed
    while True:
        print("===========================================")
        print("GLOBAL FAILURE RECOVERY TEST MENU")
        print("===========================================")
        print("\n1. Run Case 1 (Replication Failure - Node 2 to Central)")
        print("2. Run Case 2 (Central Node Recovery)")
        print("3. Run Case 3 (Replication Failure - Central to Node 2)")
        print("4. Run Case 4 (Node 2 Recovery)")
        print("5. Run All Cases")
        print("6. Cleanup Test Data")
        print("7. Exit")
        
        choice = input("\nChoose option (1-7): ").strip()
        
        if choice == '1':
            test_case1()
        elif choice == '2':
            test_case2()
        elif choice == '3':
            test_case3()
        elif choice == '4':
            print("Case 4 not implemented yet")
        elif choice == '5':
            test_case1()
            input("\nPress ENTER to continue to Case 2...")
            test_case2()
            # TODO: case 3 & 4 here
        elif choice == '6':
            cleanup_test_orders()
        elif choice == '7':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

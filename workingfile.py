import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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


# ========================================================================
# 2. ISOLATION LEVEL HANDLING
# ========================================================================
def set_isolation_level(conn, level):

    if not check_connection(conn):
        logger.error("Cannot set isolation level: connection dead")
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level};")
        cursor.close()
        return True
    except:
        logger.error(f"Failed to set isolation level.")
        return False
    
ISOLATION_LEVELS = {
    "1": "READ UNCOMMITTED",
    "2": "READ COMMITTED",
    "3": "REPEATABLE READ",
    "4": "SERIALIZABLE"
    }
    
        
# ========================================================================
# 3. PARTITIONING RULE
# ========================================================================

def determine_node(delivery_date):
    try:
        year = int(str(delivery_date)[:4])
        if year == 2024:
            return node2
        elif year == 2025:
            return node3
        else:
            return central_node
    except:
        logger.error(f"Invalid date format: {delivery_date}")
        return central_node

# ========================================================================
# 4. REPLICATION HELPERS
# ========================================================================

def replicate_to_central(order_id, delivery_date, level):
    if not check_connection(central_node):
        logger.error("Central node unavailable for replication")
        return False

    try:   
        set_isolation_level(central_node, level)
        cursor = central_node.cursor()

        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
        ON DUPLICATE KEY UPDATE 
            deliveryDate = VALUES(deliveryDate),
            updatedAt = NOW()
        """
        logger.info("REPL->CENTRAL SQL: %s -- PARAMS: (%s, %s)", sql.strip(), order_id, delivery_date)
        cursor.execute(sql, (order_id, delivery_date))
        central_node.commit()
        cursor.close()
        logger.info(f"Replicated order {order_id} to central node")
        return True
    except Exception as e:
        logger.error(f"Failed to replicate to central node: {e}")
        try:
            central_node.rollback()
        except:
            pass
        return False
        
def replicate_to_partitions(order_id, delivery_date, level):
    node = determine_node(delivery_date)
    
    if not check_connection(node):
        logger.error("Target partition node unavailable for replication")
        return False

    try:
        set_isolation_level(node, level)
        cursor = node.cursor()

        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
        ON DUPLICATE KEY UPDATE 
            deliveryDate = VALUES(deliveryDate),
            updatedAt = NOW()
        """
        logger.info("REPL->PART SQL: %s -- PARAMS: (%s, %s)", sql.strip(), order_id, delivery_date)
        cursor.execute(sql, (order_id, delivery_date))
        node.commit()
        cursor.close()
        logger.info("Replicated order %s to partition node (deliveryDate=%s)", order_id, delivery_date)
        return True

    except Exception as e:
        logger.error("Failed to replicate to noncentral nodes: {e}")
        try:
            node.rollback()
        except:
            pass
        return False

# ========================================================================
# 5. CRUD OPERATIONS
# ========================================================================

def insert_order(level):
    try:
        orderID = int(input("Input orderID: "))

        orderExists = False
        for label, node in [("Central", central_node), ("Node2", node2), ("Node3", node3)]:
            if check_connection(node):
                cursor = node.cursor()
                cursor.execute("SELECT 1 FROM FactOrders WHERE orderID = %s", (orderID,))
                if cursor.fetchone():
                    orderExists = True
                    cursor.close()
                    break
                cursor.close()

        if orderExists:
            print("Error: OrderID already exists")
            return

        deliveryDate = input("Input Delivery Date (YYYY-MM-DD): ")
    
        node = determine_node(deliveryDate)

        if not check_connection(node):
            print(f"Error: Target node ({label}) unavailable")
            return

        set_isolation_level(node, level)
        cursor = node.cursor()

        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
        """

        cursor.execute (sql, (orderID, deliveryDate))
        node.commit() 
        cursor.close()

        if node != central_node:
            if not replicate_to_central(orderID, deliveryDate, level):
                print("fError: Replication to central node failed for {lab}")
        else:
            if not replicate_to_partitions(orderID, deliveryDate, level):
                print("Error: Replication to partition failed")

        print("Insert and replication success.\n")

    except Error as e:
        logger.error(f"Insert failed: {e}")
        try:
            node.rollback()
        except:
            pass
        print(f"Insert failed: {e}\n")

def read_order(level):
    orderID = int(input("Input orderID: "))
    
    for label, node in [("Central", central_node), ("Node2", node2), ("Node3", node3)]:
        if not check_connection(node):
            print(f"[{label}] Node unavailable")
            continue

        try:
            set_isolation_level(node, level)
            cursor = node.cursor(dictionary=True, buffered=True)
            cursor.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = %s", (orderID,))
            row = cursor.fetchone()
            cursor.close()

            if row and row.get('deliveryDate') is not None:
                print(f"[{label}] Delivery Date: {row['deliveryDate']}")
            else:
                print(f"[{label}] Order not found")

        except Exception as e:
            logger.error(f"Read from {label} failed: {e}")
            print(f"[{label}] Read failed")

def update_order(level):

    try:
        orderID = int(input("Input orderID: "))
        deliveryDate = input("New Delivery Date (YYYY-MM-DD): ")

        node = determine_node(deliveryDate)

        if not check_connection(node):
            print("Target node unavailable")
            return

        #START HERE UNG CHANGE

        # Check if the order exists in the target node before updating
        try:
            chk_cur = node.cursor()
            chk_cur.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = %s LIMIT 1", (orderID,))
            existing_row = chk_cur.fetchone()
            chk_cur.close()

            if not existing_row:
                print(f"Order {orderID} not found on target node for date {deliveryDate}. Update aborted.")
                return
        except Exception as e:
            logger.exception("Existence check on target node failed: %s", e)
            print("Could not verify order existence on target node. Update aborted.")
            return

        # Step 4: Determine the current year of the existing delivery date
        existing_deliveryDate = existing_row[0]  # Assuming 'deliveryDate' is the first column
        existing_year = str(existing_deliveryDate)[:4]
        new_year = deliveryDate[:4]

        # Step 5: Handle Year Change (If the year has changed, we need to move the order)
        if existing_year != new_year:
            # Delete from the old node
            old_node = determine_node(existing_deliveryDate)
            if check_connection(old_node):
                try:
                    set_isolation_level(old_node, level)
                    cursor = old_node.cursor()
                    cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (orderID,))
                    old_node.commit()
                    cursor.close()
                    logger.info(f"Order {orderID} deleted from {old_node}")
                except Exception as e:
                    logger.exception(f"Failed to delete from {old_node}: {e}")
                    print(f"Failed to delete order from {old_node}. Update aborted.")
                    return

            # Insert into the new node (target node based on new year)
            node = determine_node(deliveryDate)  # This gets the new node based on the year
            if check_connection(node):
                try:
                    set_isolation_level(node, level)
                    cursor = node.cursor()

                    # Prepare the INSERT query
                    sql = """
                        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
                    """
                    cursor.execute(sql, (orderID, deliveryDate))
                    node.commit()
                    cursor.close()
                    logger.info(f"Order {orderID} inserted into {node}")
                except Exception as e:
                    logger.exception(f"Failed to insert into {node}: {e}")
                    print(f"Failed to insert order into {node}. Update aborted.")
                    return

        # Step 6: Update central node (no year change needed here, just an update)
        if check_connection(central_node):
            try:
                set_isolation_level(central_node, level)
                cursor = central_node.cursor()

                # Prepare the UPDATE query for central node
                sql = """
                    UPDATE FactOrders
                    SET deliveryDate = %s, updatedAt = NOW()
                    WHERE orderID = %s
                """
                cursor.execute(sql, (deliveryDate, orderID))
                central_node.commit()
                cursor.close()
                logger.info(f"Central node updated for order {orderID}")
            except Exception as e:
                logger.exception(f"Failed to update central node: {e}")
                print(f"Failed to update central node. Update aborted.")
                return

        # Step 7: Replicate the update across all nodes
        if node != central_node:
            if not replicate_to_central(orderID, deliveryDate, level):
                print("Warning: replication to central failed")
        else:
            if not replicate_to_partitions(orderID, deliveryDate, level):
                print("Warning: replication to partitions failed")

        print("Update and replication successful.\n")

    except Exception as e:
        logger.exception("Unexpected error in update_order: %s", e)
        print("Update aborted due to an unexpected error.")

    
def delete_order(level):
    orderID = int(input("Input orderID: "))
    
    sql = """
        DELETE FROM FactOrders
        WHERE orderID = %s
    """
    for node, label in [(central_node, "Central"), (node2, "Node2"), (node3, "Node3")]:
        if not check_connection(node):
            logger.warning("%s not available during delete", label)
            continue
        try:
            cur = node.cursor()
            logger.info("DELETE on %s: %s", label, orderID)
            cur.execute(sql, (orderID,)) 
            node.commit()
            cur.close()
        except Exception as e:
            logger.exception("Delete on %s failed: %s", label, e)
            try:
                node.rollback()
            except:
                pass
    
    print("Delete successful\n")

# ========================================================================
# 6. MAIN MENU
# ========================================================================

def menu():
    while True: 
        print("\nSELECT ISOLATION LEVEL TO USE\n")
        print("1. READ UNCOMMITTED")
        print("2. READ COMMITTED")
        print("3. REPEATABLE READ")
        print("4. SERIALIZABLE")
    
        user_input = input()
        if user_input not in ISOLATION_LEVELS:
            print("Invalid.")
            continue
        
        level = ISOLATION_LEVELS[user_input]
        print(f"Using isolation level: {level}\n")        
        

        print("\nMAIN MENU")
        print("1. Insert")
        print("2. Read")
        print("3. Update")
        print("4. Delete")
        print("5. Exit")

        option = input("Choose option: ")

        match option:
            case '1': 
                insert_order(level)
            case '2': 
                read_order(level)
            case '3': 
                update_order(level)
            case '4': 
                delete_order(level)
            case '5':
                break
            case _: 
                print("Invalid Choice")
                            
menu() 
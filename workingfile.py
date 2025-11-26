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

def determine_partition_node(delivery_date):
    """Determines which partition node (node2 or node3) based on year"""
    try:
        year = int(str(delivery_date)[:4])
        
        if year == 2024:
            return node2
        elif year == 2025:
            return node3
        else:
            return None
    except:
        logger.error(f"Invalid date format: {delivery_date}")
        return None


# ========================================================================
# 4. REPLICATION HELPERS
# ========================================================================

def replicate_insert_to_central(order_id, delivery_date, level):
    """Replicates an insert from partition node to central node"""
    if not check_connection(central_node):
        logger.error("Central node unavailable for replication")
        return False

    try:   
        set_isolation_level(central_node, level)
        cursor = central_node.cursor()

        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
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
        
def replicate_insert_to_partition(order_id, delivery_date, level):
    """Replicates an insert from central node to partition node"""
    partition_node = determine_partition_node(delivery_date)
    
    if partition_node is None:
        logger.warning(f"No partition node for date {delivery_date}")
        return True  # Not an error, just no partition for this year
    
    if not check_connection(partition_node):
        logger.error("Target partition node unavailable for replication")
        return False

    try:
        set_isolation_level(partition_node, level)
        cursor = partition_node.cursor()

        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
        """
        logger.info("REPL->PART SQL: %s -- PARAMS: (%s, %s)", sql.strip(), order_id, delivery_date)
        cursor.execute(sql, (order_id, delivery_date))
        partition_node.commit()
        cursor.close()
        logger.info("Replicated order %s to partition node (deliveryDate=%s)", order_id, delivery_date)
        return True

    except Exception as e:
        logger.error(f"Failed to replicate to partition node: {e}")
        try:
            partition_node.rollback()
        except:
            pass
        return False


# ========================================================================
# 5. CRUD OPERATIONS
# ========================================================================

def insert_order(level):
    try:
        orderID = int(input("Input orderID: "))

        # Check if orderID already exists in central node
        if check_connection(central_node):
            cursor = central_node.cursor()
            cursor.execute("SELECT 1 FROM FactOrders WHERE orderID = %s", (orderID,))
            if cursor.fetchone():
                cursor.close()
                print("Error: OrderID already exists")
                return
            cursor.close()

        deliveryDate = input("Input Delivery Date (YYYY-MM-DD): ")
        
        # Step 1: Insert into central node first
        if not check_connection(central_node):
            print("Error: Central node unavailable")
            return

        set_isolation_level(central_node, level)
        cursor = central_node.cursor()

        sql = """
        INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
        VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
        """

        cursor.execute(sql, (orderID, deliveryDate))
        central_node.commit()
        cursor.close()
        logger.info(f"Inserted order {orderID} into central node")

        # Step 2: Replicate to appropriate partition node
        if not replicate_insert_to_partition(orderID, deliveryDate, level):
            print("Warning: Replication to partition node failed")
        
        print("Insert and replication successful.\n")

    except Exception as e:
        logger.error(f"Insert failed: {e}")
        try:
            if check_connection(central_node):
                central_node.rollback()
        except:
            pass
        print(f"Insert failed: {e}\n")


def read_order(level):
    try:
        orderID = int(input("Input orderID: "))
        
        found = False
        
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
                    found = True
                else:
                    print(f"[{label}] Order not found")

            except Exception as e:
                logger.error(f"Read from {label} failed: {e}")
                print(f"[{label}] Read failed")
        
        if not found:
            print("\nOrderID does not exist in any node")
            
    except ValueError:
        print("Error: Invalid orderID format")


def update_order(level):
    try:
        orderID = int(input("Input orderID: "))
        
        # Check if order exists in central node
        if not check_connection(central_node):
            print("Error: Central node unavailable")
            return
        
        set_isolation_level(central_node, level)
        cursor = central_node.cursor()
        cursor.execute("SELECT deliveryDate FROM FactOrders WHERE orderID = %s", (orderID,))
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            print(f"Error: OrderID {orderID} does not exist")
            return
        
        old_delivery_date = result[0]
        old_year = int(str(old_delivery_date)[:4])
        
        new_delivery_date = input("New Delivery Date (YYYY-MM-DD): ")
        new_year = int(new_delivery_date[:4])
        
        # Determine old and new partition nodes
        old_partition = determine_partition_node(old_delivery_date)
        new_partition = determine_partition_node(new_delivery_date)
        
        # Case 1: Year changed (e.g., 2024 to 2025 or vice versa)
        if old_year != new_year and old_partition and new_partition:
            logger.info(f"Year changed from {old_year} to {new_year}, moving between partitions")
            
            # Step 1: Insert into new partition node
            if check_connection(new_partition):
                try:
                    set_isolation_level(new_partition, level)
                    cursor = new_partition.cursor()
                    sql = """
                    INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
                    VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
                    """
                    cursor.execute(sql, (orderID, new_delivery_date))
                    new_partition.commit()
                    cursor.close()
                    logger.info(f"Inserted order {orderID} into new partition")
                except Exception as e:
                    logger.error(f"Failed to insert into new partition: {e}")
                    print("Error: Failed to insert into new partition")
                    return
            
            # Step 2: Delete from old partition node
            if check_connection(old_partition):
                try:
                    set_isolation_level(old_partition, level)
                    cursor = old_partition.cursor()
                    cursor.execute("DELETE FROM FactOrders WHERE orderID = %s", (orderID,))
                    old_partition.commit()
                    cursor.close()
                    logger.info(f"Deleted order {orderID} from old partition")
                except Exception as e:
                    logger.error(f"Failed to delete from old partition: {e}")
            
            # Step 3: Update central node
            if check_connection(central_node):
                try:
                    set_isolation_level(central_node, level)
                    cursor = central_node.cursor()
                    sql = """
                    UPDATE FactOrders
                    SET deliveryDate = %s, updatedAt = NOW()
                    WHERE orderID = %s
                    """
                    cursor.execute(sql, (new_delivery_date, orderID))
                    central_node.commit()
                    cursor.close()
                    logger.info(f"Updated order {orderID} in central node")
                except Exception as e:
                    logger.error(f"Failed to update central node: {e}")
                    print("Error: Failed to update central node")
                    return
        
        # Case 2: Same year, just update date
        else:
            logger.info(f"Same year update for order {orderID}")
            
            # Step 1: Update central node
            if check_connection(central_node):
                try:
                    set_isolation_level(central_node, level)
                    cursor = central_node.cursor()
                    sql = """
                    UPDATE FactOrders
                    SET deliveryDate = %s, updatedAt = NOW()
                    WHERE orderID = %s
                    """
                    cursor.execute(sql, (new_delivery_date, orderID))
                    central_node.commit()
                    cursor.close()
                    logger.info(f"Updated order {orderID} in central node")
                except Exception as e:
                    logger.error(f"Failed to update central node: {e}")
                    print("Error: Failed to update central node")
                    return
            
            # Step 2: Update partition node (if exists)
            if new_partition and check_connection(new_partition):
                try:
                    set_isolation_level(new_partition, level)
                    cursor = new_partition.cursor()
                    sql = """
                    UPDATE FactOrders
                    SET deliveryDate = %s, updatedAt = NOW()
                    WHERE orderID = %s
                    """
                    cursor.execute(sql, (new_delivery_date, orderID))
                    new_partition.commit()
                    cursor.close()
                    logger.info(f"Updated order {orderID} in partition node")
                except Exception as e:
                    logger.error(f"Failed to update partition node: {e}")
                    print("Warning: Failed to update partition node")
        
        print("Update successful.\n")

    except ValueError:
        print("Error: Invalid input format")
    except Exception as e:
        logger.exception(f"Unexpected error in update_order: {e}")
        print("Update aborted due to an unexpected error.")

    
def delete_order(level):
    try:
        orderID = int(input("Input orderID: "))
        
        # Check if order exists first
        exists = False
        if check_connection(central_node):
            cursor = central_node.cursor()
            cursor.execute("SELECT 1 FROM FactOrders WHERE orderID = %s", (orderID,))
            if cursor.fetchone():
                exists = True
            cursor.close()
        
        if not exists:
            print(f"Error: OrderID {orderID} does not exist")
            return
        
        sql = "DELETE FROM FactOrders WHERE orderID = %s"
        
        for node, label in [(central_node, "Central"), (node2, "Node2"), (node3, "Node3")]:
            if not check_connection(node):
                logger.warning("%s not available during delete", label)
                continue
            try:
                set_isolation_level(node, level)
                cur = node.cursor()
                logger.info("DELETE on %s: %s", label, orderID)
                cur.execute(sql, (orderID,))
                node.commit()
                cur.close()
                logger.info(f"Deleted from {label}")
            except Exception as e:
                logger.exception("Delete on %s failed: %s", label, e)
                try:
                    node.rollback()
                except:
                    pass
        
        print("Delete successful\n")
        
    except ValueError:
        print("Error: Invalid orderID format")


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
    
        user_input = input("Enter choice: ")
        if user_input not in ISOLATION_LEVELS:
            print("Invalid choice.")
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
                print("Exiting...")
                break
            case _: 
                print("Invalid Choice")

if __name__ == "__main__":
    menu()
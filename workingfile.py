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
# 3. CONCURRENCY WARNING DISPLAY
# ========================================================================
def display_isolation_warnings (level, operation_type):
    """Display warnings about possible concurrency problems for current isolation level"""
    warnings = {
        "READ UNCOMMITTED": {
            "read": ["DIRTY READS possible"],
            "update": ["NON-REPEATABLES READS possible", "DIRTY READS possible"],
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
            "insert": ["Full serializablity guaranteed"]
        }
    }

    if level in warnings and operation_type in warnings[level]:
        print(f"\n Isolation level: {level}")
        for warning in warnings[level][operation_type]:
            print(f"    {warning}")
        print()

# ========================================================================
# 4. PARTITIONING RULE
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
# 5. REPLICATION HELPERS
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
# 6. AUTOMATIC CONCURRENCY DETECTION IN CRUD
# ========================================================================

def check_for_phantom_insert(year, level):
    """Check if phantom reads are possible after insert"""
    if level in ["READ UNCOMITTED", "READ COMMITTED", "REPEATABLE READ"]:
        print(f"\n PHANTOM READ WARNING:")
        print(f"    New order in year {year} could appear as 'phantom row' in concurrent transactions")
        print(f"    at {level} isolation level")


def check_for_non_repeatable_read_update(order_id, old_date, new_date, level):
    """Check if non-repeatable reads are possible after update"""
    if level in ["READ UNCOMMITTED", "READ COMMITTED"]:
        print(f"\n NON-REPEATABLE READ WARNING:")
        print(f"    Order {order_id} changed from {old_date} to {new_date}")
        print(f"    Concurrent transactions at {level} may see inconsistent values")
        
def check_for_dirty_read_uncommitted(order_id, level):
    """Warning about dirty reads during uncommitted transactions"""
    if level == "READ UNCOMMITTED":
        print(f"\n DIRTY READ WARNING:")
        print(f"    Other transactions at READ UNCOMMITTED can see uncommitted changes to order {order_id}")

# ========================================================================
# 7. CONTROLLED TEST FUNCTIONS (Menu Options)
# ========================================================================

def detect_dirty_read(order_id, level):
    """
    Controlled test for dirty reads
    TO TEST: Open another terminal/device and start an UPDATE without committing
    """
    print("\n" + "="*70)
    print("DIRTY READ DETECTION TEST")
    print("="*70)

    if not check_connection(central_node):
        print("Central node unavailable")
        return
    
    try:
        set_isolation_level(central_node, level)
        cursor = central_node.cursor(dictionary=True)
        cursor.execute("SELECT deliveryDate, updatedAt FROM FactOrders WHERE orderID = %s", (order_id,))
        initial_read = cursor.fetchone()
        cursor.close()
        
        if not initial_read:
            print(f" Order {order_id} not found.")
            return
        
        initial_date = initial_read['deliveryDate']
        initial_updated = initial_read['updatedAt']
        print(f" First Read: Order {order_id}")
        print(f" Delivery Date: {initial_date}")
        print(f" Updated At: {initial_updated}")
        
        print("\n Now update this order in another terminal WITHOUT committing...")
        print(" Then press ENTER here to read again...")
        input(" Press ENTER when ready: ")
        
        cursor = central_node.cursor(dictionary=True)
        cursor.execute("SELECT deliveryDate, updatedAt FROM FactOrders WHERE orderID = %s", (order_id,))
        second_read = cursor.fetchone()
        cursor.close()
        
        second_date = second_read['deliveryDate'] if second_read else None
        second_updated = second_read['updatedAt'] if second_read else None
        
        print(f"\n Second Read: Order {order_id}")
        print(f" Delivery Date: {second_date}")
        print(f" Updated At: {second_updated}")

        if initial_date != second_date or initial_updated != second_updated:
            print(f"\n DIRTY READ DETECTED!")
            print(f"    Read uncommitted data from another transaction!")
        else:
            print(f"\n No Dirty Read: Data remained consistent")
        
        if level == "READ UNCOMMITTED":
            print(f"\n Isolation Level: {level}")
            print(f"    This level ALLOWS dirty reads!")
        else:
            print(f"\n Isolation level: {level}")
            print("     This level PREVENTS dirty reads")
            
    except Exception as e:
        print(f" Error: {e}")
    
    print("="*70 + "\n")

def detect_non_repeatable_read(order_id, level):
    """
    Controlled test for non-repeatable reads
    TO TEST: Open another terminal/device and UPDATE + COMMIT while this transaction is running
    """
    print("\n" + "="*70)
    print(" NON-REPEATABLE READ DETECTION TEST")
    print("="*70)
    
    if not check_connection(central_node):
        print(" Central node unavailable")
        return
    
    try:
        set_isolation_level(central_node, level)
        central_node.start_transaction()
        
        cursor = central_node.cursor(dictionary=True)
        cursor.execute("SELECT deliveryDate, updatedAt FROM FactOrders WHERE orderID = %s", (order_id,))
        first_read = cursor.fetchone()
        cursor.close()
        
        if not first_read:
            central_node.rollback()
            print(f" Order {order_id} not found.")
            return
        
        first_date = first_read['deliveryDate']
        first_updated = first_read['updatedAt']
        print(f" First Read (in transaction): Order {order_id}")
        print(f"   Delivery Date: {first_date}")
        print(f"   Updated At: {first_updated}")
        
        print("\n Now UPDATE and COMMIT this order in another terminal...")
        print("   Then press ENTER here to read again (still in same transaction)...")
        input("   Press ENTER when ready: ")
        
        cursor = central_node.cursor(dictionary=True)
        cursor.execute("SELECT deliveryDate, updatedAt FROM FactOrders WHERE orderID = %s", (order_id,))
        second_read = cursor.fetchone()
        cursor.close()
        
        second_date = second_read['deliveryDate'] if second_read else None
        second_updated = second_read['updatedAt'] if second_read else None
        
        print(f"\n Second Read (same transaction): Order {order_id}")
        print(f"   Delivery Date: {second_date}")
        print(f"   Updated At: {second_updated}")
        
        if first_date != second_date or first_updated != second_updated:
            print(f"\n NON-REPEATABLE READ DETECTED!")
            print(f"   First Read:  Date={first_date}, Updated={first_updated}")
            print(f"   Second Read: Date={second_date}, Updated={second_updated}")
            print(f"   Same query returned different results in the same transaction!")
        else:
            print(f"\n No Non-Repeatable Read: Data remained consistent")
        
        central_node.rollback()
        
        if level in ["READ UNCOMMITTED", "READ COMMITTED"]:
            print(f"\n Isolation Level: {level}")
            print("   This level ALLOWS non-repeatable reads!")
        else:
            print(f"\n Isolation Level: {level}")
            print("   This level PREVENTS non-repeatable reads")
            
    except Exception as e:
        print(f" Error: {e}")
        try:
            central_node.rollback()
        except:
            pass
    
    print("="*70 + "\n")


def detect_phantom_read_mysql(level):
    """
    Controlled test for phantom reads
    TO TEST: Open another terminal/device and INSERT + COMMIT a new order while this transaction is running
    """
    print("\n" + "="*70)
    print(" PHANTOM READ DETECTION TEST (MySQL Optimized)")
    print("="*70)
    
    if not check_connection(central_node):
        print(" Central node unavailable")
        return
    
    try:
        search_year = input("Enter year to search (e.g., 2024 or 2025): ")
        
        set_isolation_level(central_node, level)
        central_node.start_transaction()
        
        cursor = central_node.cursor(dictionary=True)
        cursor.execute(
            "SELECT COUNT(*) as count, MAX(orderID) as max_id, MIN(orderID) as min_id FROM FactOrders WHERE YEAR(deliveryDate) = %s",
            (search_year,)
        )
        first_stats = cursor.fetchone()
        cursor.close()
        
        print(f" First Read (in transaction):")
        print(f"   Total orders: {first_stats['count']}")
        print(f"   Min orderID: {first_stats['min_id']}")
        print(f"   Max orderID: {first_stats['max_id']}")
        
        # Get a specific range
        cursor = central_node.cursor(dictionary=True)
        cursor.execute(
            "SELECT COUNT(*) as count FROM FactOrders WHERE YEAR(deliveryDate) = %s AND orderID > %s",
            (search_year, first_stats['max_id'] - 100)
        )
        first_range = cursor.fetchone()['count']
        cursor.close()
        print(f"   Orders with ID > {first_stats['max_id'] - 100}: {first_range}")
        
        print(f"\n  Now INSERT a new order with a HIGH orderID (e.g., 99999999) in year {search_year}")
        print("   Use this command in another terminal:")
        print(f"\n   INSERT INTO FactOrders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)")
        print(f"   VALUES (99999999, 0, '{search_year}-12-31', 0, NOW(), NOW(), 0, 1);")
        print(f"   COMMIT;\n")
        input("   Press ENTER when done: ")
        
        cursor = central_node.cursor(dictionary=True)
        cursor.execute(
            "SELECT COUNT(*) as count, MAX(orderID) as max_id, MIN(orderID) as min_id FROM FactOrders WHERE YEAR(deliveryDate) = %s",
            (search_year,)
        )
        second_stats = cursor.fetchone()
        cursor.close()
        
        print(f"\n Second Read (same transaction):")
        print(f"   Total orders: {second_stats['count']}")
        print(f"   Min orderID: {second_stats['min_id']}")
        print(f"   Max orderID: {second_stats['max_id']}")
        
        cursor = central_node.cursor(dictionary=True)
        cursor.execute(
            "SELECT COUNT(*) as count FROM FactOrders WHERE YEAR(deliveryDate) = %s AND orderID > %s",
            (search_year, first_stats['max_id'] - 100)
        )
        second_range = cursor.fetchone()['count']
        cursor.close()
        print(f"   Orders with ID > {first_stats['max_id'] - 100}: {second_range}")
        
        # Check for phantom
        phantom_detected = False
        if first_stats['count'] != second_stats['count']:
            print(f"\n PHANTOM READ DETECTED!")
            print(f"   Count changed from {first_stats['count']} to {second_stats['count']}")
            phantom_detected = True
        
        if first_stats['max_id'] != second_stats['max_id']:
            print(f"\n PHANTOM READ DETECTED!")
            print(f"   Max ID changed from {first_stats['max_id']} to {second_stats['max_id']}")
            print(f"   A new high-value order appeared!")
            phantom_detected = True
        
        if first_range != second_range:
            print(f"\n PHANTOM READ DETECTED!")
            print(f"   Range count changed from {first_range} to {second_range}")
            phantom_detected = True
        
        if not phantom_detected:
            print(f"\n No Phantom Read: All statistics remained consistent")
            print(f"   (Either no insert happened, or isolation level prevented it)")
        
        central_node.rollback()
        
        
    except Exception as e:
        print(f" Error: {e}")
        import traceback
        traceback.print_exc()
        try:
            central_node.rollback()
        except:
            pass
    
    print("="*70 + "\n")

# ========================================================================
# 8. CRUD OPERATIONS
# ========================================================================

def insert_order(level):
    try:
        display_isolation_warnings(level, "insert")
        
        orderID = int(input("Input orderID: "))

        if check_connection(central_node):
            cursor = central_node.cursor()
            cursor.execute("SELECT 1 FROM FactOrders WHERE orderID = %s", (orderID,))
            if cursor.fetchone():
                cursor.close()
                print("Error: OrderID already exists")
                return
            cursor.close()

        deliveryDate = input("Input Delivery Date (YYYY-MM-DD): ")
        year = int(deliveryDate[:4])
        
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

        if not replicate_insert_to_partition(orderID, deliveryDate, level):
            print("Warning: Replication to partition node failed")
        
        print(" Insert and replication successful.\n")
        
        # Automatic concurrency detection
        check_for_phantom_insert(year, level)

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
        display_isolation_warnings(level, "read")
        
        orderID = int(input("Input orderID: "))
        
        found = False
        results = {}
        
        for label, node in [("Central", central_node), ("Node2", node2), ("Node3", node3)]:
            if not check_connection(node):
                print(f"[{label}] Node unavailable")
                results[label] = None
                continue

            try:
                set_isolation_level(node, level)
                cursor = node.cursor(dictionary=True, buffered=True)
                cursor.execute("SELECT deliveryDate, updatedAt FROM FactOrders WHERE orderID = %s", (orderID,))
                row = cursor.fetchone()
                cursor.close()

                if row and row.get('deliveryDate') is not None:
                    print(f"[{label}] Delivery Date: {row['deliveryDate']}, Updated: {row['updatedAt']}")
                    results[label] = row
                    found = True
                else:
                    print(f"[{label}] Order not found")
                    results[label] = None

            except Exception as e:
                logger.error(f"Read from {label} failed: {e}")
                print(f"[{label}] Read failed")
                results[label] = None
        
        if not found:
            print("\nOrderID does not exist in any node")
        
        # Automatic concurrency detection for READ operations
        if level in ["READ UNCOMMITTED", "READ COMMITTED"]:
            print(f"\n  CONCURRENCY NOTE:")
            print(f"   At {level}, this data could change if you read it again")
            print(f"   (Non-repeatable read possible)")
            
    except ValueError:
        print("Error: Invalid orderID format")


def update_order(level):
    try:
        display_isolation_warnings(level, "update")
        
        orderID = int(input("Input orderID: "))
        
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
        
        old_partition = determine_partition_node(old_delivery_date)
        new_partition = determine_partition_node(new_delivery_date)
        
        # Warn about uncommitted changes
        check_for_dirty_read_uncommitted(orderID, level)
        
        if old_year != new_year and old_partition and new_partition:
            logger.info(f"Year changed from {old_year} to {new_year}, moving between partitions")
            
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
        else:
            logger.info(f"Same year update for order {orderID}")
            
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
        
        print(" Update successful.\n")
        
        # Automatic concurrency detection
        check_for_non_repeatable_read_update(orderID, old_delivery_date, new_delivery_date, level)

    except ValueError:
        print("Error: Invalid input format")
    except Exception as e:
        logger.exception(f"Unexpected error in update_order: {e}")
        print("Update aborted due to an unexpected error.")

    
def delete_order(level):
    try:
        orderID = int(input("Input orderID: "))
        
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
        
        print(" Delete successful\n")
        
        if level in ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ"]:
            print(f"\n CONCURRENCY NOTE:")
            print(f"   At {level}, this deletion might affect concurrent transactions")
            print(f"   reading counts (potential phantom read impact)")
        
    except ValueError:
        print("Error: Invalid orderID format")


# ========================================================================
# 9. MAIN MENU
# ========================================================================

def menu():
    while True: 
        print("\n" + "="*70)
        print("SELECT ISOLATION LEVEL TO USE")
        print("="*70)
        print("1. READ UNCOMMITTED")
        print("2. READ COMMITTED")
        print("3. REPEATABLE READ")
        print("4. SERIALIZABLE")
    
        user_input = input("\nEnter choice: ")
        if user_input not in ISOLATION_LEVELS:
            print("Invalid choice.")
            continue
        
        level = ISOLATION_LEVELS[user_input]
        print(f"\n✓ Using isolation level: {level}\n")        
        
        while True:
            print("\n" + "="*70)
            print("MAIN MENU")
            print("="*70)
            print("CRUD OPERATIONS:")
            print("1. Insert Order")
            print("2. Read Order")
            print("3. Update Order")
            print("4. Delete Order")
            print("\nCONTROLLED CONCURRENCY TESTS:")
            print("5. Test Dirty Read Detection")
            print("6. Test Non-Repeatable Read Detection")
            print("7. Test Phantom Read Detection")
            print("\nOTHER OPTIONS:")
            print("8. Change Isolation Level")
            print("9. Exit")

            option = input("\nChoose option: ")

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
                    order_id = int(input("Enter orderID to test: "))
                    detect_dirty_read(order_id, level)
                case '6':
                    order_id = int(input("Enter orderID to test: "))
                    detect_non_repeatable_read(order_id, level)
                case '7':
                    detect_phantom_read(level)
                case '8':
                    break
                case '9':
                    print("\n Exiting... Goodbye!")
                    return
                case _: 
                    print(" Invalid Choice")

if __name__ == "__main__":
    menu()
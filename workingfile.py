import mysql.connector
from mysql.connector import Error

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
    cursor = conn.cursor()
    cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level};")
    cursor.close()


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
    year = int(str(delivery_date)[:4])
    if year == 2024:
        return node2
    elif year == 2025:
        return node3
    else:
        return central_node


# ========================================================================
# 4. REPLICATION HELPERS
# ========================================================================

def replicate_to_central(order_id, delivery_date):
    cursor = central_node.cursor()

    sql = """
    INSERT INTO FactOrder (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
    VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
    ON DUPLICATE KEY UPDATE 
        deliveryDate = VALUES(deliveryDate),
        updatedAt = NOW();
    """

    cursor.execute(sql, (order_id, delivery_date))
    central_node.commit()
    cursor.close()


def replicate_to_partitions(order_id, delivery_date):
    node = determine_node(delivery_date)
    cursor = node.cursor()

    sql = """
    INSERT INTO FactOrder (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
    VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
    ON DUPLICATE KEY UPDATE 
        deliveryDate = VALUES(deliveryDate),
        updatedAt = NOW();
    """

    cursor.execute(sql, (order_id, delivery_date))
    node.commit()
    cursor.close()

# ========================================================================
# 5. CRUD OPERATIONS
# ========================================================================

def insert_order(level):
    orderID = input("Input orderID: ")
    deliveryDate = input("Input Delivery Date (YYYY-MM-DD): ")
    
    node = determine_node(deliveryDate)
    set_isolation_level(node, level)

    cursor = node.cursor()
    sql = """
    INSERT INTO factorders (orderID, userID, deliveryDate, riderID, createdAt, updatedAt, productID, quantity)
    VALUES (%s, 0, %s, 0, NOW(), NOW(), 0, 1)
    """

    cursor.execute (sql, (deliveryDate, orderID))
    node.commit() 

def read_order(level):
    #STILL TO DO
    orderID = input("Input orderID: ")
    
    for label, node in [("Central", central_node), ("Node2", node2), ("Node3", node3)]:
        try:
            set_isolation_level(node, level)
            cursor = node.cursor(dictionary=True)
            cursor.execute("SELECT * FROM FactOrder WHERE orderID = %s", (order_id,))
            row = cursor.fetchone()
            cursor.close()
            if row:
                print(f"[{label}] {row}")
        except:
            pass


def update_order(level):
    orderID = input("Input orderID: ")
    deliveryDate = input("New Delivery Date (YYYY-MM-DD): ")

    node = determine_node(deliveryDate)
    set_isolation_level(node, level)
    cursor = node.cursor()
    sql = """
        UPDATE factorders
        SET deliveryDate = %s, updatedAT = NOW()
        WHERE orderID = %s
        """
    
    cursor.execute(sql, (deliveryDate, orderID))
    node.commit()
    
    if node != central_node:
        replicate_to_central(orderID, deliveryDate)
    else:
        replicate_to_partitions(orderID, deliveryDate)

def delete_order(level):
    orderID = input("Input orderID: ")
    
    sql = """
        DELETE FROM factorders
        WHERE orderID = %s
    """
    
    for node in [central_node, node2, node3]:
        cursor = node.cursor() 
        cursor.execute(sql, (orderID))
        node.commit()
        cursor.close
    

# ========================================================================
# 6. MAIN MENU
# ========================================================================

def menu():
    while True: 
        print("SELECT ISOLATION LEVEL TO USE\n")
        print("1. READ UNCOMMITTED")
        print("2. READ COMMITTED")
        print("3. REPEATABLE READ")
        print("4. SERIALIZABLE")
    
        input = input()
        if input not in ISOLATION_LEVELS:
            print("Invalid.")
            continue
        
        level = ISOLATION_LEVELS[input]

        print("MAIN MENU")
        print("1. Insert")
        print("2. Read")
        print("3. Update")
        print("4. Delete")
        print("5. Exit")

        option = input("Choose option: ")

        match option:
            case 1: insert_order(level)
            case 2: read_order(level)
            case 3: update_order(level)
            case 4: delete_order(level)
            case 5: break
            case _: print("Invalid Choice")
                
menu() 
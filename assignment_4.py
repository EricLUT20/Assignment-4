from bson import ObjectId
import psycopg2
import pymongo

# SQL (PostgreSQL) connection
database_postgres = psycopg2.connect(
    database="lpr_db",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)

# NoSQL (MongoDB) connection
database_mongo = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = database_mongo['lahti_db']
mongo_customers = mongo_db['customers']
mongo_orders = mongo_db['orders']


# Main Menu
def menu():
    print("\n--- Menu ---")
    print("1. Print data")
    print("2. Insert data")
    print("3. Delete data")
    print("4. Modify data")
    print("0. Exit")

# Asking for database choice between SQL and NoSQL seperately, or both
def ask_for_db():
    print("\nWhich database do you want to use for this operation?")
    print("1. SQL (PostgreSQL)")
    print("2. NoSQL (MongoDB)")
    print("3. Both databases (PostgreSQL and MongoDB)")
    print("0. Nevermind, go back to main menu")
    return int(input("\nEnter your choice: "))

def ask_table_choice():
    print("\nWhich table do you want to modify data from?")
    print("1. Customers")
    print("2. Orders")
    return int(input("\nEnter your choice: "))

# Asking for customer details for modify operation
def ask_customer_details():
    first_name = input("Enter new first name (or leave blank to keep unchanged): ")
    last_name = input("Enter new last name (or leave blank to keep unchanged): ")
    email = input("Enter new email address (or leave blank to keep unchanged): ")
    address = input("Enter new address (or leave blank to keep unchanged): ")
    return first_name, last_name, email, address

# Asking for customer details for insert operation
def ask_insert_details():
    first_name = input("Enter first name: ")
    last_name = input("Enter last name: ")
    email = input("Enter email address: ")
    address = input("Enter shipping address: ")
    product = input("Enter product name: ")
    price = float(input("Enter product price: "))
    return first_name, last_name, email, address, product, price

# Print data
def print_data():
    print("\n--- Print Data ---")

    # Asking for database choice
    choice = ask_for_db()
    if choice == 0:
        print("Returning to main menu.")
        return
    elif choice != 1 and choice != 2 and choice != 3:
        print("Invalid choice, returning to main menu.")
        return

    if choice == 1:  # SQL
        print("\n--- Data from SQL Database ---")
        try:
            with database_postgres.cursor() as cursor:
                cursor.execute("SELECT * FROM Customers;")
                customers = cursor.fetchall()
                cursor.execute("SELECT * FROM Orders;")
                orders = cursor.fetchall()
        except Exception as error:
            database_postgres.rollback()
            print(f"Error: {error}")

        # Printing customers table
        print("\nCustomers:")
        for customer in customers:
            print(customer)

        # Printing orders table
        print("\nOrders:")
        for order in orders:
            print(order)

    elif choice == 2:  # MongoDB
        print("\n--- Data from NoSQL Database ---")
        customers = list(mongo_customers.find())
        orders = list(mongo_orders.find())

        # Printing customers table
        print("\nCustomers:")
        for customer in customers:
            print(
                (
                    str(customer["_id"]),
                    customer["first_name"],
                    customer["last_name"],
                    customer["email"],
                    customer["address"],
                )
            )

        # Printing orders table
        print("\nOrders:")
        for order in orders:
            print(
                (
                    str(order["_id"]),
                    str(order["customer_id"]),
                    order["product_name"],
                    order["price"]
                )
            )

    # If user chooses to print data from both databases (joined table/data)
    elif choice == 3:  # Both databases
        print("\n--- Combined Joined Data from Both Databases ---")

        joined_table = []

        try:
            with database_postgres.cursor() as cursor:

                # Getting customers and orders from postgresql
                cursor.execute("SELECT * FROM Customers;")
                customers_table = cursor.fetchall()
                cursor.execute("SELECT * FROM Orders;")
                orders_table = cursor.fetchall()

                for customer in customers_table:
                    customer_id = customer[0]
                    for order in orders_table:
                        if order[1] == customer_id:
                            joined_table.append(
                                (
                                    customer_id,
                                    customer[1],
                                    customer[2],
                                    customer[3],
                                    customer[4],
                                    order[3],
                                    order[2],
                                )
                            )
        except Exception as error:
            database_postgres.rollback()
            print(f"Error: {error}")

        # Getting customers and orders tables from MongoDB
        customers_list = list(mongo_customers.find())
        orders_list = list(mongo_orders.find())

        # Combining data from both databases to a joined table
        for customer in customers_list:
            customer_id = customer["_id"]
            for order in orders_list:
                if order["customer_id"] == customer_id:
                    joined_table.append(
                        (
                            str(customer_id),
                            customer["first_name"],
                            customer["last_name"],
                            customer["email"],
                            customer["address"],
                            order["product_name"],
                            order["price"],
                        )
                    )

        # Printing joined table
        print("\nJoined Table (Customers + Orders):")
        for row in joined_table:
            print(row)

# Insert Data
def insert_data():
    print("\n--- Insert Data ---")

    # Asking for database choice between SQL and NoSQL seperately, or both
    choice = ask_for_db()
    if choice == 0:
        print("Returning to main menu.")
        return
    elif choice != 1 and choice != 2 and choice != 3:
        print("Invalid choice, returning to main menu.")
        return

    print()

    # Getting user input for customer details
    first_name, last_name, email, address, product, price = ask_insert_details()
    
    print()

    # Inserting data into the databases based on the previous choices
    if choice == 1 or choice == 3:  # SQL
        try:
            with database_postgres.cursor() as cursor:

                # Inserting data into the Customers table
                cursor.execute(
                    """
                    INSERT INTO Customers (first_name, last_name, email, address)
                    VALUES (%s, %s, %s, %s) RETURNING customer_id;
                    """,
                    (first_name, last_name, email, address)
                )
                
                customer_id = cursor.fetchone()[0] # Getting the customer_id to insert it to the Orders table foreign key

                # Inserting data into the Orders table
                cursor.execute(
                    """
                    INSERT INTO Orders (customer_id, price, product_name)
                    VALUES (%s, %s, %s);
                    """,
                    (customer_id, price, product)
                )
                database_postgres.commit()
        except Exception as error:
            database_postgres.rollback()
            print(f"Error: {error}")
        print("Data inserted into SQL (PostgreSQL) database.")

    # Inserting data into the NoSQL (MongoDB) databases
    if choice == 2 or choice == 3:  # MongoDB

        # Inserting data into the Customers collection and saving result for foreign key in the Orders collection/table
        result = mongo_customers.insert_one(
            {"first_name": first_name, "last_name": last_name, "email": email, "address": address}
        )

        # Inserting data into the Orders collection
        mongo_orders.insert_one(
            {
                "customer_id": result.inserted_id,
                "product_name": product,
                "price": price,
            }
        )
        print("Data inserted into NoSQL (MongoDB) database.")

# Delete Data
def delete_data():
    print("\n--- Delete Data ---")

    # Ask whether to delete data from SQL, MongoDB, or both
    choice = ask_for_db()
    if choice == 0:
        print("Returning to main menu.")
        return
    if choice != 1 and choice != 2 and choice != 3:
        print("Invalid choice, returning to main menu.")
        return

    # Ask whether to delete from Customers or Orders
    table_choice = ask_table_choice()

    # Ask for search attribute (Customer ID or Order ID) based on the table choice
    if table_choice == 1:
        search_attribute = input("Enter customer ID to delete: ")
    elif table_choice == 2:
        search_attribute = input("Enter order ID to delete: ")
    else:
        print("Invalid choice, returning to main menu.")
        return

    # Deleting data from the databases based on choices
    if choice == 1 or choice == 3:  # SQL
        try:
            with database_postgres.cursor() as cursor:

                # Deleting data from the Customers table
                if table_choice == 1:  # Customers table

                    # Checking if the customer exists
                    cursor.execute("SELECT * FROM Customers WHERE customer_id = %s", (search_attribute,))
                    customer_exists = cursor.fetchone()
                    if customer_exists:

                        # Deleting related orders with the customer ID attached and the customers with the customer ID
                        cursor.execute("DELETE FROM Orders WHERE customer_id = %s", (search_attribute,))
                        cursor.execute("DELETE FROM Customers WHERE customer_id = %s", (search_attribute,))
                        database_postgres.commit()
                        print(f"Customer with ID \"{search_attribute}\" and related orders deleted from PostgreSQL.")
                    else:
                        print(f"Customer with ID \"{search_attribute}\" not found in PostgreSQL.")
                
                # Deleting data from the Orders table
                elif table_choice == 2:  # Orders table

                    # Checking if the order exists
                    cursor.execute("SELECT * FROM Orders WHERE order_id = %s", (search_attribute,))
                    customer_exists = cursor.fetchone()
                    if customer_exists:

                        # Deleting the order from the orders table based on the order ID
                        cursor.execute("DELETE FROM Orders WHERE order_id = %s", (search_attribute,))
                        database_postgres.commit()
                        print(f"Order with ID \"{search_attribute}\" deleted from PostgreSQL.")
                    else:
                        print(f"Order with ID \"{search_attribute}\" not found in PostgreSQL.")
        except Exception as error:
            database_postgres.rollback()
            print(f"Error: {error}")

    # Deleting data from MongoDB
    if choice == 2 or choice == 3:  # MongoDB
        try:
            object_id = ObjectId(search_attribute) # Convert the search attribute to an ObjectId for MongoDB
            if table_choice == 1:  # Customers table

                # Checking if the customer exists
                customer_exists = mongo_customers.find_one({"_id": object_id})
                if customer_exists:

                    # Deleting related orders with the customer ID attached and the customers with the customer ID
                    mongo_customers.delete_one({"_id": object_id})
                    mongo_orders.delete_many({"customer_id": object_id})
                    print(f"Customer with ID \"{search_attribute}\" and related orders deleted from MongoDB.")
                else:
                    print(f"Customer with ID \"{search_attribute}\" not found in MongoDB.")

            # Deleting data from the Orders table
            elif table_choice == 2:  # Orders table

                # Checking if the order exists
                customer_exists = mongo_orders.find_one({"_id": object_id})
                if customer_exists:

                    # Deleting the order from the orders table based on the order ID
                    mongo_orders.delete_one({"_id": object_id})
                    print(f"Order with ID \"{search_attribute}\" deleted from MongoDB.")
                else:
                    print(f"Order with ID \"{search_attribute}\" not found in MongoDB.")
        except Exception as error:
            print(f"Error: {error}")


# Modify Data
def modify_data():
    print("\n--- Modify Data ---")

    # Ask whether to modify data from SQL, MongoDB, or both
    choice = ask_for_db()
    if choice == 0:
        print("Returning to main menu.")
        return
    if choice != 1 and choice != 2 and choice != 3:
        print("Invalid choice, returning to main menu.")
        return

    # Ask whether to modify Customers or Orders
    table_choice = ask_table_choice()

    # Ask for search attribute (Customer ID or Order ID) and new details based on the table choice
    if table_choice == 1:
        search_attribute = input("Enter customer ID to modify: ")
        first_name, last_name, email, address = ask_customer_details()
    elif table_choice == 2:
        search_attribute = input("Enter order ID to modify: ")
        product_name = input("Enter new product name (or leave blank to keep unchanged): ")
        price = input("Enter new product price (or leave blank to keep unchanged): ")
    else: 
        print("Invalid choice, returning to main menu.")
        return

    # Modifying data in the databases based on choices
    if choice == 1 or choice == 3:  # SQL
        try:
            with database_postgres.cursor() as cursor:

                # If the user wants to modify a customer table
                if table_choice == 1:  # Customers table

                    # Checking if the customer with the customer ID exists
                    cursor.execute("SELECT * FROM Customers WHERE customer_id = %s", (search_attribute,))
                    customer_exists = cursor.fetchone()
                    if customer_exists:

                        # If user left blank inputs for details then the old details are kept
                        if first_name == "":
                            first_name = customer_exists[1]
                        if last_name == "":
                            last_name = customer_exists[2]
                        if email == "":
                            email = customer_exists[3]
                        if address == "":
                            address = customer_exists[4]

                        # Updating the customer with the new details
                        cursor.execute(
                            """
                            UPDATE Customers
                            SET first_name = %s, last_name = %s, email = %s, address = %s
                            WHERE customer_id = %s
                            """,
                            (first_name, last_name, email, address, search_attribute)
                        )
                        database_postgres.commit()
                        print(f"Customer with ID \"{search_attribute}\" modified in PostgreSQL.")
                    else:
                        print(f"Customer with ID \"{search_attribute}\" not found in PostgreSQL.")
                
                # If the user wants to modify an order table instead
                elif table_choice == 2:  # Orders table

                    # Checking if the order with the order ID exists
                    cursor.execute("SELECT * FROM Orders WHERE order_id = %s", (search_attribute,))
                    order_exists = cursor.fetchone()
                    if order_exists:
                        
                        # If user left blank inputs for details then the old details are kept
                        if product_name == "":
                            product_name = order_exists[2]
                        if price == "":
                            price = order_exists[3]

                        # Updating the orders table with the new details
                        cursor.execute(
                            """
                            UPDATE Orders
                            SET product_name = %s, price = %s
                            WHERE order_id = %s
                            """,
                            (product_name, price, search_attribute)
                        )
                        database_postgres.commit()
                        print(f"Order with ID \"{search_attribute}\" modified in PostgreSQL.")
                    else:
                        print(f"Order with ID \"{search_attribute}\" not found in PostgreSQL.")
        except Exception as error:
            database_postgres.rollback()
            print(f"Error: {error}")

    # Modifying data in the MongoDB database if chosen
    if choice == 2 or choice == 3:  # MongoDB
        try:
            object_id = ObjectId(search_attribute)  # Convert inputted ID to ObjectId for MongoDB
            
            # If the user wants to modify a customer table
            if table_choice == 1:  # Customers table

                # Checking if the customer with the customer ID exists
                customer = mongo_customers.find_one({"_id": object_id})
                if customer:

                    # If user left blank inputs for details then the old details are kept
                    if first_name == "":
                        first_name = customer["first_name"]
                    if last_name == "":
                        last_name = customer["last_name"]
                    if email == "":
                        email = customer["email"]
                    if address == "":
                        address = customer["address"]

                    mongo_customers.update_one(
                        {"_id": object_id},
                        {"$set": {
                            "first_name": first_name,
                            "last_name": last_name,
                            "email": email,
                            "address": address,
                        }}
                    )
                    print(f"Customer with ID \"{search_attribute}\" modified in MongoDB.")
                else:
                    print(f"Customer with ID \"{search_attribute}\" not found in MongoDB.")
            
            # If the user wants to modify an order table instead
            elif table_choice == 2:  # Orders table

                # Checking if the order with the order ID exists
                order = mongo_orders.find_one({"_id": object_id})
                if order:

                    # If user left blank inputs for details then the old details are kept
                    if product_name == "":
                        product_name = order["product_name"]
                    if price == "":
                        price = order["price"]

                    # Updating the orders table with the new details
                    mongo_orders.update_one(
                        {"_id": object_id},
                        {"$set": {
                            "product_name": product_name,
                            "price": price,
                        }}
                    )
                    print(f"Order with ID \"{search_attribute}\" modified in MongoDB.")
                else:
                    print(f"Order with ID \"{search_attribute}\" not found in MongoDB.")
        except Exception as error:
            print(f"Error: {error}")
        
# Main
def main():
    print("-------------------------------------")
    print(" ###### Database Application ######")
    print("-------------------------------------")
    while True:
        menu()
        try:
            choice = int(input("\nEnter your choice: "))
            if choice == 1:
                print_data()
            elif choice == 2:
                insert_data()
            elif choice == 3:
                delete_data()
            elif choice == 4:
                modify_data()
            elif choice == 0:
                print("\nExiting the application...\n")
                break
            else:
                print("Invalid choice. Try again.")
        except Exception as error:
            print(f"Error: {error}")

    database_postgres.close()
    database_mongo.close()

if __name__ == '__main__':
    main()
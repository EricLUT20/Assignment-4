import psycopg2
import pymongo

def create_postgresdb():
    try:
        postgres_connection = psycopg2.connect(
            dbname="postgres", user="postgres", password="admin", host="localhost", port="5432"
        )
        postgres_connection.autocommit = True
        cursor = postgres_connection.cursor()

        cursor.execute("DROP DATABASE IF EXISTS lpr_db")
        cursor.execute("CREATE DATABASE lpr_db")
        cursor.close()
        postgres_connection.close()

        print("PostgreSQL database 'lpr_db' created successfully.")

        lpr_connection = psycopg2.connect(
            dbname="lpr_db", user="postgres", password="admin", host="localhost", port="5432"
        )
        lpr_cursor = lpr_connection.cursor()

        lpr_cursor.execute("""
            CREATE TABLE customers (
                customer_id SERIAL PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(100),
                address VARCHAR(100)
            );
        """)

        lpr_cursor.execute("""
            CREATE TABLE orders (
                order_id SERIAL PRIMARY KEY,
                customer_id INT,
                product_name VARCHAR(100),
                price DECIMAL,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
            );
        """)

        customers = [
            ("John", "Doe", "john.doe@gmail.com", "Lappeenranta"),
            ("Jane", "Smith", "jane.smith@gmail.com", "Lappeenranta"),
            ("Robert", "Johnson", "robert.johnson@gmail.com", "Lappeenranta"),
            ("Emily", "Williams", "emily.williams@gmail.com", "Lappeenranta"),
            ("Michael", "Brown", "michael.brown@gmail.com", "Lappeenranta")
        ]
        
        for customer in customers:
            lpr_cursor.execute("""
                INSERT INTO customers (first_name, last_name, email, address)
                VALUES (%s, %s, %s, %s)
            """, customer)

        orders = [
            {"customer_id": 1, "product_name": "iPhone 15", "price": 1099.99},
            {"customer_id": 2, "product_name": "MacBook Pro", "price": 2399.99},
            {"customer_id": 3, "product_name": "Apple Watch Ultra", "price": 799.99},
            {"customer_id": 4, "product_name": "Dell XPS 15", "price": 1899.99},
            {"customer_id": 5, "product_name": "Bose QuietComfort 45", "price": 329.99}
        ]
        
        for order in orders:
            lpr_cursor.execute("""
                INSERT INTO orders (customer_id, product_name, price)
                VALUES (%s, %s, %s)
            """, (order["customer_id"], order["product_name"], order["price"]))

        lpr_connection.commit()
        lpr_cursor.close()
        lpr_connection.close()

        print("Tables Customers and Orders inserted into PostgreSQL.")

    except Exception as error:
        print(f"Error creating PostgreSQL database: {error}")


def create_mongodb():
    try:
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        mongo_db = mongo_client["lahti_db"]

        mongo_db.drop_collection("customers")
        mongo_db.drop_collection("orders")

        mongo_customers = mongo_db["customers"]
        mongo_orders = mongo_db["orders"]

        customers = [
            {"first_name": "Anna", "last_name": "White", "email": "anna.white@gmail.com", "address": "Lahti"},
            {"first_name": "Brian", "last_name": "Green", "email": "brian.green@gmail.com", "address": "Lahti"},
            {"first_name": "Clara", "last_name": "Black", "email": "clara.black@gmail.com", "address": "Lahti"},
            {"first_name": "Derek", "last_name": "Brown", "email": "derek.brown@gmail.com", "address": "Lahti"},
            {"first_name": "Emma", "last_name": "Gray", "email": "emma.gray@gmail.com", "address": "Lahti"}
        ]

        customer_ids = []
        for customer in customers:
            result = mongo_customers.insert_one(customer)
            customer_ids.append(result.inserted_id)

        orders = [
            {"customer_id": customer_ids[0], "product_name": "OnePlus 11", "price": 899.99},
            {"customer_id": customer_ids[1], "product_name": "Microsoft Surface Pro", "price": 1499.99},
            {"customer_id": customer_ids[2], "product_name": "Bose SoundLink Revolve", "price": 299.99},
            {"customer_id": customer_ids[3], "product_name": "Sony WH-1000XM5", "price": 349.99},
            {"customer_id": customer_ids[4], "product_name": "Apple AirPods Pro 2", "price": 249.99}
        ]

        mongo_orders.insert_many(orders)

        print("MongoDB database created successfully.")

    except Exception as error:
        print(f"Error creating MongoDB database: {error}")


def main():
    print("-------------------------------------------")
    print("###### Creating databases ######")
    print("-------------------------------------------")
    print("\n--- Creating PostgreSQL and MongoDB databases ---")
    create_postgresdb()
    create_mongodb()
    print()

if __name__ == "__main__":
    main()
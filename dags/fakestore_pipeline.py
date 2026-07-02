import os
from datetime import datetime
import pendulum
import requests
import psycopg2
from airflow.decorators import dag, task

# Define timezone anchored in America/Sao_Paulo
local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'owner': 'calango_investimentos',
    'start_date': datetime(2026, 7, 1, tzinfo=local_tz),
}

@dag(
    dag_id='fakestore_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['marketplace', 'fakestore', 'calango'],
)
def fakestore_pipeline():

    @task
    def create_tables():
        """Creates target database tables if they do not exist."""
        db_url = os.getenv("TARGET_DB_URL", "postgresql://airflow:airflow@postgres/airflow")
        if db_url.startswith("postgresql+psycopg2://"):
            db_url = db_url.replace("postgresql+psycopg2://", "postgresql://")
        print(f"Connecting to database to initialize tables...")
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        # Create users table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            email VARCHAR(255),
            username VARCHAR(255),
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            phone VARCHAR(50),
            city VARCHAR(255),
            street VARCHAR(255),
            zipcode VARCHAR(50)
        );
        """)
        
        # Create products table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INT PRIMARY KEY,
            title VARCHAR(255),
            price NUMERIC,
            description TEXT,
            category VARCHAR(255),
            image TEXT,
            rating_rate NUMERIC,
            rating_count INT
        );
        """)
        
        # Create carts table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS carts (
            id INT PRIMARY KEY,
            user_id INT,
            date TIMESTAMP
        );
        """)
        
        # Create cart_items table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cart_items (
            id SERIAL PRIMARY KEY,
            cart_id INT,
            product_id INT,
            quantity INT
        );
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Tables initialized successfully.")
        return "Initialization Done"

    @task
    def ingest_users(trigger_result: str):
        """Fetches users from the FakeStore API and stores them in the DB."""
        print(f"Trigger received: {trigger_result}. Starting users ingestion...")
        url = "https://fakestoreapi.com/users"
        response = requests.get(url)
        response.raise_for_status()
        users = response.json()
        
        db_url = os.getenv("TARGET_DB_URL", "postgresql://airflow:airflow@postgres/airflow")
        if db_url.startswith("postgresql+psycopg2://"):
            db_url = db_url.replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        for user in users:
            cursor.execute("""
                INSERT INTO users (id, email, username, firstname, lastname, phone, city, street, zipcode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    email = EXCLUDED.email,
                    username = EXCLUDED.username,
                    firstname = EXCLUDED.firstname,
                    lastname = EXCLUDED.lastname,
                    phone = EXCLUDED.phone,
                    city = EXCLUDED.city,
                    street = EXCLUDED.street,
                    zipcode = EXCLUDED.zipcode;
            """, (
                user['id'],
                user['email'],
                user['username'],
                user.get('name', {}).get('firstname', ''),
                user.get('name', {}).get('lastname', ''),
                user['phone'],
                user.get('address', {}).get('city', ''),
                user.get('address', {}).get('street', ''),
                user.get('address', {}).get('zipcode', '')
            ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully ingested {len(users)} users.")
        return len(users)

    @task
    def ingest_products(trigger_result: str):
        """Fetches products from the FakeStore API and stores them in the DB."""
        print(f"Trigger received: {trigger_result}. Starting products ingestion...")
        url = "https://fakestoreapi.com/products"
        response = requests.get(url)
        response.raise_for_status()
        products = response.json()
        
        db_url = os.getenv("TARGET_DB_URL", "postgresql://airflow:airflow@postgres/airflow")
        if db_url.startswith("postgresql+psycopg2://"):
            db_url = db_url.replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        for prod in products:
            cursor.execute("""
                INSERT INTO products (id, title, price, description, category, image, rating_rate, rating_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    price = EXCLUDED.price,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    image = EXCLUDED.image,
                    rating_rate = EXCLUDED.rating_rate,
                    rating_count = EXCLUDED.rating_count;
            """, (
                prod['id'],
                prod['title'],
                prod['price'],
                prod['description'],
                prod['category'],
                prod['image'],
                prod.get('rating', {}).get('rate', 0.0),
                prod.get('rating', {}).get('count', 0)
            ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully ingested {len(products)} products.")
        return len(products)

    @task
    def ingest_carts(trigger_result: str):
        """Fetches carts/sales from the FakeStore API and stores them in the DB."""
        print(f"Trigger received: {trigger_result}. Starting carts ingestion...")
        url = "https://fakestoreapi.com/carts"
        response = requests.get(url)
        response.raise_for_status()
        carts = response.json()
        
        db_url = os.getenv("TARGET_DB_URL", "postgresql://airflow:airflow@postgres/airflow")
        if db_url.startswith("postgresql+psycopg2://"):
            db_url = db_url.replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        for cart in carts:
            # Insert or update cart
            cursor.execute("""
                INSERT INTO carts (id, user_id, date)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    date = EXCLUDED.date;
            """, (
                cart['id'],
                cart['userId'],
                cart['date']
            ))
            
            # Clean old items for this cart to prevent duplication
            cursor.execute("DELETE FROM cart_items WHERE cart_id = %s;", (cart['id'],))
            
            # Insert items
            for item in cart['products']:
                cursor.execute("""
                    INSERT INTO cart_items (cart_id, product_id, quantity)
                    VALUES (%s, %s, %s);
                """, (
                    cart['id'],
                    item['productId'],
                    item['quantity']
                ))
                
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully ingested {len(carts)} carts.")
        return len(carts)

    @task
    def notify_success(users_count: int, products_count: int, carts_count: int):
        """Consolidates execution counts using TaskFlow dependencies."""
        summary = (
            f"--- INGESTION SUMMARY ---\n"
            f"Total Users Ingested: {users_count}\n"
            f"Total Products Ingested: {products_count}\n"
            f"Total Carts Ingested: {carts_count}\n"
            f"--------------------------"
        )
        print(summary)
        return summary

    # Dependencies emerge strictly from function calls
    tables_created = create_tables()
    
    # Ingestion tasks run in parallel, triggered by table creation
    users_ingested = ingest_users(tables_created)
    products_ingested = ingest_products(tables_created)
    carts_ingested = ingest_carts(tables_created)
    
    # Summary depends on the parallel ingestions completing
    notify_success(users_ingested, products_ingested, carts_ingested)

dag_instance = fakestore_pipeline()

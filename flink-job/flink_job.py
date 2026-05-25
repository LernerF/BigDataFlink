import json
from datetime import datetime
import psycopg2
from kafka import KafkaConsumer
import os
import logging
import time
import signal
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StarSchemaProcessor:
    def __init__(self):
        self.connection = None
        self.running = True
        self.cache = {
            'category': {},
            'pet': {},
            'customer': {},
            'product': {},
            'seller': {},
            'store': {},
            'supplier': {}
        }
        self.processed_messages = set()

    def signal_handler(self, signum, frame):
        logger.info("Received shutdown signal")
        self.running = False

    def connect_db(self):
        max_retries = 30
        for attempt in range(max_retries):
            try:
                self.connection = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'postgres'),
                    port=os.getenv('POSTGRES_PORT', '5432'),
                    database=os.getenv('POSTGRES_DB', 'star_schema'),
                    user=os.getenv('POSTGRES_USER', 'flink_user'),
                    password=os.getenv('POSTGRES_PASSWORD', 'flink_password')
                )
                self.connection.autocommit = False
                logger.info("Connected to PostgreSQL")
                return True
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} - PostgreSQL connection failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    logger.error("Failed to connect to PostgreSQL")
                    return False
        return False

    def safe_int(self, value, default=0):
        if value is None or value == 'None' or value == '':
            return default
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def safe_float(self, value, default=0):
        if value is None or value == 'None' or value == '':
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def safe_string(self, value, default=''):
        if value is None or value == 'None' or value == '':
            return default
        return str(value)

    def get_or_create_category(self, category_name: str) -> int:
        if not category_name or category_name == 'None' or category_name == '':
            return None
        if category_name in self.cache['category']:
            return self.cache['category'][category_name]

        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id FROM dim_category WHERE category = %s", (category_name,))
            result = cursor.fetchone()
            if result:
                category_id = result[0]
            else:
                cursor.execute("INSERT INTO dim_category (category) VALUES (%s) RETURNING id", (category_name,))
                category_id = cursor.fetchone()[0]
            self.connection.commit()
            self.cache['category'][category_name] = category_id
            return category_id

    def get_or_create_pet(self, data: dict) -> int:
        pet_type = self.safe_string(data.get('customer_pet_type'))
        pet_name = self.safe_string(data.get('customer_pet_name'))
        pet_breed = self.safe_string(data.get('customer_pet_breed'))
        email = self.safe_string(data.get('customer_email'))

        if not pet_type:
            return None

        pet_key = f"{pet_type}_{pet_name}_{pet_breed}_{email}"

        if pet_key in self.cache['pet']:
            return self.cache['pet'][pet_key]

        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM dim_pet 
                WHERE pet_type = %s AND pet_name = %s 
                AND pet_breed = %s AND pet_customer_email = %s
            """, (pet_type, pet_name, pet_breed, email))
            result = cursor.fetchone()

            if result:
                pet_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO dim_pet 
                    (pet_type, pet_name, pet_breed, pet_customer_email)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (pet_type, pet_name, pet_breed, email))
                pet_id = cursor.fetchone()[0]

            self.connection.commit()
            self.cache['pet'][pet_key] = pet_id
            return pet_id

    def get_or_create_customer(self, data: dict) -> int:
        email = self.safe_string(data.get('customer_email'))
        if not email:
            return None
        if email in self.cache['customer']:
            return self.cache['customer'][email]

        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id FROM dim_customer WHERE customer_email = %s", (email,))
            result = cursor.fetchone()
            if result:
                customer_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO dim_customer 
                    (customer_first_name, customer_last_name, customer_age, 
                     customer_email, customer_country, customer_postal_code)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    self.safe_string(data.get('customer_first_name')),
                    self.safe_string(data.get('customer_last_name')),
                    self.safe_int(data.get('customer_age'), 0),
                    email,
                    self.safe_string(data.get('customer_country')),
                    self.safe_string(data.get('customer_postal_code'))
                ))
                customer_id = cursor.fetchone()[0]
            self.connection.commit()
            self.cache['customer'][email] = customer_id
            return customer_id

    def get_or_create_product(self, data: dict) -> int:
        product_name = data.get('product_name')
        if not product_name or product_name == 'None' or product_name == '':
            return None

        release_date = None
        expiry_date = None

        if data.get('product_release_date') and data['product_release_date'] not in ['None', '']:
            try:
                release_date = datetime.strptime(data['product_release_date'], '%m/%d/%Y').date()
            except:
                pass

        if data.get('product_expiry_date') and data['product_expiry_date'] not in ['None', '']:
            try:
                expiry_date = datetime.strptime(data['product_expiry_date'], '%m/%d/%Y').date()
            except:
                pass

        product_reviews = data.get('product_reviews')
        if product_reviews is None or product_reviews == 'None' or product_reviews == '':
            product_reviews = 0
        else:
            try:
                product_reviews = int(product_reviews)
            except (ValueError, TypeError):
                product_reviews = 0

        product_key_fields = (
            product_name,
            self.safe_string(data.get('product_category')),
            self.safe_float(data.get('product_price')),
            self.safe_int(data.get('product_quantity')),
            self.safe_float(data.get('product_weight')),
            self.safe_string(data.get('product_color')),
            self.safe_string(data.get('product_size')),
            self.safe_string(data.get('product_brand')),
            self.safe_string(data.get('product_material')),
            self.safe_string(data.get('product_description')),
            self.safe_float(data.get('product_rating')),
            product_reviews,
            str(release_date),
            str(expiry_date)
        )
        product_key = hashlib.md5(str(product_key_fields).encode()).hexdigest()

        if product_key in self.cache['product']:
            return self.cache['product'][product_key]

        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM dim_product 
                WHERE product_name = %s AND product_category = %s 
                AND product_price = %s AND product_quantity = %s
                AND product_weight = %s AND product_color = %s
                AND product_size = %s AND product_brand = %s
                AND product_material = %s AND product_description = %s
                AND product_rating = %s AND product_reviews = %s
                AND (product_release_date = %s OR (product_release_date IS NULL AND %s IS NULL))
                AND (product_expiry_date = %s OR (product_expiry_date IS NULL AND %s IS NULL))
            """, (
                product_name,
                self.safe_string(data.get('product_category')),
                self.safe_float(data.get('product_price')),
                self.safe_int(data.get('product_quantity')),
                self.safe_float(data.get('product_weight')),
                self.safe_string(data.get('product_color')),
                self.safe_string(data.get('product_size')),
                self.safe_string(data.get('product_brand')),
                self.safe_string(data.get('product_material')),
                self.safe_string(data.get('product_description')),
                self.safe_float(data.get('product_rating')),
                product_reviews,
                release_date, release_date,
                expiry_date, expiry_date
            ))
            result = cursor.fetchone()

            if result:
                product_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO dim_product 
                    (product_name, product_category, product_price, product_quantity,
                     product_weight, product_color, product_size, product_brand,
                     product_material, product_description, product_rating, product_reviews,
                     product_release_date, product_expiry_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    product_name,
                    self.safe_string(data.get('product_category')),
                    self.safe_float(data.get('product_price')),
                    self.safe_int(data.get('product_quantity')),
                    self.safe_float(data.get('product_weight')),
                    self.safe_string(data.get('product_color')),
                    self.safe_string(data.get('product_size')),
                    self.safe_string(data.get('product_brand')),
                    self.safe_string(data.get('product_material')),
                    self.safe_string(data.get('product_description')),
                    self.safe_float(data.get('product_rating')),
                    product_reviews,
                    release_date,
                    expiry_date
                ))
                product_id = cursor.fetchone()[0]

            self.connection.commit()
            self.cache['product'][product_key] = product_id
            return product_id

    def get_or_create_seller(self, data: dict) -> int:
        email = self.safe_string(data.get('seller_email'))
        if not email:
            return None
        if email in self.cache['seller']:
            return self.cache['seller'][email]

        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id FROM dim_seller WHERE seller_email = %s", (email,))
            result = cursor.fetchone()
            if result:
                seller_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO dim_seller 
                    (seller_first_name, seller_last_name, seller_email,
                     seller_country, seller_postal_code)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    self.safe_string(data.get('seller_first_name')),
                    self.safe_string(data.get('seller_last_name')),
                    email,
                    self.safe_string(data.get('seller_country')),
                    self.safe_string(data.get('seller_postal_code'))
                ))
                seller_id = cursor.fetchone()[0]
            self.connection.commit()
            self.cache['seller'][email] = seller_id
            return seller_id

    def get_or_create_store(self, data: dict) -> int:
        email = self.safe_string(data.get('store_email'))
        if not email:
            return None
        if email in self.cache['store']:
            return self.cache['store'][email]

        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id FROM dim_store WHERE store_email = %s", (email,))
            result = cursor.fetchone()
            if result:
                store_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO dim_store 
                    (store_name, store_location, store_city, store_state,
                     store_country, store_phone, store_email)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    self.safe_string(data.get('store_name')),
                    self.safe_string(data.get('store_location')),
                    self.safe_string(data.get('store_city')),
                    self.safe_string(data.get('store_state')),
                    self.safe_string(data.get('store_country')),
                    self.safe_string(data.get('store_phone')),
                    email
                ))
                store_id = cursor.fetchone()[0]
            self.connection.commit()
            self.cache['store'][email] = store_id
            return store_id

    def get_or_create_supplier(self, data: dict) -> int:
        email = self.safe_string(data.get('supplier_email'))
        if not email:
            return None
        if email in self.cache['supplier']:
            return self.cache['supplier'][email]

        with self.connection.cursor() as cursor:
            cursor.execute("SELECT id FROM dim_supplier WHERE supplier_email = %s", (email,))
            result = cursor.fetchone()
            if result:
                supplier_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO dim_supplier 
                    (supplier_name, supplier_contact, supplier_email,
                     supplier_phone, supplier_address, supplier_city, supplier_country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    self.safe_string(data.get('supplier_name')),
                    self.safe_string(data.get('supplier_contact')),
                    email,
                    self.safe_string(data.get('supplier_phone')),
                    self.safe_string(data.get('supplier_address')),
                    self.safe_string(data.get('supplier_city')),
                    self.safe_string(data.get('supplier_country'))
                ))
                supplier_id = cursor.fetchone()[0]
            self.connection.commit()
            self.cache['supplier'][email] = supplier_id
            return supplier_id

    def fact_sale_exists(self, data: dict, category_id, pet_id, customer_id,
                         product_id, seller_id, store_id, supplier_id) -> bool:
        sale_date = None
        if data.get('sale_date') and data['sale_date'] not in ['None', '']:
            try:
                sale_date = datetime.strptime(data['sale_date'], '%m/%d/%Y').date()
            except:
                pass

        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM fact_sale 
                WHERE sale_date = %s 
                AND sale_quantity = %s 
                AND sale_total_price = %s
                AND category_id = %s
                AND pet_id = %s
                AND customer_id = %s
                AND product_id = %s
                AND seller_id = %s
                AND store_id = %s
                AND supplier_id = %s
            """, (
                sale_date,
                self.safe_int(data.get('sale_quantity')),
                self.safe_float(data.get('sale_total_price')),
                category_id, pet_id, customer_id, product_id,
                seller_id, store_id, supplier_id
            ))
            return cursor.fetchone() is not None

    def process_message(self, data: dict):
        try:
            category_id = self.get_or_create_category(data.get('pet_category'))
            pet_id = self.get_or_create_pet(data)
            customer_id = self.get_or_create_customer(data)
            product_id = self.get_or_create_product(data)
            seller_id = self.get_or_create_seller(data)
            store_id = self.get_or_create_store(data)
            supplier_id = self.get_or_create_supplier(data)

            if not self.fact_sale_exists(data, category_id, pet_id, customer_id,
                                         product_id, seller_id, store_id, supplier_id):

                sale_date = None
                if data.get('sale_date') and data['sale_date'] not in ['None', '']:
                    try:
                        sale_date = datetime.strptime(data['sale_date'], '%m/%d/%Y').date()
                    except:
                        pass

                with self.connection.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO fact_sale 
                        (sale_date, sale_quantity, sale_total_price,
                         category_id, pet_id, customer_id, product_id,
                         seller_id, store_id, supplier_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        sale_date,
                        self.safe_int(data.get('sale_quantity')),
                        self.safe_float(data.get('sale_total_price')),
                        category_id, pet_id, customer_id, product_id,
                        seller_id, store_id, supplier_id
                    ))
                    self.connection.commit()

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            if self.connection:
                self.connection.rollback()

    def run(self):
        if not self.connect_db():
            logger.error("Failed to connect to database")
            return

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        group_id = 'flink-consumer-group-fixed'

        try:
            consumer = KafkaConsumer(
                os.getenv('KAFKA_TOPIC', 'sales-topic'),
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=30000
            )
            logger.info(f"Connected to Kafka, consuming from topic: {os.getenv('KAFKA_TOPIC', 'sales-topic')}")
            logger.info(f"Using fixed group_id: {group_id}")

            message_count = 0
            for message in consumer:
                if not self.running:
                    break

                msg_key = f"{message.topic}_{message.partition}_{message.offset}"
                if msg_key in self.processed_messages:
                    logger.debug(f"Skipping duplicate message: {msg_key}")
                    continue

                self.processed_messages.add(msg_key)
                self.process_message(message.value)
                message_count += 1

                if message_count % 500 == 0:
                    logger.info(f"Processed {message_count} messages")
                    logger.info(f"Cache sizes - Category: {len(self.cache['category'])}, "
                                f"Customer: {len(self.cache['customer'])}, "
                                f"Product: {len(self.cache['product'])}, "
                                f"Seller: {len(self.cache['seller'])}, "
                                f"Store: {len(self.cache['store'])}, "
                                f"Supplier: {len(self.cache['supplier'])}, "
                                f"Pet: {len(self.cache['pet'])}")

            logger.info(f"Total messages processed: {message_count}")

        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
            raise

        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting Full Star Schema Processor")
    logger.info("=" * 60)

    processor = StarSchemaProcessor()
    processor.run()

DROP TABLE IF EXISTS fact_sale CASCADE;
DROP TABLE IF EXISTS dim_category CASCADE;
DROP TABLE IF EXISTS dim_pet CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;

-- Создание таблиц схемы звезда
CREATE TABLE IF NOT EXISTS dim_category (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_pet (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pet_type VARCHAR NOT NULL,
    pet_name VARCHAR NOT NULL,
    pet_breed VARCHAR NOT NULL,
    pet_customer_email VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_first_name VARCHAR NOT NULL,
    customer_last_name VARCHAR NOT NULL,
    customer_age INTEGER NOT NULL,
    customer_email VARCHAR NOT NULL UNIQUE,
    customer_country VARCHAR NOT NULL,
    customer_postal_code VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_product (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    product_category VARCHAR NOT NULL,
    product_price NUMERIC NOT NULL,
    product_quantity INTEGER NOT NULL,
    product_weight NUMERIC NOT NULL,
    product_color VARCHAR NOT NULL,
    product_size VARCHAR NOT NULL,
    product_brand VARCHAR NOT NULL,
    product_material VARCHAR NOT NULL,
    product_description TEXT NOT NULL,
    product_rating NUMERIC(3,2) NOT NULL,
    product_reviews INTEGER NOT NULL,
    product_release_date DATE NOT NULL,
    product_expiry_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_seller (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seller_first_name VARCHAR NOT NULL,
    seller_last_name VARCHAR NOT NULL,
    seller_email VARCHAR NOT NULL UNIQUE,
    seller_country VARCHAR NOT NULL,
    seller_postal_code VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_store (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_name VARCHAR NOT NULL,
    store_location VARCHAR NOT NULL,
    store_city VARCHAR NOT NULL,
    store_state VARCHAR,
    store_country VARCHAR NOT NULL,
    store_phone VARCHAR NOT NULL,
    store_email VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_name VARCHAR NOT NULL,
    supplier_contact VARCHAR NOT NULL,
    supplier_email VARCHAR NOT NULL UNIQUE,
    supplier_phone VARCHAR NOT NULL,
    supplier_address VARCHAR NOT NULL,
    supplier_city VARCHAR NOT NULL,
    supplier_country VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sale (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sale_date DATE,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12,2),
    category_id bigint REFERENCES dim_category(id),
    pet_id bigint REFERENCES dim_pet(id),
    customer_id bigint REFERENCES dim_customer(id),
    product_id bigint REFERENCES dim_product(id),
    seller_id bigint REFERENCES dim_seller(id),
    store_id bigint REFERENCES dim_store(id),
    supplier_id bigint REFERENCES dim_supplier(id)
);

-- Создание индексов
CREATE INDEX IF NOT EXISTS idx_dim_customer_email ON dim_customer(customer_email);
CREATE INDEX IF NOT EXISTS idx_dim_seller_email ON dim_seller(seller_email);
CREATE INDEX IF NOT EXISTS idx_dim_store_email ON dim_store(store_email);
CREATE INDEX IF NOT EXISTS idx_dim_supplier_email ON dim_supplier(supplier_email);
CREATE INDEX IF NOT EXISTS idx_dim_category_category ON dim_category(category);
CREATE INDEX IF NOT EXISTS idx_dim_pet_all ON dim_pet (pet_type, pet_name, pet_breed, pet_customer_email);
CREATE INDEX IF NOT EXISTS idx_dim_product_all ON dim_product (
    product_name, product_category, product_price, product_quantity,
    product_weight, product_color, product_size, product_brand,
    product_material, product_description, product_rating, product_reviews,
    product_release_date, product_expiry_date
);
#!python config

oltp_conn_string = "postgresql+psycopg2://postgres:nasrul123@localhost:5432/oltp"
warehouse_conn_string = "postgresql+psycopg2://postgres:nasrul123@localhost:5432/dwh"

oltp_tables = {
    "users": "tb_users",
    "payments": "tb_payments",
    "shippers": "tb_shippers",
    "ratings": "tb_ratings",
    "vouchers": "tb_vouchers",
    "orders": "tb_orders",
    "products": "tb_products",
    "product_category": "tb_product_category",
    "order_items": "tb_order_items"
}

warehouse_tables = {
    "users": "dim_user",
    "payments": "dim_payment",
    "shippers": "dim_shipper",
    "ratings": "dim_rating",
    "vouchers": "dim_voucher",
    "orders": "fact_orders",
    "product_category": "dim_product_category",
    "products": "dim_product",
    "order_items": "fact_order_items"
}

dimension_columns = {
    "dim_user": ["user_id", "user_first_name", "user_last_name", "user_gender", "user_address", "user_birthday", "user_join"],
    "dim_payment": ["payment_id", "payment_name", "payment_status"],
    "dim_shipper": ["shipper_id", "shipper_name"],
    "dim_rating": ["rating_id", "rating_level", "rating_status"],
    "dim_voucher": ["voucher_id", "voucher_name", "voucher_price", "voucher_created","user_id"], 
    "fact_orders": ["order_id", "order_date", "user_id", "payment_id", "shipper_id", "order_price","order_discount", "voucher_id", "order_total", "rating_id"],
    "dim_product_category": ["product_category_id", "product_category_name"],
    "dim_product": ["product_id", "product_category_id", "product_name", "product_created", "product_price", "product_discount"],
    "fact_order_items": ["order_item_id", "order_id", "product_id", "order_item_quantity", "product_discount", "product_subdiscount", "product_price", "product_subprice"]
}

ddl_statements = {
    "dim_user": """
        CREATE TABLE IF NOT EXISTS dim_user (
            user_id INT NOT NULL PRIMARY KEY,
            user_first_name VARCHAR(255) NOT NULL,
            user_last_name VARCHAR(255) NOT NULL,
            user_gender VARCHAR(50) NOT NULL,
            user_address VARCHAR(255),
            user_birthday DATE NOT NULL,
            user_join DATE NOT NULL
        );
    """,
    "dim_payment": """
        CREATE TABLE IF NOT EXISTS dim_payment (
            payment_id INT NOT NULL PRIMARY KEY,
            payment_name VARCHAR(255) NOT NULL,
            payment_status BOOLEAN NOT NULL
        );
    """,
    "dim_shipper": """
        CREATE TABLE IF NOT EXISTS dim_shipper (
            shipper_id INT NOT NULL PRIMARY KEY,
            shipper_name VARCHAR(255) NOT NULL
        );
    """,
    "dim_rating": """
        CREATE TABLE IF NOT EXISTS dim_rating (
            rating_id INT NOT NULL PRIMARY KEY,
            rating_level INT NOT NULL,
            rating_status VARCHAR(255) NOT NULL
        );
    """,
    "dim_voucher": """
        CREATE TABLE IF NOT EXISTS dim_voucher (
            voucher_id INT NOT NULL PRIMARY KEY,
            voucher_name VARCHAR(255) NOT NULL,
            voucher_price INT,
            voucher_created DATE NOT NULL,
            user_id INT NOT NULL
        );
    """,
    "fact_orders": """
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INT NOT NULL PRIMARY KEY,
            order_date DATE NOT NULL,
            user_id INT NOT NULL,
            payment_id INT NOT NULL,
            shipper_id INT NOT NULL,
            order_price INT NOT NULL,
            order_discount INT,
            voucher_id INT,
            order_total INT NOT NULL,
            rating_id INT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
            FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id),
            FOREIGN KEY (shipper_id) REFERENCES dim_shipper(shipper_id),
            FOREIGN KEY (voucher_id) REFERENCES dim_voucher(voucher_id),
            FOREIGN KEY (rating_id) REFERENCES dim_rating(rating_id)
        );
    """,
    "dim_product_category": """
        CREATE TABLE IF NOT EXISTS dim_product_category (
            product_category_id INT NOT NULL PRIMARY KEY,
            product_category_name VARCHAR(255) NOT NULL
        );
    """,
    "dim_product": """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id INT NOT NULL PRIMARY KEY,
            product_category_id INT NOT NULL,
            product_name VARCHAR(255) NOT NULL,
            product_created DATE NOT NULL,
            product_price INT NOT NULL,
            product_discount INT,
            FOREIGN KEY (product_category_id) REFERENCES dim_product_category (product_category_id)
        );
    """,
    "fact_order_items": """
        CREATE TABLE IF NOT EXISTS fact_order_items (
            order_item_id INT NOT NULL PRIMARY KEY,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            order_item_quantity INT,
            product_discount INT,
            product_subdiscount INT,
            product_price INT NOT NULL,
            product_subprice INT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES dim_product (product_id),
            FOREIGN KEY (order_id) REFERENCES fact_orders (order_id)
        );
    """
}

ddl_marts = {
    "dim_sales": """
        CREATE TABLE IF NOT EXISTS dm_sales (
            sale_id SERIAL PRIMARY KEY,
            order_id INT NOT NULL,
            order_date DATE NOT NULL,
            user_id INT NOT NULL,
            user_name VARCHAR(255),
            order_item_id INT NOT NULL,
            order_item_quantity INT NOT NULL,
            product_id INT NOT NULL,
            product_name VARCHAR(255),
            product_category_id INT NOT NULL,
            product_category_name VARCHAR(255),
            payment_type VARCHAR(255),
            shipper_name VARCHAR(255),
            order_price INT NOT NULL,
            order_discount INT,
            voucher_name VARCHAR(255),
            order_total INT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
            FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
            FOREIGN KEY (order_item_id) REFERENCES fact_order_items(order_item_id),
            FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
            FOREIGN KEY (product_category_id) REFERENCES dim_product_category(product_category_id)
        );
    """,
    "insert_dm_sales": """
        TRUNCATE TABLE dm_sales; 
        INSERT INTO dm_sales (order_id, order_date, user_id, user_name, payment_type, shipper_name, 
                              order_price, order_discount, voucher_name, order_total, order_item_id,
                              order_item_quantity, product_id, product_name, product_category_id,
                              product_category_name)
        SELECT fo.order_id, fo.order_date, fo.user_id, du.user_first_name || ' ' || du.user_last_name, 
            dp.payment_name, ds.shipper_name, fo.order_price, fo.order_discount, 
            dv.voucher_name, fo.order_total, foi.order_item_id, foi.order_item_quantity, 
            dp2.product_id, dp2.product_name, dpc.product_category_id, 
            dpc.product_category_name 
        FROM fact_orders fo
        INNER JOIN dim_user du ON fo.user_id = du.user_id
        INNER JOIN dim_payment dp ON fo.payment_id = dp.payment_id
        INNER JOIN dim_shipper ds ON fo.shipper_id = ds.shipper_id
        INNER JOIN dim_voucher dv ON fo.voucher_id = dv.voucher_id
        INNER JOIN fact_order_items foi ON fo.order_id = foi.order_id
        INNER JOIN dim_product dp2 ON foi.product_id = dp2.product_id
        INNER JOIN dim_product_category dpc ON dp2.product_category_id = dpc.product_category_id;
    """
}


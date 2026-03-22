# -*- coding: utf-8 -*-
"""
农价数据导入 MySQL（完整版本）
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ==============================
# 1. 配置
# ==============================
CSV_PATH = r"D:\华信实训\project_1\code\xinfadi_price_all.csv"

DB_USER = "root"
DB_PASSWORD = "123456"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "agri_price"

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    connect_args={"charset": "utf8mb4"}
)

# ==============================
# 2. 创建数据库和表
# ==============================
def create_tables():
    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS categories (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(50),
            parent_id INT
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS products (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100),
            category_id INT,
            spec VARCHAR(100),
            unit VARCHAR(20),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS regions (
            id INT PRIMARY KEY AUTO_INCREMENT,
            province VARCHAR(50),
            city VARCHAR(50),
            full_name VARCHAR(100)
        )
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS price_records (
            id INT PRIMARY KEY AUTO_INCREMENT,
            product_id INT,
            region_id INT,
            publish_date DATE,
            min_price FLOAT,
            avg_price FLOAT,
            max_price FLOAT,
            price_spread FLOAT,
            volatility_index FLOAT,
            FOREIGN KEY (product_id) REFERENCES products(id),
            FOREIGN KEY (region_id) REFERENCES regions(id)
        )
        """))

        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))

# ==============================
# 3. 数据清洗
# ==============================
def load_and_clean():
    df = pd.read_csv(CSV_PATH)

    df.columns = [
        "product", "category", "min_price",
        "max_price", "avg_price", "region",
        "unit", "date"
    ]

    # 类型转换
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for col in ["min_price", "max_price", "avg_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 删除空值
    df = df.dropna()

    # 计算价差
    df["price_spread"] = df["max_price"] - df["min_price"]

    return df

# ==============================
# 4. 写入分类表
# ==============================
def insert_categories(df):
    categories = df["category"].drop_duplicates()

    cat_df = pd.DataFrame({
        "name": categories,
        "parent_id": None
    })

    cat_df.to_sql("categories", engine, if_exists="append", index=False)

# ==============================
# 5. 写入产品表
# ==============================
def insert_products(df):
    products = df[["product", "category", "unit"]].drop_duplicates()

    cat_map = pd.read_sql("SELECT * FROM categories", engine)
    cat_dict = dict(zip(cat_map["name"], cat_map["id"]))

    products["category_id"] = products["category"].map(cat_dict)
    products = products.drop(columns=["category"])
    products.rename(columns={
        "product": "name"
    }, inplace=True)

    products.to_sql("products", engine, if_exists="append", index=False)

# ==============================
# 6. 写入产地表
# ==============================
def insert_regions(df):
    regions = df["region"].drop_duplicates()

    region_df = pd.DataFrame({
        "full_name": regions,
        "province": None,
        "city": None
    })

    region_df.to_sql("regions", engine, if_exists="append", index=False)

# ==============================
# 7. 写入价格表
# ==============================
def insert_price_records(df):

    product_map = pd.read_sql("SELECT id, name FROM products", engine)
    region_map = pd.read_sql("SELECT id, full_name FROM regions", engine)

    product_dict = dict(zip(product_map["name"], product_map["id"]))
    region_dict = dict(zip(region_map["full_name"], region_map["id"]))

    df["product_id"] = df["product"].map(product_dict)
    df["region_id"] = df["region"].map(region_dict)

    # 排序（用于波动率）
    df = df.sort_values("date")

    # 计算波动率（7天滚动标准差）
    df["volatility_index"] = (
        df.groupby("product")["avg_price"]
        .rolling(7)
        .std()
        .reset_index(level=0, drop=True)
    )

    price_df = df[[
        "product_id", "region_id", "date",
        "min_price", "avg_price", "max_price",
        "price_spread", "volatility_index"
    ]]

    price_df.rename(columns={
        "date": "publish_date"
    }, inplace=True)

    price_df.to_sql("price", engine, if_exists="append", index=False)


def main():
    print("🚀 开始执行...")
    create_tables()
    df = load_and_clean()
    print("✅ 数据清洗完成")
    insert_categories(df)
    print("✅ 分类表完成")
    insert_products(df)
    print("✅ 产品表完成")
    insert_regions(df)
    print("✅ 产地表完成")
    insert_price_records(df)
    print("✅ 价格表完成")
    print("全部导入完成！")

# ==============================
if __name__ == "__main__":
    main()

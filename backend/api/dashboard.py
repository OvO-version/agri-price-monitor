from flask import Blueprint, jsonify
import pandas as pd
from extensions import engine

dashboard_bp = Blueprint("dashboard", __name__)

@dashboard_bp.route("/dashboard")
def dashboard():
    total = pd.read_sql("SELECT COUNT(*) as cnt FROM products", engine)

    avg_price = pd.read_sql("""
        SELECT AVG(avg_price) as avg_price FROM price_records
    """, engine)

    volatility = pd.read_sql("""
        SELECT pr.name, AVG(p.volatility_index) as vol
        FROM price_records p
        JOIN products pr ON p.product_id = pr.id
        GROUP BY pr.name
        ORDER BY vol DESC
        LIMIT 3
    """, engine)

    return jsonify({
        "total_products": int(total["cnt"][0]),
        "avg_price": float(avg_price["avg_price"][0]),
        "top_volatility": volatility["name"].tolist()
    })

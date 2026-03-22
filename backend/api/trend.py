from flask import Blueprint, request, jsonify
import pandas as pd
from extensions import engine

trend_bp = Blueprint("trend", __name__)

@trend_bp.route("/trend")
def get_trend():
    product = request.args.get("product")
    days = int(request.args.get("days", 30))

    query = """
    SELECT p.publish_date, p.avg_price
    FROM price_records p
    JOIN products pr ON p.product_id = pr.id
    WHERE pr.name = %s
    ORDER BY p.publish_date DESC
    LIMIT %s
    """
    df = pd.read_sql(query, engine, params=(product, days))

    if df.empty:
        return jsonify({"error": "没有数据"})

    df = df.sort_values("publish_date")

    return jsonify({
        "dates": df["publish_date"].astype(str).tolist(),
        "prices": df["avg_price"].tolist()
    })

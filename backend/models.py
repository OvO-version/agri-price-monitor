from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Category(db.Model):
    __tablename__ = 'categories'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    parent_id = db.Column(db.Integer, db.ForeignKey('categories.id'), nullable=True)

    # 关系
    products = db.relationship('Product', backref='category', lazy='dynamic')
    children = db.relationship('Category', backref=db.backref('parent', remote_side=[id]), lazy='dynamic')

    def __repr__(self):
        return f'<Category {self.name}>'

class Product(db.Model):
    __tablename__ = 'products'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey('categories.id'), nullable=False)
    spec = db.Column(db.String(100))
    unit = db.Column(db.String(20))

    # 关系
    prices = db.relationship('Price', backref='product', lazy='dynamic')

    # 索引建议 (需手动在数据库创建)
    # CREATE INDEX idx_product_category ON products(category_id);

    def __repr__(self):
        return f'<Product {self.name}>'

class Region(db.Model):
    __tablename__ = 'regions'
    id = db.Column(db.Integer, primary_key=True)
    province = db.Column(db.String(50))
    city = db.Column(db.String(50))
    full_name = db.Column(db.String(100), nullable=False)

    # 关系
    prices = db.relationship('Price', backref='region', lazy='dynamic')

    def __repr__(self):
        return f'<Region {self.full_name}>'

class Price(db.Model):
    __tablename__ = 'price_records'
    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'), nullable=False)
    region_id = db.Column(db.Integer, db.ForeignKey('regions.id'), nullable=False)
    publish_date = db.Column(db.Date, nullable=False)
    min_price = db.Column(db.Float)
    avg_price = db.Column(db.Float)
    max_price = db.Column(db.Float)
    price_spread = db.Column(db.Float)
    volatility_index = db.Column(db.Float)

    # 索引建议 (需手动在数据库创建)
    # CREATE INDEX idx_price_date ON prices(publish_date);
    # CREATE INDEX idx_price_product ON prices(product_id);
    # CREATE INDEX idx_price_region ON prices(region_id);
    # CREATE INDEX idx_price_compound ON prices(product_id, publish_date);

    def __repr__(self):
        return f'<Price {self.publish_date} product={self.product_id}>'
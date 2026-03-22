import os
import functools
import re
import threading
from datetime import datetime, timedelta

from flask import Flask, render_template, redirect, url_for, request, session, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, desc, and_, text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import atexit

from models import db, Category, Product, Region, Price
from crawler import Crawler

# 日志配置
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 全局变量：爬虫运行状态锁
crawl_running = False
crawl_lock = threading.Lock()

# ---------- 省份名称映射字典 (用于从 full_name 解析标准省份名) ----------
PROVINCE_MAP = {
    '京': '北京', '津': '天津', '沪': '上海', '渝': '重庆',
    '冀': '河北', '晋': '山西', '辽': '辽宁', '吉': '吉林',
    '黑': '黑龙江', '苏': '江苏', '浙': '浙江', '皖': '安徽',
    '闽': '福建', '赣': '江西', '鲁': '山东', '豫': '河南',
    '鄂': '湖北', '湘': '湖南', '粤': '广东', '桂': '广西',
    '琼': '海南', '川': '四川', '贵': '贵州', '云': '云南',
    '藏': '西藏', '陕': '陕西', '甘': '甘肃', '青': '青海',
    '宁': '宁夏', '新': '新疆', '台': '台湾', '港': '香港', '澳': '澳门'
}

def extract_province(full_name):
    """从 full_name 提取标准省份名称"""
    if not full_name:
        return None
    # 如果 full_name 已经是完整名称（如 '山东'），直接返回
    if full_name in PROVINCE_MAP.values() or full_name in ['北京', '天津', '上海', '重庆']:
        return full_name
    # 尝试匹配缩写
    for abbr, name in PROVINCE_MAP.items():
        if abbr in full_name:
            return name
    return None

# ---------- 登录保护装饰器 ----------
def login_required(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ---------- 应用工厂 ----------
def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')

    # 数据库配置
    app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:123456@localhost:3306/agri_price'
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_size': 10,
        'pool_recycle': 3600,
        'max_overflow': 20,
        'connect_args': {'charset': 'utf8mb4'}  # 通过 connect_args 设置编码
    }

    db.init_app(app)

    # ---------- 初始化定时爬虫任务 ----------
    def start_crawler_scheduler():
        """启动定时爬虫调度器"""
        scheduler = BackgroundScheduler()

        # 每天凌晨1点执行爬虫（避开网站高峰）
        scheduler.add_job(
            func=daily_crawl_job,
            trigger=CronTrigger(hour=1, minute=0),
            id='xinfadi_daily_crawl',
            name='新发地每日价格爬取',
            replace_existing=True
        )

        scheduler.start()
        logger.info("定时爬虫调度器已启动")

        # 应用退出时关闭调度器
        atexit.register(lambda: scheduler.shutdown())

    def daily_crawl_job():
        """每日爬取任务"""
        with app.app_context():
            db_config = {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': '123456',
                'database': 'agri_price'
            }
            crawler = Crawler(db_config)
            crawler.run_daily()

    # 只在主进程中启动调度器
    if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        start_crawler_scheduler()

    # ---------- 认证路由 ----------
    @app.route('/login', methods=['GET', 'POST'])
    def login():
        error = None
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            if username == 'admin' and password == '123456':
                session['user'] = username
                return redirect(url_for('dashboard'))
            else:
                error = '用户名或密码错误'
        return render_template('login.html', error=error)

    @app.route('/logout')
    def logout():
        session.pop('user', None)
        return redirect(url_for('login'))

    @app.route('/')
    def root():
        return redirect(url_for('login'))

    # ---------- 受保护主界面 ----------
    @app.route('/dashboard')
    @login_required
    def dashboard():
        return render_template('dashboard.html', username=session['user'])

    # ---------- 一键触发爬虫接口 ----------
    @app.route('/api/trigger-crawl', methods=['POST'])
    @login_required
    def trigger_crawl():
        global crawl_running, crawl_lock
        with crawl_lock:
            if crawl_running:
                return jsonify({'status': 'running', 'message': '爬虫正在运行中，请稍后刷新'})
            crawl_running = True

        def run_crawl():
            global crawl_running
            try:
                db_config = {
                    'host': 'localhost',
                    'port': 3306,
                    'user': 'root',
                    'password': '123456',
                    'database': 'agri_price'
                }
                crawler = Crawler(db_config)
                crawler.run_daily()
            except Exception as e:
                app.logger.error(f"爬虫执行出错: {e}", exc_info=True)
            finally:
                with crawl_lock:
                    crawl_running = False

        thread = threading.Thread(target=run_crawl)
        thread.daemon = True
        thread.start()
        return jsonify({'status': 'started', 'message': '开始爬取最新数据，稍后自动刷新'})

    # ---------- API: 地图热力图数据 ----------
    @app.route('/api/map_data')
    @login_required
    def api_map_data():
        latest_date = db.session.query(func.max(Price.publish_date)).scalar()
        if not latest_date:
            latest_date = datetime.now().date()

        results = db.session.query(
            Region.id,
            Region.full_name,
            func.avg(Price.avg_price).label('avg_price')
        ).join(Price, Price.region_id == Region.id)\
         .filter(Price.publish_date == latest_date)\
         .group_by(Region.id).all()

        map_data = []
        for r in results:
            province = extract_province(r.full_name)
            if province:
                map_data.append({
                    'name': province,
                    'value': round(r.avg_price, 2)
                })
            else:
                map_data.append({
                    'name': r.full_name,
                    'value': round(r.avg_price, 2)
                })

        aggregated = {}
        for item in map_data:
            name = item['name']
            if name in aggregated:
                aggregated[name] = (aggregated[name] + item['value']) / 2
            else:
                aggregated[name] = item['value']

        final_data = [{'name': k, 'value': round(v, 2)} for k, v in aggregated.items()]
        return jsonify(final_data)

    # ---------- API: 最新价格表格（分页） ----------
    @app.route('/api/latest_prices')
    @login_required
    def api_latest_prices():
        page = request.args.get('page', 1, type=int)
        per_page = 10
        sort_col = request.args.get('sort', 'publish_date')
        sort_dir = request.args.get('dir', 'desc')

        latest_date = db.session.query(func.max(Price.publish_date)).scalar()
        if not latest_date:
            return jsonify({'data': [], 'pagination': {'page': page, 'total': 0, 'pages': 0}, 'total_products': 0})

        total_products = db.session.query(func.count(func.distinct(Product.id))).scalar()

        base_query = db.session.query(
            Product.name.label('product_name'),
            func.avg(Price.avg_price).label('avg_price'),
            func.min(Price.min_price).label('min_price'),
            func.max(Price.max_price).label('max_price'),
            func.max(Region.full_name).label('region_name'),
            func.max(Product.spec).label('spec'),
            func.max(Product.unit).label('unit')
        ).join(Product, Product.id == Price.product_id) \
         .join(Region, Region.id == Price.region_id) \
         .filter(Price.publish_date == latest_date) \
         .group_by(Product.name)

        if sort_col == 'product':
            order_column = Product.name
        elif sort_col == 'province':
            order_column = Region.full_name
        elif sort_col == 'min_price':
            order_column = Price.min_price
        elif sort_col == 'avg_price':
            order_column = Price.avg_price
        elif sort_col == 'max_price':
            order_column = Price.max_price
        else:
            order_column = Price.publish_date

        if sort_dir == 'desc':
            base_query = base_query.order_by(desc(order_column))
        else:
            base_query = base_query.order_by(order_column)

        total = base_query.count()
        items = base_query.limit(per_page).offset((page-1)*per_page).all()

        data = [{
            'product': row.product_name,
            'province': row.region_name,
            'spec': row.spec or '-',
            'unit': row.unit or '-',
            'min_price': row.min_price,
            'avg_price': row.avg_price,
            'max_price': row.max_price
        } for row in items]

        pagination = {
            'page': page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        }
        return jsonify({
            'data': data,
            'pagination': pagination,
            'total_products': total_products
        })

    # ---------- API: 价格异常预警雷达 ----------
    @app.route('/api/price_alerts')
    @login_required
    def api_price_alerts():
        threshold = request.args.get('threshold', 20, type=float)
        latest_date = db.session.query(func.max(Price.publish_date)).scalar()
        if not latest_date:
            return jsonify({'alerts': []})

        seven_days_ago = latest_date - timedelta(days=7)

        product_names = db.session.query(Product.name).join(Price).filter(
            Price.publish_date == latest_date).distinct().all()
        product_names = [p[0] for p in product_names]

        alerts = []
        for name in product_names:
            today_price = db.session.query(func.avg(Price.avg_price)) \
                .join(Product, Product.id == Price.product_id) \
                .filter(Product.name == name, Price.publish_date == latest_date).scalar()
            if not today_price:
                continue

            avg_7d = db.session.query(func.avg(Price.avg_price)) \
                .join(Product, Product.id == Price.product_id) \
                .filter(Product.name == name,
                        Price.publish_date >= seven_days_ago,
                        Price.publish_date < latest_date).scalar()
            if not avg_7d:
                continue

            change = ((today_price - avg_7d) / avg_7d) * 100
            if abs(change) >= threshold:
                reason = "价格上涨" if change > 0 else "集中上市"
                alerts.append({
                    'product': name,
                    'change': round(change, 1),
                    'type': 'rise' if change > 0 else 'fall',
                    'reason': reason
                })

        alerts.sort(key=lambda x: abs(x['change']), reverse=True)
        return jsonify({'alerts': alerts})

    # ---------- API: 今日最佳买入排行榜 ----------
    @app.route('/api/best_buy')
    @login_required
    def api_best_buy():
        latest_date = db.session.query(func.max(Price.publish_date)).scalar()
        if not latest_date:
            return jsonify({'best_buy': []})

        product_names = db.session.query(Product.name).join(Price).filter(
            Price.publish_date == latest_date).distinct().all()
        product_names = [p[0] for p in product_names]

        results = []
        for name in product_names:
            today_avg = db.session.query(func.avg(Price.avg_price)) \
                .join(Product, Product.id == Price.product_id) \
                .filter(Product.name == name, Price.publish_date == latest_date).scalar()
            if not today_avg:
                continue

            volatility = db.session.query(func.stddev(Price.avg_price)) \
                             .join(Product, Product.id == Price.product_id) \
                             .filter(Product.name == name).scalar() or 1.0

            last_year_date = latest_date - timedelta(days=365)
            last_year_avg = db.session.query(func.avg(Price.avg_price)) \
                .join(Product, Product.id == Price.product_id) \
                .filter(Product.name == name,
                        func.month(Price.publish_date) == func.month(latest_date),
                        func.year(Price.publish_date) == func.year(last_year_date)).scalar()
            if not last_year_avg:
                last_year_avg = today_avg
            season_factor = today_avg / last_year_avg

            price_score = max(0, 40 - today_avg)
            vol_score = max(0, 30 - volatility * 5)
            season_score = (1 - min(season_factor, 2) + 1) * 15
            total_score = price_score + vol_score + season_score

            trend = []
            for i in range(6, -1, -1):
                d = latest_date - timedelta(days=i)
                p = db.session.query(func.avg(Price.avg_price)) \
                    .join(Product, Product.id == Price.product_id) \
                    .filter(Product.name == name, Price.publish_date == d).scalar()
                trend.append(round(p, 2) if p else None)

            results.append({
                'product': name,
                'price': round(today_avg, 2),
                'score': round(total_score),
                'trend': trend,
                'volatility': round(volatility, 2),
                'season_factor': round(season_factor, 2)
            })

        results.sort(key=lambda x: x['score'], reverse=True)
        return jsonify({'best_buy': results[:5]})

    # ---------- API: 季节性热力图 ----------
    @app.route('/api/seasonal_heatmap')
    @login_required
    def api_seasonal_heatmap():
        hot_products = db.session.query(
            Product.name,
            func.count(Price.id).label('cnt')
        ).join(Price, Price.product_id == Product.id) \
         .group_by(Product.name) \
         .order_by(func.count(Price.id).desc()) \
         .limit(10).all()

        product_names = [p.name for p in hot_products]

        months = list(range(1, 13))
        result_values = []

        for prod_name in product_names:
            monthly = db.session.query(
                func.month(Price.publish_date).label('month'),
                func.avg(Price.avg_price).label('avg_price')
            ).join(Product, Product.id == Price.product_id) \
             .filter(Product.name == prod_name) \
             .group_by(func.month(Price.publish_date)) \
             .order_by('month').all()
            month_prices = [None] * 12
            for m in monthly:
                month_prices[m.month - 1] = round(m.avg_price, 2)
            result_values.append(month_prices)

        return jsonify({
            'products': product_names,
            'months': months,
            'values': result_values
        })

    # ---------- API: 趋势分析 ----------
    @app.route('/api/trend_analysis')
    @login_required
    def api_trend_analysis():
        veg_name = request.args.get('vegetable', '白菜')
        time_range = request.args.get('range', 'month')

        product = Product.query.filter_by(name=veg_name).first()
        if not product:
            return jsonify({'error': '产品不存在'}), 404

        product_id = product.id
        end_date = datetime.now().date()
        if time_range == 'month':
            start_date = end_date - timedelta(days=30)
            group_by = func.date_format(Price.publish_date, '%Y-%m-%d')
        else:
            start_date = end_date - timedelta(days=365)
            group_by = func.date_format(Price.publish_date, '%Y-%m')

        results = db.session.query(
            group_by.label('date_label'),
            func.avg(Price.avg_price).label('avg_price'),
            func.max(Price.max_price).label('max_price'),
            func.min(Price.min_price).label('min_price'),
            func.stddev(Price.avg_price).label('volatility')
        ).filter(
            Price.product_id == product_id,
            Price.publish_date >= start_date,
            Price.publish_date <= end_date
        ).group_by('date_label').order_by('date_label').all()

        if not results:
            dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(0, 31, 3)]
            prices = [round(2.5 + i*0.1, 2) for i in range(len(dates))]
            chart_data = {'dates': dates, 'prices': prices}
            ai_text = f"近一个月无{veg_name}数据，此为基础模拟趋势。"
        else:
            dates = [r.date_label for r in results]
            prices = [round(r.avg_price, 2) for r in results]
            chart_data = {'dates': dates, 'prices': prices}

            max_price = max(r.max_price for r in results)
            min_price = min(r.min_price for r in results)
            avg_price = sum(r.avg_price for r in results) / len(results)
            volatility = sum(r.volatility or 0 for r in results) / len(results) if any(r.volatility for r in results) else 0

            if len(prices) >= 2:
                change = ((prices[-1] - prices[0]) / prices[0]) * 100
                trend = "上升" if change > 2 else ("下降" if change < -2 else "平稳")
            else:
                trend = "平稳"
            ai_text = (f"过去{'一个月' if time_range=='month' else '一年'}，{veg_name}价格呈{trend}趋势。"
                       f"最高价{max_price:.2f}元，最低价{min_price:.2f}元，均价{avg_price:.2f}元。"
                       f"波动率{volatility:.2f}。建议" + ("观望" if trend == '上升' else "适时采购"))

        return jsonify({
            'chart_data': chart_data,
            'ai_insight': ai_text
        })

    # 调试打印路由
    with app.app_context():
        print("=== 已注册路由 ===")
        for rule in app.url_map.iter_rules():
            print(rule)

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
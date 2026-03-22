"""
新发地农产品价格爬虫
自动抓取每日最新菜价并存入数据库
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import logging
from sqlalchemy import create_engine, text
import json

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Crawler:
    """新发地市场爬虫"""

    # 一级分类映射（可根据需要扩展）
    CATEGORY_MAP = {
        '蔬菜': 1,
        '水果': 2,
        '肉禽蛋': 3,
        '水产': 4,
        '粮油': 5,
        '豆制品': 6,
        '调料': 7
    }

    def __init__(self, db_config):
        """
        初始化爬虫
        :param db_config: 数据库配置字典
        """
        self.db_config = db_config
        self.engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
            f"charset=utf8mb4"
        )
        self.base_url = "http://www.xinfadi.com.cn/getPriceData.html"
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'www.xinfadi.com.cn',
            'Origin': 'http://www.xinfadi.com.cn',
            'Referer': 'http://www.xinfadi.com.cn/priceDetail.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

    def fetch_page(self, page=1, start_date=None, end_date=None, category_id='', prod_name=''):
        """
        抓取单页数据
        :param page: 页码
        :param start_date: 开始日期 (YYYY/MM/DD)
        :param end_date: 结束日期 (YYYY/MM/DD)
        :param category_id: 分类ID
        :param prod_name: 产品名称
        :return: 解析后的数据列表
        """
        # 处理日期参数
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y/%m/%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y/%m/%d')

        form_data = {
            'limit': 20,
            'current': page,
            'pubDateStartTime': start_date,
            'pubDateEndTime': end_date,
            'prodPcatid': category_id,  # 一级分类
            'prodCatid': '',  # 二级分类
            'prodName': prod_name
        }

        try:
            response = requests.post(
                self.base_url,
                data=form_data,
                headers=self.headers,
                timeout=10
            )
            response.encoding = 'utf-8'

            if response.status_code == 200:
                data = response.json()
                if data and 'list' in data:
                    logger.debug(f"第{page}页抓取成功，获取到{len(data['list'])}条数据")
                    return data['list']
                else:
                    logger.warning(f"第{page}页返回数据为空")
                    return []
            else:
                logger.warning(f"第{page}页请求失败，状态码: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"抓取第{page}页时出错: {e}")
            return []

    def fetch_all_pages(self, start_date=None, end_date=None, max_pages=100):
        """
        抓取所有页数据（自动翻页直到没有数据）
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param max_pages: 最大页数限制，避免死循环
        :return: 所有数据列表
        """
        all_data = []
        page = 1

        while page <= max_pages:
            logger.info(f"正在抓取第{page}页...")
            page_data = self.fetch_page(page, start_date, end_date)

            if not page_data:
                logger.info(f"第{page}页无数据，抓取结束")
                break

            all_data.extend(page_data)

            # 如果这一页数据少于20条，说明是最后一页
            if len(page_data) < 20:
                logger.info(f"第{page}页数据少于20条，视为最后一页")
                break

            page += 1
            # 随机延时，避免请求过快
            time.sleep(random.uniform(0.5, 1.5))

        logger.info(f"抓取完成，共获取{len(all_data)}条数据")
        return all_data

    def fetch_today_data(self):
        """抓取今日数据"""
        today = datetime.now().strftime('%Y/%m/%d')
        logger.info(f"开始抓取今日({today})数据")
        return self.fetch_all_pages(
            start_date=today,
            end_date=today,
            max_pages=50  # 今日数据通常不会太多页
        )

    def fetch_date_range(self, days=7):
        """
        抓取指定天数的数据
        :param days: 天数
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        start_str = start_date.strftime('%Y/%m/%d')
        end_str = end_date.strftime('%Y/%m/%d')

        logger.info(f"开始抓取 {start_str} 至 {end_str} 的数据")
        return self.fetch_all_pages(
            start_date=start_str,
            end_date=end_str,
            max_pages=500  # 历史数据可能较多
        )

    def save_to_database(self, data):
        """
        将抓取的数据保存到数据库
        :param data: 从接口获取的原始数据列表
        """
        if not data:
            logger.warning("没有数据可保存")
            return 0

        # 转换为DataFrame
        df = pd.DataFrame(data)

        # 重命名列以匹配数据库
        df.rename(columns={
            'prodName': 'product',
            'lowPrice': 'min_price',
            'avgPrice': 'avg_price',
            'highPrice': 'max_price',
            'place': 'region',
            'unitInfo': 'unit',
            'pubDate': 'publish_date'
        }, inplace=True)

        # 处理日期格式
        df['publish_date'] = pd.to_datetime(df['publish_date']).dt.date

        # 价格转换为浮点数
        for col in ['min_price', 'avg_price', 'max_price']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # 计算价差
        df['price_spread'] = df['max_price'] - df['min_price']
        df['volatility_index'] = 0.0  # 初始为0，后续计算

        # 删除空值
        df.dropna(subset=['product', 'avg_price'], inplace=True)

        # 去重（同一天同一产品同一产地）
        df.drop_duplicates(subset=['product', 'region', 'publish_date'], keep='first', inplace=True)

        saved_count = 0

        with self.engine.connect() as conn:
            # 开启事务
            trans = conn.begin()
            try:
                # 1. 处理分类 - 先确保基础分类存在
                for cat_name, cat_id in self.CATEGORY_MAP.items():
                    conn.execute(
                        text("INSERT IGNORE INTO categories (id, name) VALUES (:id, :name)"),
                        {"id": cat_id, "name": cat_name}
                    )

                # 2. 获取现有产品映射
                products_df = pd.read_sql(
                    "SELECT id, name, spec FROM products",
                    conn
                )
                product_dict = {}
                for _, row in products_df.iterrows():
                    key = row['name']
                    if key not in product_dict:
                        product_dict[key] = row['id']

                # 3. 处理新产品
                new_products = []
                for product_name in df['product'].unique():
                    if product_name not in product_dict:
                        # 猜测分类（根据名称关键词，简化处理）
                        category_id = 1  # 默认蔬菜
                        if any(keyword in product_name for keyword in ['苹果', '梨', '香蕉', '橙']):
                            category_id = 2  # 水果
                        elif any(keyword in product_name for keyword in ['猪', '牛', '羊', '鸡', '蛋']):
                            category_id = 3  # 肉禽蛋
                        elif any(keyword in product_name for keyword in ['鱼', '虾', '蟹']):
                            category_id = 4  # 水产

                        new_products.append({
                            'name': product_name,
                            'category_id': category_id,
                            'spec': '',
                            'unit': df[df['product'] == product_name].iloc[0].get('unit', '公斤')
                        })

                if new_products:
                    new_products_df = pd.DataFrame(new_products)
                    new_products_df.to_sql(
                        'products',
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    # 重新获取产品映射
                    products_df = pd.read_sql(
                        "SELECT id, name FROM products",
                        conn
                    )
                    product_dict = dict(zip(products_df['name'], products_df['id']))

                # 4. 获取现有产地映射
                regions_df = pd.read_sql(
                    "SELECT id, full_name FROM regions",
                    conn
                )
                region_dict = dict(zip(regions_df['full_name'], regions_df['id']))

                # 5. 处理新产地
                new_regions = []
                for region_name in df['region'].unique():
                    if region_name and region_name not in region_dict:
                        # 尝试从产地名称中提取省份
                        province = None
                        for p in ['北京', '天津', '河北', '山西', '内蒙古', '辽宁', '吉林',
                                  '黑龙江', '上海', '江苏', '浙江', '安徽', '福建', '江西',
                                  '山东', '河南', '湖北', '湖南', '广东', '广西', '海南',
                                  '重庆', '四川', '贵州', '云南', '西藏', '陕西', '甘肃',
                                  '青海', '宁夏', '新疆']:
                            if p in region_name:
                                province = p
                                break

                        new_regions.append({
                            'full_name': region_name,
                            'province': province,
                            'city': None
                        })

                if new_regions:
                    new_regions_df = pd.DataFrame(new_regions)
                    new_regions_df.to_sql(
                        'regions',
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    regions_df = pd.read_sql(
                        "SELECT id, full_name FROM regions",
                        conn
                    )
                    region_dict = dict(zip(regions_df['full_name'], regions_df['id']))

                # 6. 映射ID
                df['product_id'] = df['product'].map(product_dict)
                df['region_id'] = df['region'].map(region_dict)

                # 7. 准备价格记录
                price_df = df[[
                    'product_id', 'region_id', 'publish_date',
                    'min_price', 'avg_price', 'max_price',
                    'price_spread', 'volatility_index'
                ]].dropna(subset=['product_id', 'region_id'])

                # 8. 批量插入价格记录
                if not price_df.empty:
                    price_df.to_sql(
                        'price_records',
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    saved_count = len(price_df)
                    logger.info(f"成功保存 {saved_count} 条价格记录")

                trans.commit()

            except Exception as e:
                trans.rollback()
                logger.error(f"保存数据时出错: {e}")
                raise

        return saved_count

    def run_daily(self):
        """每日运行任务"""
        logger.info("=" * 50)
        logger.info("开始执行每日爬虫任务")

        # 抓取今日数据
        data = self.fetch_today_data()

        if data:
            saved = self.save_to_database(data)
            logger.info(f"今日任务完成，新增 {saved} 条记录")
        else:
            logger.warning("今日无新数据")

        # 可选：每周全量更新一次历史数据
        if datetime.now().weekday() == 0:  # 周一
            logger.info("执行周度全量更新...")
            weekly_data = self.fetch_date_range(days=30)
            if weekly_data:
                self.save_to_database(weekly_data)

        logger.info("每日爬虫任务结束")
        logger.info("=" * 50)


# 独立运行时的入口
if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '123456',  # 请修改为您的密码
        'database': 'agri_price'
    }

    crawler = XinFaDiCrawler(db_config)

    # 测试抓取今日数据
    crawler.run_daily()

    # 如果需要抓取历史数据，可以这样：
    # data = crawler.fetch_date_range(days=30)
    # crawler.save_to_database(data)
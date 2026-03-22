from sqlalchemy import create_engine
from config import DB_URI   # 直接导入同级模块

engine = create_engine(DB_URI)
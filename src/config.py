import os
import minio
from io import BytesIO

DEBUG = True

version_info = "v1.3"
app_env = os.getenv("ENV", 'dev').lower()

# 配置使用的决策引擎
# 获取环境变量，如果没有返回'http://192.168.1.20:8091/rest/S1Public'
STRATEGY_URL = os.getenv('STRATEGY_URL',
                         'http://192.168.1.20:8091/rest/S1Public')

# OPEN_STRATEGY_URL = os.getenv('OPEN_STRATEGY_URL')
OPEN_STRATEGY_URL = os.getenv('OPEN_STRATEGY_URL', "http://192.168.1.36:8088/rest/S1Public")

EUREKA_SERVER = os.getenv('EUREKA_SERVER', 'http://192.168.1.27:8030/eureka/')

"""
通过环境变量配置不同部署环境的是数据库
"""

GEARS_DB = {
    'user': os.getenv('DB_USER', 'admin'),
    'pw': os.getenv('DB_PW', 'admin'),
    'host': os.getenv('DB_HOST', '62.107.231.100'),
    'port': os.getenv('DB_PORT', 3880),
    'db': os.getenv('DB_NAME', 'gears'),
}

# GEARS_DB = {
#     'user': os.getenv('DB_USER', 'root'),
#     'pw': os.getenv('DB_PW', 'magfin2021'),
#     'host': os.getenv('DB_HOST', '192.168.2.21'),
#     'port': os.getenv('DB_PORT', 3306),
#     'db': os.getenv('DB_NAME', 'gears_lpt'),
# }

MINIO_CONF = {
    'endpoint': os.getenv("MINIO_ENDPOINT", '62.107.233.90:9000'),
    'access_key': os.getenv("MINIO_ACCESS_KEY", 'minio'),
    'secret_key': os.getenv("MINIO_SECRET_KEY", 'minio123456'),
    'secure': False
}


# minio client upload
def upload_minio(file_name: str, file_path, bucket='transparser'):
    client = minio.Minio(**MINIO_CONF)
    client.fput_object(bucket_name=bucket, object_name=file_name, file_path=file_path, content_type='application/text')


# minio client download
def download_minio(file_path, bucket='transparser'):
    client = minio.Minio(**MINIO_CONF)
    if not client.bucket_exists(bucket):
        return None
    data = client.get_object(bucket, file_path)
    file = BytesIO(data.read())
    return file

FROM registry.cn-shanghai.aliyuncs.com/transformer/pipes_qdns_base:1.1

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY . /usr/src/app

RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install minio==7.1.0
RUN pip3 install xmltodict==0.13.0

ENV PYTHONUNBUFFERED 1
EXPOSE 5000

CMD ["gunicorn", "--workers=50", "--bind=0.0.0.0:8010", "app:app", "-t=600"]

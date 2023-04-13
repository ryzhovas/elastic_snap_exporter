# elastic_snap_exporter
snapshots and shards info

Fast docker start
```
docker run -p 9091:9091 teamat/es_custom_exporter:v1 -u=http://elasticsearch_host:9200
```

Dockerfile
```
FROM python:3.11

ADD code /code
RUN pip install -r /code/pip-requirements.txt

WORKDIR /code
ENV PYTHONPATH '/code/'

EXPOSE 9091
ENTRYPOINT [ "python", "es_custom_exporter.py" ]
```

DIR code
```
pip-requirements.txt
es_custom_exporter.py (main.py)
```

pip-requirements.txt
```
requests
prometheus_client
argparse
```

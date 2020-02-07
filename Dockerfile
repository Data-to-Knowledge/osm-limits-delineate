FROM python:3.7-slim

COPY requirements.txt utils.py process_limits.py process_allocation.py process_waps.py main.py ./

ENV TZ='Pacific/Auckland'

RUN apt-get update && apt-get install -y unixodbc-dev gcc g++ libspatialindex-dev python-rtree

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py", "parameters.yml"]

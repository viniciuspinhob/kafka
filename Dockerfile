FROM python:3.11.4-slim 

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY util util
COPY src src
COPY App.py App.py

CMD ["python", "App.py"]

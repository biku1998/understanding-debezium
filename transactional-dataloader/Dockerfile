FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./e-commerce-data/*.csv .

COPY data-loader.py .

CMD ["python", "data-loader.py"]
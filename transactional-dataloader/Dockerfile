FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all CSV data files
COPY ./e-commerce-data/*.csv ./e-commerce-data/

# Copy the data loader
COPY data-loader.py .

CMD ["python", "data-loader.py"]
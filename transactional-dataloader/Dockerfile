FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all CSV data files
COPY ./e-commerce-data/*.csv ./e-commerce-data/

# Copy the realistic data loader
COPY realistic-data-loader.py .

CMD python realistic-data-loader.py --speed ${SPEED_MULTIPLIER:-10.0} --duration ${DURATION_HOURS:-2.0} $([ "$CLEAR_DATA" = "true" ] && echo "--clear" || echo "")
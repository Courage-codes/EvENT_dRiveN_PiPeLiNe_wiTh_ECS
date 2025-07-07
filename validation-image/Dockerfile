FROM python:3.9-slim

WORKDIR /app

COPY validation-image/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY validation-image/validation.py .

CMD ["python", "validation.py"]

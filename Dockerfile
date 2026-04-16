FROM python:3.10-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8090

# Default to running the API. (Dask will override this automatically for the workers).
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8090"]
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY coordinator/coordinator_node.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "coordinator_node.py"]

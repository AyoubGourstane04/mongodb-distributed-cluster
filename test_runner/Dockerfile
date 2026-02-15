FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install pymongo prometheus-client

# Copy the script
COPY app.py .

# Run unbuffered so logs show up immediately in Docker
CMD ["python", "-u", "app.py"]
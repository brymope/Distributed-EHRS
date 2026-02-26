FROM python:3.10-slim

WORKDIR /app

# Copy requirements and install
# (Ensure you have a requirements.txt in each folder)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Ensure the db folder exists for persistence
RUN mkdir -p db

# We will define the start command in docker-compose
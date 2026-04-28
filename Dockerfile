# Use a lightweight, official Python base image
FROM python:3.11-slim

# Set environment variables to optimize Python execution in containers
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files to disk
# PYTHONUNBUFFERED: Ensures logs are sent directly to the terminal without buffering
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies required for building Python packages (like numpy or psycopg2)
# The apt-get cache is cleared afterwards to keep the image size small
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only the requirements file first to leverage Docker/Podman layer caching
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code into the container
COPY . .

# Create the directory where SQLite will store the database file
RUN mkdir -p /app/data

# Expose the port Uvicorn will listen on
EXPOSE 8000

# Command to run the application using Uvicorn
CMD ["python", "-m", "uvicorn", "api_layer.main:app", "--host", "0.0.0.0", "--port", "8000"]
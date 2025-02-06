# Use an official lightweight Python image
FROM python:3.11

# Install system dependencies for ODBC
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    odbcinst \
    odbc-postgresql \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the function code
COPY Cloud Run /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV GOOGLE_CLOUD_PROJECT="danubehome-dlf-prd"

# Run the function framework
CMD ["python", "main.py"]

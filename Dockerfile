# Use lightweight Python
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies (optional but safe)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project
COPY . .

# Expose Cloud Run port
EXPOSE 8080

# Streamlit configuration so it doesn’t try to open a browser
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_PORT=8080
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Launch the Streamlit app
CMD ["streamlit", "run", "app.py"]

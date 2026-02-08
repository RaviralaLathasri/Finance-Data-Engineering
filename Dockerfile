# 1. Base image
FROM python:3.12-slim

# 2. Set working directory inside container
WORKDIR /app

# 3. Copy requirements first (for caching)
COPY requirements.txt .

# 4. Install all Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy entire project into container
COPY . .

# 6. Default command (can override in docker-compose)
CMD ["bash"]

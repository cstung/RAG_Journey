# --- Stage 1: Build Frontend ---
FROM node:20-slim AS frontend-builder
WORKDIR /app/frontend
# Copy package files for better caching
COPY frontend/package*.json ./
RUN npm install
# Copy the rest of the frontend and build
COPY frontend/ ./
RUN npm run build

# --- Stage 2: Final Image ---
FROM python:3.11-slim
WORKDIR /app

# Install system deps (for ChromaDB, OCR, and legacy .doc support)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl tesseract-ocr tesseract-ocr-vie poppler-utils antiword && \
    rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend application code
COPY backend/ /app/

# Copy the built React frontend to a location that won't be clobbered by the /app volume
COPY --from=frontend-builder /app/frontend/dist /frontend_dist

# Ensure admin UI is also in that protected location
COPY backend/static/admin_datasets.html /frontend_dist/admin_datasets.html

EXPOSE 8000

# Start the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

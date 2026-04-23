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

# Copy the built React frontend from Stage 1 into the backend's static directory
# This ensures FastAPI serves the React app instead of the legacy Vanilla JS files
COPY --from=frontend-builder /app/frontend/dist /app/static

EXPOSE 8000

# Start the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

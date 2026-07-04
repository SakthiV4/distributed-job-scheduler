# Stage 1: Build the React frontend
FROM node:20-alpine AS frontend-builder
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Stage 2: Build the FastAPI backend
FROM python:3.12-slim
WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend files
COPY backend/ .

# Copy built frontend assets from Stage 1 into the expected location
COPY --from=frontend-builder /frontend/dist /app/frontend/dist

ENV PYTHONPATH=/app

CMD ["sh", "-c", "alembic upgrade head && python app/seed.py && python app/seed_pipeline_jobs.py && uvicorn app.main:app --host 0.0.0.0 --port 8000"]

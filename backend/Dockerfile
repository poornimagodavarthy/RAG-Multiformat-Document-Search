FROM python:3.12.12 AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install build dependencies for WeasyPrint
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libpangoft2-1.0-0 \
    libgdk-pixbuf-2.0-0 \
    libffi-dev \
    shared-mime-info \
    libcairo2 \
    libcairo2-dev \
    libgobject-2.0-0 \
    libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv .venv
COPY requirements.txt ./
RUN .venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.12.12-slim

WORKDIR /app

# Install runtime dependencies for WeasyPrint + LibreOffice in the slim image
RUN apt-get update && apt-get install -y \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libpangoft2-1.0-0 \
    libgdk-pixbuf-2.0-0 \
    libffi8 \
    shared-mime-info \
    libcairo2 \
    libgobject-2.0-0 \
    libreoffice \
    libreoffice-writer \
    libreoffice-impress \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv .venv/
COPY . .

EXPOSE 8000

CMD ["/app/.venv/bin/python", "-m", "uvicorn", "api.server:app", "--host", "0.0.0.0", "--port", "8000"]
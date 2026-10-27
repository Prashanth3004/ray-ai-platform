
#  Build a lightweight runtime image for the Ray data pipeline.
FROM python:3.12-slim AS builder

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

RUN python -m venv "${VIRTUAL_ENV}"
WORKDIR /build

COPY requirements.txt .

#  Install CPU-only PyTorch to avoid downloading unnecessary CUDA libraries.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch

#  Install remaining dependencies without retaining pip download caches.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

#  Keep only the runtime environment and application files.
FROM python:3.12-slim AS runtime

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace

COPY --from=builder /opt/venv /opt/venv
COPY app/ /workspace/app/

CMD ["python", "app/ray_pipeline.py"]

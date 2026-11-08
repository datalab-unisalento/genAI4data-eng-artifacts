FROM python@sha256:6b25cbe6577518889d553cdd5a5da97d259348d8f6e45b527dd731caaba27a07

WORKDIR /app
COPY src ./src
COPY fixtures ./fixtures
ENV PYTHONPATH=/app/src

EXPOSE 8090
CMD ["python", "-m", "pipeline_bench.cli", "serve"]

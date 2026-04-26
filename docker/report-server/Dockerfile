FROM python:3.12-slim AS builder
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir ".[report-server]"

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
COPY --from=builder /usr/local/bin/seeknal /usr/local/bin/seeknal
EXPOSE 8787
VOLUME /data
ENV SEEKNAL_REPORT_SERVER_DATA_DIR=/data
CMD ["seeknal", "report-server", "start", "--host", "0.0.0.0", "--port", "8787"]

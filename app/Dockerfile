FROM python:3.12-slim
WORKDIR /work
COPY app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY app/ /work/app/
COPY schemas/ /work/schemas/
ENV PYTHONUNBUFFERED=1
CMD ["python", "app/producer.py"]

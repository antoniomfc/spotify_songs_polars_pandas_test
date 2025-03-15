FROM python:3.10-slim
WORKDIR /usr/local/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd app
USER app

CMD ["python", "app.py"]
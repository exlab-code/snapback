FROM python:3.11-slim

WORKDIR /app

COPY requirements-live.txt .
RUN pip install --no-cache-dir -r requirements-live.txt

COPY smc_trader/ ./smc_trader/
COPY orb_trader/ ./orb_trader/

RUN mkdir -p data/cache logs

CMD ["python", "-m", "smc_trader.main", "live"]

FROM python:3.12-slim-bullseye

RUN apt-get update

WORKDIR /app
COPY requirements.lock ./
RUN pip install --upgrade pip
RUN PYTHONDONTWRITEBYTECODE=1 pip install --no-cache-dir -r requirements.lock

COPY src/ve_til_isy ./ve_til_isy

CMD python -m ve_til_isy
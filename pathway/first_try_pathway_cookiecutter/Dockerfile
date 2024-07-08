FROM python:3.11

ENV PYTHONUNBUFFERED=1

ARG BUILD_ENVIRONMENT=dev

WORKDIR /first_try_pathway_cookiecutter

COPY requirements-common.txt requirements-common.txt
COPY requirements-${BUILD_ENVIRONMENT}.txt requirements-${BUILD_ENVIRONMENT}.txt
RUN pip install --no-cache-dir --upgrade -r /first_try_pathway_cookiecutter/requirements-${BUILD_ENVIRONMENT}.txt

COPY . .
CMD ["python", "run.py"]
FROM python:3.10

WORKDIR /etl_tienda
COPY . /etl_tienda/

RUN pip install -r requirements.txt

CMD [ "python","etl/etl_pipeline.py" ]
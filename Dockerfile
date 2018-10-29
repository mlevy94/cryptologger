FROM python:alpine

WORKDIR /logger

ADD *.py ./

RUN chmod +x *.py

RUN pip install requests

ENTRYPOINT ["./cryptologger.py"]

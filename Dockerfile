FROM python:alpine

WORKDIR /logger

ADD cryptologger.py /logger

RUN chmod +x cryptologger.py

RUN pip install requests

ENTRYPOINT ["./cryptologger.py"]

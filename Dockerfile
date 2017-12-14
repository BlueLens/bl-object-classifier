FROM bluelens/ubuntu-16.04:py3

RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN pip3 install -r requirements.txt

CMD ["python3", "main.py"]

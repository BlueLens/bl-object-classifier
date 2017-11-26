FROM bluelens/ubuntu-16.04:py3

RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN pip3 install -r requirements.txt

ENV AWS_ACCESS_KEY AKIAIHJDBJ2YFQS4HQZA
ENV AWS_SECRET_ACCESS_KEY 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
ENV REDIS_SERVER bl-mem-store-master-vm
ENV REDIS_PASSWORD xBmrxj4VsQSP
ENV OD_HOST magi-0.stylelens.io
ENV OD_PORT 50052


CMD ["python3", "main.py"]

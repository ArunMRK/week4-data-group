FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt .
COPY .env .

RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

ENV AWS_ACCESS_KEY_ID AKIAYZZCV7OUZIBOIOFJ
ENV AWS_SECRET_ACCESS_KEY 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
ENV AWS_DEFAULT_REGION eu-west-2

ENV AWS_HOST sigma-data-engineering-instance-1.c1i5dspnearp.eu-west-2.rds.amazonaws.com
ENV AWS_PORT 5432
ENV AWS_USERNAME alex
ENV AWS_PASSWORD sigmastudent
ENV AWS_DATABASE postgres

COPY script.py ${LAMBDA_TASK_ROOT}

CMD [ "script.lambda_handler" ]
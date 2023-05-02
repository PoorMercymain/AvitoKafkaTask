FROM python
RUN pip install kafka-python
WORKDIR /task/
COPY consumer.py /task/
CMD [ "python3", "/task/consumer.py" ]
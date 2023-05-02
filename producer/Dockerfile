FROM python
RUN pip install kafka-python
WORKDIR /task/
COPY producer.py /task/
CMD [ "python3", "/task/producer.py" ]
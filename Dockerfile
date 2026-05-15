FROM spark:latest
USER root
RUN pip3 install pymongo
USER spark

FROM python
ADD . /web
WORKDIR /web
ENV TEAM_NAME=CC_0160_1540_1868
RUN pip install -r ./requirements.txt
ENTRYPOINT ["python"]
CMD ["user.py"]

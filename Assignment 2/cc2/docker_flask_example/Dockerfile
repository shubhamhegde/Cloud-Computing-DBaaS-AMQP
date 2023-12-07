FROM python
COPY . /web
WORKDIR /web/api
EXPOSE 8080
ENV TEAM_NAME=CC_0160_1540_1868
RUN pip install -r ./requirements.txt
ENTRYPOINT ["python"]
CMD ["user.py"]

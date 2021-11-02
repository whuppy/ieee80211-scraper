FROM python:3.6.8-stretch
WORKDIR /app
ADD . /app

# You may need to install other dependencies

RUN pip install -r requirements.txt
CMD ["python", "-u", "src/update.py"]

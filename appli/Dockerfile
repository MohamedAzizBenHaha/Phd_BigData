FROM python:3.9-slim-buster

WORKDIR /appli

# Copy the requirements file into the container and install the dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files into the container
COPY . .


EXPOSE 5000

CMD ["python", "principale.py"]

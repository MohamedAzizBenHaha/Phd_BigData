FROM python:3.9-slim-buster

WORKDIR /prod

# Copy the requirements file into the container and install the dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files into the container
COPY . .

EXPOSE 2001

CMD ["python", "producteur.py"]

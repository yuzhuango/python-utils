# Use an official Python runtime as a parent image
FROM python3.6
CMD pip install -r requirements.txt

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app



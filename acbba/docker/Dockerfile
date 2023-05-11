# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY ./src /app

# Install the required Python packages
RUN pip install requests
RUN pip install flask

# Set the entry point to the Python script
ENTRYPOINT ["python", "main.py"]
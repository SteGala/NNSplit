# Use an official Python runtime as a parent image
FROM python:3.10.6 as dep

# Install the required Python packages
RUN pip install requests
RUN pip install kubernetes
RUN pip install psutil
RUN pip install numpy
RUN pip install prometheus_api_client

# use the previous stage and copy the src files
FROM dep as final

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY ./src /app

# Set the entry point to the Python script
ENTRYPOINT ["python", "main.py"]
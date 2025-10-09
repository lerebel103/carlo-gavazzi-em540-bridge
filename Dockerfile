# Use an official Python runtime as a base image
FROM python:3.14

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container at /app
COPY . .

RUN chmod +x ./main.py

# Define the command to run the application
CMD ["./main.py"]

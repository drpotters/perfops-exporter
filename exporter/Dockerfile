# syntax=docker/dockerfile:1
# Dockerfile to containerize the perfops_exporter.py Python script.

# Start from an official, lightweight Python base image.
# Using a specific version tag is recommended over 'latest' for predictable builds. 
FROM python:3.9-slim

# Set environment variables to optimize Python in a container.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create a non-root user and group for security.
# Running containers as a non-root user is a critical security best practice. 
RUN addgroup --system nonroot && adduser --system --ingroup nonroot nonroot

# Set the working directory.
WORKDIR /app

# Copy only the requirements file first to leverage Docker's layer caching. 
COPY requirements.txt .

# Install the Python dependencies using pip.
# --no-cache-dir keeps the image size small. 
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application script.
COPY perfops_exporter.py .

# Change ownership of the app directory to the non-root user.
RUN chown -R nonroot:nonroot /app

# Switch to the non-root user.
USER nonroot

# Expose port 8000, which the exporter uses to serve metrics. 
EXPOSE 8000

# Specify the default command to execute the script. 
CMD ["python", "./perfops_exporter.py"]

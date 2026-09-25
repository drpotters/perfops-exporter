# syntax=docker/dockerfile:1
# Dockerfile to containerize the perfops_exporter.py Python script.

# ---- Base Stage ----
# 1. Start from an official, lightweight Python base image.
# Using a specific version tag is recommended over 'latest' for predictable builds. 
FROM python:3.9-slim as base

# Set environment variables to optimize Python in a container.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ---- Builder Stage ----
# This stage installs dependencies into a separate image.
FROM base as builder

# Set the working directory. 
WORKDIR /app

# Copy only the requirements file first to leverage Docker's layer caching. 
COPY requirements.txt .

# Install the Python dependencies using pip.
# --no-cache-dir keeps the image size small. 
RUN pip install --no-cache-dir -r requirements.txt

# ---- Final Stage ----
# This is the final, lightweight image that will be deployed.
FROM base

# Create a non-root user and group for security.
# Running containers as a non-root user is a critical security best practice. 
RUN addgroup --system nonroot && adduser --system --ingroup nonroot nonroot

# Set the working directory.
WORKDIR /app

# Copy the installed dependencies from the builder stage and the application script.
COPY --from=builder /app /app
COPY perfops_exporter.py .

# Change ownership of the app directory to the non-root user.
RUN chown -R nonroot:nonroot /app

# Switch to the non-root user.
USER nonroot

# Expose port 8000, which the exporter uses to serve metrics. 
EXPOSE 8000

# Specify the default command to execute the script. 
CMD ["python", "./perfops_exporter.py"]


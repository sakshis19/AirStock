# Use a Python base image
FROM python:3.9-slim-buster

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Streamlit application file (streamlit_app.py)
# Make sure your actual Streamlit app is named 'streamlit_app.py' in your local 'streamlit_app' folder
COPY streamlit_app.py .

# If you have stock_model.py, copy it too
# This line is removed as per your request to exclude ML from the project.
# COPY stock_model.py .

# Expose the port Streamlit runs on
EXPOSE 8501

# Command to run the Streamlit application
# This is the most reliable way to ensure 'python' is found and executed correctly.
CMD python -m streamlit run streamlit_app.py

# Alternative (if the above still fails, less likely):
# ENTRYPOINT ["python", "-m", "streamlit", "run", "streamlit_app.py"]

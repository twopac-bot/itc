#!/bin/bash

echo "GST Reconciliation Tool Setup"
echo "============================"

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Detected Python version: $python_version"

# Check if Python 3.13 is being used
if [[ $python_version == 3.13* ]]; then
    echo "⚠️  Warning: Python 3.13 detected. Using compatibility requirements..."
    requirements_file="requirements-py313.txt"
else
    echo "✅ Using standard requirements..."
    requirements_file="requirements.txt"
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "Installing requirements from $requirements_file..."
pip install -r $requirements_file

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p uploads reports static test_data

# Copy .env.example to .env if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cp .env.example .env
    echo "⚠️  Please edit .env file to set your API key!"
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "To run the application:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Run the application: python main.py"
echo "3. Open http://localhost:8000 in your browser"
echo ""
echo "API documentation available at: http://localhost:8000/docs"
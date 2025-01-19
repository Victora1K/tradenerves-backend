# Clone the repository and navigate to the project folder
git clone <your-repository-url>
cd <your-repository-folder>

# 1. Backend Setup (Flask)

# 1.1 Create a virtual environment
# Install virtualenv if you don't have it installed
pip install virtualenv

# Create the virtual environment
virtualenv venv

# Activate the virtual environment
# On Windows:
.\venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# 1.2 Install Backend Dependencies
pip install -r requirements.txt

# 1.3 Initialize the SQLite Database
# Run the script to fetch and store stock data in the database
python data/fetch_data.py

# 1.4 Run the Flask Backend Server
python app.py

# Backend will now be running on http://localhost:5000

# 2. Frontend Setup (React)

# 2.1 Navigate to the frontend folder and install frontend dependencies
cd frontend  # Replace 'frontend' with your actual frontend folder name
yarn install  # or npm install if you're using npm

# 2.2 Start the React Development Server
yarn start  # or npm start if you're using npm

# Frontend will now be running on http://localhost:3000

# Import the Flask class from the flask module
from flask import Flask

# Create an instance of the Flask class to represent the application
app = Flask(__name__)

# Define a route for the root URL ("/")
@app.route('/')
def hello_world():
    # This function returns a simple string when the root URL is accessed
    return "Hello, World from Flask!"

# Check if the script is being run directly (not imported as a module)
if __name__ == "__main__":
    # Run the app on all available network interfaces (0.0.0.0) at port 5000
    app.run(host="0.0.0.0", port=5000)
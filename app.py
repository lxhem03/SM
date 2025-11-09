# web.py
from flask import Flask
import os

# Initialize the Flask app
app = Flask(__name__)

@app.route('/')
def hello_world():
    """This function runs when someone visits the root URL."""
    return '<h1>Bot is alive!</h1><p>This web page confirms that the deployment is running.</p>'

if __name__ == "__main__":
    # Get the port from the environment variable, default to 8080
    port = int(os.environ.get('PORT', 8080))
    # Run the app. Host '0.0.0.0' is important to make it accessible outside the container.
    app.run(host='0.0.0.0', port=port)

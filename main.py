from app import create_app
import os
from config import log_directory

app = create_app()

if __name__ == '__main__':
    # Create the log directory if it doesn't exist
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    app.run(host='0.0.0.0', port=5000)
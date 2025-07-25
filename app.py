from flask import Flask, render_template, jsonify
import json
import threading
import time
from consumer import dashboard_data, start_consumer_in_thread # Import consumer logic

app = Flask(__name__)

# Start the Kafka consumer in a separate thread when the app starts
consumer_thread = start_consumer_in_thread()

@app.route('/')
def index():
    # A simple HTML template to display the data
    return render_template('dashboard.html')

@app.route('/data')
def get_dashboard_data():
    # Return the current in-memory dashboard data as JSON
    # Sort for top N for display purposes if needed
    sorted_nations = sorted(dashboard_data["users_by_nation"].items(), key=lambda item: item[1], reverse=True)[:5]
    sorted_cities = sorted(dashboard_data["users_by_city"].items(), key=lambda item: item[1], reverse=True)[:5]

    return jsonify({
        "total_users": dashboard_data["total_users"],
        "gender_distribution": dashboard_data["gender_distribution"],
        "users_by_nation": dict(sorted_nations),
        "users_by_city": dict(sorted_cities),
        "recent_users": dashboard_data["recent_users"]
    })

if __name__ == '__main__':
    # You might want to run this in a production-ready WSGI server like Gunicorn
    app.run(debug=True, port=5000)
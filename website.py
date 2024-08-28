import configparser
import os

from flask import Flask, render_template, request, flash, session, redirect, url_for
from backend import *
from datetime import timedelta

app = Flask(__name__)
app.secret_key = "key"
app.permanent_session_lifetime = timedelta(minutes=5)
config = configparser.ConfigParser()
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, 'config.ini')
config.read(config_path)
mongo_db_username = config["mongo_db"]["username"]
mongo_db_pass = config["mongo_db"]["password"]


def get_pumped_coin_data():
    mongo_app = App(mongo_db_username, mongo_db_pass)
    pumped_coin_data = mongo_app.get_pumped_coin_data()
    return pumped_coin_data


@app.route("/home", methods=["POST", "GET"])
@app.route("/", methods=["POST", "GET"])
def home():
    return render_template("home.html")


@app.route("/graphs", methods=["POST", "GET"])
def about():
    if request.method == 'POST':
        return redirect(url_for("home"))
    return render_template("graphs.html", coin_data=get_pumped_coin_data())


if __name__ == "__main__":
    app.run(debug=True)
    # app.run(host='0.0.0.0', port=5000, debug=True)

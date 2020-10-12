from flask import Flask, request, render_template
import json

app = Flask(__name__)

@app.route('/')
def index():
	title = "Arman Asryan's Project"
	return render_template('index.html', title = title)

@app.route('/about')
def about():
	names = ["John","Mary","Wes","Sally"]
	return render_template('about.html', names = names)
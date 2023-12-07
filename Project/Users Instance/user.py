from flask import Flask, render_template,\
jsonify,request,abort,redirect
from flask_sqlalchemy import SQLAlchemy
import random
import csv
from datetime import datetime
import requests
import json
import time

import re

app=Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False


@app.route('/api/v1/users',methods=['PUT','GET'])
def add_user():
	if(request.method=='PUT'):
		x=request.get_json()
		pattern=re.compile("^[a-fA-F0-9]{40}$")
		if(not("username" in x.keys() and "password" in x.keys())):
			abort(400,"Wrong request parameters recieved")
		user=request.get_json()["username"]
		password=request.get_json()["password"]
		if(not(pattern.match(password))):
			abort(400,"Password not valid SHA1 hash rex")

		url='http://34.236.25.7/api/v1/db/read'
		obj="{\"table\":\"User\",\"columns\":[\"username\",\"password\"],\"where\":\"username="+user+"\"}"
		x=dict()
		x["message"]=obj
		response = eval(requests.post(url, json=x).json())
		print(response)
		check=response["info"]
		if(check):
			return "Request succesfully recieved but username already exists",200
		else:
			url='http://34.236.25.7/api/v1/db/write'
			obj = "{\"insert_data\":[{\"col\":\"username\",\"data\":\""+user+"\"},{\"col\":\"password\",\"data\":\""+password+"\"}],\"table\":\"User\"}"
			x=dict()
			x["message"]=obj
			response = requests.post(url, json=x).json()
			print(response)
			return {},201 #succesful PUT
	else:
		url='http://34.236.25.7/api/v1/db/read'
		obj="{\"table\": \"User\",\"columns\": [\"username\"],\"where\":\"\"}"
		x=dict()
		x["message"]=obj
		response = requests.post(url, json=x)
		print(response)
		if(not(response)):
			return {},204
		else:
			print(type(response.json()))
			print(response)
			response=response.json()
			print(response)
			response=eval(response)
			print(response)
			l=[]
			for x in response["info"]:
				l.append(x["username"])
			return json.dumps(l),200


@app.route('/api/v1/users/<username>',methods=["DELETE"])
def delete_user(username):
	url='http://34.236.25.7/api/v1/db/read'
	obj="{\"table\":\"User\",\"columns\":[\"username\",\"password\"],\"where\":\"username="+username+"\"}"
	x=dict()
	x["message"]=obj
	response = eval(requests.post(url, json=x).json())
	print(response)
	check=response["info"]
	if(check):
		url='http://34.236.25.7/api/v1/db/write'
		obj = "{\"delete_data\":[{\"col\":\"username\",\"data\":\""+username+"\"}],\"table\":\"User\"}"
		x=dict()
		x["message"]=obj
		response = requests.post(url, json=x)
		obj = "{\"delete_data\":[{\"col\":\"Created_by\",\"data\":\""+username+"\"}],\"table\":\"Rides\"}"
		x=dict()
		x["message"]=obj
		response = requests.post(url, json=x)
		obj = "{\"delete_data\":[{\"col\":\"User\",\"data\":\""+username+"\"}],\"table\":\"Share\"}"
		x=dict()
		x["message"]=obj
		response = requests.post(url, json=x)
	else:
		abort(400,"Username not registered")
	return {},200

if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0")

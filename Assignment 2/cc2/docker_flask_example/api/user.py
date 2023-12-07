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
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite3'
db = SQLAlchemy(app)

class User(db.Model):
    username = db.Column(db.String(80), unique=True,primary_key=True)
    password = db.Column(db.String(40), nullable=False)

db.create_all()
client = app.test_client()


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

		url='/api/v1/db/read'
		obj={
			"table": "User",
			"columns": ["username","password"],
			"where": "username="+user
			}
		response = client.post(url, json=obj).get_json()
		

		check=response["info"]
		if(check):
			return "Request succesfully recieved but username already exists",200
		else:
			url='/api/v1/db/write'
			obj = {
					"insert_data": [
					{
						"col": "username",
						"data": user
					},
					{
						"col": "password",
						"data": password
					}
				],
			"table": "User"
			}
			response = client.post(url, json=obj).get_json()
			return {},201 #succesful PUT
	else:
		url='/api/v1/db/read'
		obj={
			"table": "User",
			"columns": ["username"],
			"where":""
			}
		response = client.post(url, json=obj).get_json()
		if(not(response)):
			return {},204
		else:
			l=[]
			for x in response["info"]:
				l.append(x["username"])
			return json.dumps(l),200


@app.route('/api/v1/users/<username>',methods=["DELETE"])
def delete_user(username):
	url='/api/v1/db/read'
	obj={
		"table": "User",
		"columns": ["username","password"],
		"where": "username="+username
		}
	response = client.post(url, json=obj).get_json()
	check=response["info"]
	if(check):
		url='/api/v1/db/write'
		obj = {
				"delete_data": [
				{
					"col": "username",
					"data": username
				}
			],
		"table": "User"
		}
		response = client.post(url, json=obj).get_json()
		url='http://35.174.217.206:8000/api/v1/db/write'
		obj = {
				"delete_data": [
				{
					"col": "Created_by",
					"data": username
				}
			],
		"table": "Rides"
		}
		response = requests.post(url, json=obj)
		url='http://35.174.217.206:8000/api/v1/db/write'
		obj = {
				"delete_data": [
				{
					"col": "User",
					"data": username
				}
			],
		"table": "Share"
		}
		response = requests.post(url, json=obj)
		#db.session.delete(User.query.filter_by(username=username).first())
		#db.session.commit()
	else:
		abort(400,"Username not registered")
	return {},200

@app.route('/api/v1/db/write',methods=['POST'])
def write_db():
	x=request.get_json()
	if("insert_data" in x):
		insert_data=x["insert_data"]
		tab=x["table"]
		newentry=eval(tab)()
		#return str(newentry)
		for i in insert_data:
			hi=i["col"]
			setattr(newentry,hi,i["data"])
		db.session.add(newentry)
	elif("delete_data" in x):
		if(not(x["delete_data"])):
			tab=eval(x["table"])
			x=tab.query.delete()
		else:
			delete_data=x["delete_data"]
			tab=eval(x["table"])
			d1=getattr(tab,delete_data[0]["col"])
			x=tab.query.filter(d1==delete_data[0]["data"]).delete()
		#db.session.delete(x)
	db.session.commit()
	return '',200

@app.route('/api/v1/db/read',methods=['POST'])
def read_db():
	x=request.get_json()
	flag=0
	cols=x["columns"]
	tab=eval(x["table"])
	if(not(x["where"])):
		q=tab.query.all()
	else:
		if('&' in x["where"]):
			d_form="%d-%m-%Y:%S-%M-%H"
			curr_time=time.strftime(d_form)
			
			#return {"info":[curr_time]}
			sp=x["where"].split('&')
			lhs1=sp[0].split("=")[0]
			d1=getattr(tab,lhs1)
			rhs1=sp[0].split("=")[1]
			lhs2=sp[1].split("=")[0]
			d2=getattr(tab,lhs2)
			rhs2=sp[1].split("=")[1]
			if(x["table"]=="Share"):
				q=tab.query.filter((d1==rhs1)&(d2==rhs2)).all()
			else:
				flag=1
				d3=getattr(tab,"Timestamp")
				d4=getattr(tab,"RideID")
				#return {"info":"hi"}
				#q=tab.query.filter((d1==rhs1)&(d2==rhs2)&((datetime.strptime(getattr(tab,"Timestamp"),d_form)-datetime.strptime(curr_time,d_form)).total_seconds()>0)).all()
				q=tab.query.filter((d1==rhs1)&(d2==rhs2)).all()
				#return {"info":[hi]}

		else:
			lhs=x["where"].split("=")[0]
			d=getattr(tab,lhs)
			rhs=x["where"].split("=")[1]
			q=tab.query.filter(d==rhs).all()
	response={"info":[]}
	for j in q:
		#return {"info":str(j)}
		temp={}
		for i in cols:
			col=i
			if(i=="Timestamp" and flag==1):
				if((datetime.strptime(getattr(j,i),d_form)-datetime.strptime(curr_time,d_form)).total_seconds()>0):
					temp[i]=getattr(j,i)
					continue
				else:
					temp={}
					break
				#return {"info":str(type(getattr(j,i)))}
			temp[i]=getattr(j,i)
		if(temp):
			response["info"].append(temp)
	return response
@app.route('/api/v1/db/clear',methods=['POST'])
def clear_db():
	url='/api/v1/db/write'
	obj = {
			"delete_data": [],
			"table": "User"
	}
	response = client.post(url, json=obj).get_json()
	return {},200
if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0")

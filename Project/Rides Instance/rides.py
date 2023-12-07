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

with open('AreaNameEnum.csv', mode='r') as infile:
    reader = csv.reader(infile)
    mydict = {rows[0]:rows[1] for rows in reader}

app=Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False

session={}
rides_count=0
HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']

@app.route('/api/v1/rides',methods=HTTP_METHODS)
def add_ride():
	if(request.method=='POST'):
			if not "counter" in session:
				session["counter"]=0
			session["counter"]+=1
			x=request.get_json()
			pattern=re.compile("((0[1-9]|[12][0-9]|3[01])-(0[13578]|1[02])-(18|19|20)[0-9]{2})|(0[1-9]|[12][0-9]|30)-(0[469]|11)-(18|19|20)[0-9]{2}|(0[1-9]|1[0-9]|2[0-8])-(02)-(18|19|20)[0-9]{2}|29-(02)-(((18|19|20)(04|08|[2468][048]|[13579][26]))|2000) [0-5][0-9]:[0-5][0-9]:(2[0-3]|[01][0-9])")
			if(not("created_by" in x.keys() and "timestamp" in x.keys() and "source" in x.keys() and "destination" in x.keys())):
				abort(400,"Wrong request parameters recieved")
			username=x["created_by"]

			url='http://54.90.29.43/api/v1/users'
			response = eval(requests.get(url,headers={'Origin': '54.90.3.12'}).text)
			check=username in response
			if(check):
				time_=x["timestamp"]
				try:
					time.strptime(time_, '%d-%m-%Y:%S-%M-%H')
				except ValueError:
					abort(400, "Timestamp is of wrong format")

				#check valid date
				source=x["source"]
				dest=x["destination"]

				if(not(source in mydict.keys() and dest in mydict.keys()) or source==dest):
					abort(400,"Invalid source and destination codes")


				ride_id=str(random.randint(1000,2000))
				url='http://34.236.25.7/api/v1/db/read'
				obj="{\"table\":\"Rides\",\"columns\":[\"RideID\"],\"where\":\"\"}"
				x=dict()
				x["message"]=obj
				response = eval(requests.post(url, json=x).json())
				check=response["info"]
		
				ride_id=str(random.randint(1000,2000))
				while(ride_id in list(map(lambda x:x["RideID"],check))):
					ride_id=str(random.randint(1000,2000))
		

				url='http://34.236.25.7/api/v1/db/write'
				obj = "{\"insert_data\":[{\"col\":\"RideID\",\"data\":\""+ride_id+"\"},{\
					\"col\":\"Created_by\",\
					\"data\":\""+username+"\"\
				},\
				{\
					\"col\":\"Timestamp\",\
					\"data\":\""+time_+"\"\
				},\
				{\
					\"col\":\"Source\",\
					\"data\":\""+source+"\"\
				},\
				{\
					\"col\":\"Destination\",\
					\"data\":\""+dest+"\"\
				}\
			],\
				\"table\":\"Rides\"\
				}"

				x=dict()
				x["message"]=obj
				response = requests.post(url, json=x)
				
				url='http://34.236.25.7/api/v1/db/write'
				obj = "{\"insert_data\":[\
				{\
					\"col\":\"RideID\",\
					\"data\":\""+ride_id+"\"\
				},\
				{\
					\"col\":\"User\",\
					\"data\":\""+username+"\"\
				}\
			],\
				\"table\":\"Share\"\
				}"

				x=dict()
				x["message"]=obj
				response = requests.post(url, json=x)
				return {},201#successful POST
			else:
				abort(400,"User doesn't exist")
	elif(request.method=="GET"):
		if not "counter" in session:
			session["counter"]=0
		session["counter"]+=1
		source = request.args.get('source')
		destination = request.args.get('destination')
		if(source=="" or destination==""):
			abort(400,"Give the required source and destination parameters")
		if(not(source in mydict.keys() and destination in mydict.keys())):
			abort(400,"Invalid source or destination codes")
		response=[]
		f=0
		url='http://34.236.25.7/api/v1/db/read'
		obj="{\"table\":\"Rides\",\"columns\":[\"RideID\",\"Created_by\",\"Timestamp\"],\"where\":\"Source="+source+"&Destination="+destination+"\"}"
		x=dict()
		x["message"]=obj
		response = eval(requests.post(url, json=x).json())
		try:
			check1=response["info"]
		except:
			return "hello"
		response=[]
		for x in check1:
				f=1
				details={}
				details["rideId"]=int(x["RideID"])
				details["username"]=x["Created_by"]
				details["timestamp"]=x["Timestamp"]
				response.append(details)

		if(f==0):
			return 'There are no upcoming rides from the given source to destination',204 #no content
		else:
			return json.dumps(response),200
	else:
		if not "counter" in session:
			session["counter"]=0
		session["counter"]+=1
		abort(405,"Wrong method")


@app.route('/api/v1/rides/<rideId>',methods=HTTP_METHODS)
def get_rides(rideId):
	if(request.method=='GET'):
		if not "counter" in session:
			session["counter"]=0
		session["counter"]+=1
		url='http://34.236.25.7/api/v1/db/read'
		obj="{\"table\":\"Rides\",\"columns\":[\"RideID\",\"Created_by\",\"Timestamp\",\"Source\",\"Destination\"],\"where\":\"RideID="+rideId+"\"}"
		x=dict()
		x["message"]=obj
		response = eval(requests.post(url, json=x).json())
		check=response["info"][0]
		if(check):
			url='http://34.236.25.7/api/v1/db/read'
			obj="{\"table\":\"Share\",\"columns\":[\"User\"],\"where\":\"RideID="+rideId+"\"}"
			x=dict()
			x["message"]=obj
			response = eval(requests.post(url, json=x).json())
			check1=response["info"]
			y=list(map(lambda x:x["User"],check1))
			y=list(filter(lambda x:x!=check["Created_by"],y))
			details={}
			details["rideId"]=int(check["RideID"])
			details["created_by"]=check["Created_by"]
			details["timestamp"]=check["Timestamp"]
			details["users"]=y
			details["source"]=check["Source"]
			details["destination"]=check["Destination"]
		else:
			abort(400,"Ride Id is not valid")
			details={}
		return jsonify(details),200
	
	elif(request.method=='POST'):
		if not "counter" in session:
			session["counter"]=0
		session["counter"]+=1
		x=request.get_json()
		if(not("username" in x.keys())):
			abort(400,"Wrong request parameters recieved")
		user=x["username"]
		url='http://34.236.25.7/api/v1/db/read'
		obj="{\"table\":\"Rides\",\"columns\":[\"RideID\"],\"where\":\"RideID="+rideId+"\"}"
		x=dict()
		x["message"]=obj
		response = eval(requests.post(url, json=x).json())
		check=response["info"]

		url='http://54.90.29.43/api/v1/users'
		response = eval(requests.get(url,headers={'Origin': '54.90.3.12'}).text)
		check1=user in response

		url='http://34.236.25.7/api/v1/db/read'
		obj="{\"table\":\"Share\",\"columns\":[\"RideID\",\"User\"],\"where\":\"User="+user+"&RideID="+rideId+"\"}"
		x=dict()
		x["message"]=obj
		response = eval(requests.post(url, json=x).json())
		check2=response["info"]
		if(check2):
			abort(400,"User has already joined the ride")
		if(check and check1):
			url='http://34.236.25.7/api/v1/db/write'
			obj = "{\"insert_data\":[{\"col\":\"RideID\",\"data\":\""+rideId+"\"},{\"col\":\"User\",\"data\":\""+user+"\"}],\"table\":\"Share\"}"
			x=dict()
			x["message"]=obj
			response = requests.post(url, json=x)
			return {},200
		else:
			abort(400,"Incorrect rideid or username")

	elif(request.method=='DELETE'):
		if not "counter" in session:
			session["counter"]=0
		session["counter"]+=1
		url='http://34.236.25.7/api/v1/db/read'
		obj="{\"table\":\"Rides\",\"columns\":[\"RideID\"],\"where\":\"RideID="+rideId+"\"}"
		x=dict()
		x["message"]=obj
		response = eval(requests.post(url, json=x).json())
		check=response["info"]
		if(not(check)):
			abort(400,"RideId is incorrect")
		else:
			url='http://34.236.25.7/api/v1/db/write'
			obj = "{\"delete_data\":[{\"col\":\"RideID\",\"data\":\""+rideId+"\"}],\"table\":\"Rides\"}"
			x=dict()
			x["message"]=obj
			response = requests.post(url, json=x)

			url='http://34.236.25.7/api/v1/db/write'
			obj = "{\"delete_data\":[{\"col\":\"RideID\",\"data\":\""+rideId+"\"}],\"table\":\"Share\"}"
			x=dict()
			x["message"]=obj
			response = requests.post(url, json=x)
		return {},200
	else:
		if not "counter" in session:
			session["counter"]=0
		session["counter"]+=1
		abort(405,"Wrong method")
		  

@app.route('/api/v1/_count',methods=['GET','DELETE'])
def req_count():
	if(request.method=='DELETE'):
		session["counter"]=0
		return {},200
	elif(request.method=='GET'):
		if not "counter" in session:
			session["counter"]=0
		l=[]
		l.append(session["counter"])
		return json.dumps(l),200

@app.route('/api/v1/rides/count',methods=HTTP_METHODS)
def ride_count():
	
	if(request.method=='GET'):
			if not "counter" in session:
				session["counter"]=0
			session["counter"]+=1
			url='http://34.236.25.7/api/v1/db/read'
			obj="{\
			\"table\":\"Rides\",\
			\"columns\":[\"RideID\"],\
			\"where\":\"\"\
				}"
			response = requests.post(url, json=obj)
			check=response["info"]
			ride_id=list(map(lambda x:x["RideID"],check))
			l=[]
			l.append(len(ride_id))
			return json.dumps(l),200
	else:
		if not "counter" in session:
				session["counter"]=0
		session["counter"]+=1
		abort(405,"Wrong method")
if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0")

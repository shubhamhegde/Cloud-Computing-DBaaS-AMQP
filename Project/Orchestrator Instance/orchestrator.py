from flask import Flask, render_template,session, jsonify, request, abort, redirect
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from flask_session import Session
import random
import csv
import requests
import json
import time
import threading
import pika
import logging
import uuid
import docker
import math
import os
from kazoo.client import KazooClient
from kazoo.client import KazooState
from functools import partial


logging.basicConfig()


counter=0
flag=0

app=Flask(__name__)
db=SQLAlchemy(app)


def container_logs(x):
	y=x.attach(stderr=True,stdout=True,logs=True,stream=True)
	for i in y:
		print(i)

def election(worker_type,event):
	print(event,event.path,event.type)
	client=docker.from_env()
	c=docker.APIClient()
	if(worker_type=="master"):
		print("##################### YOU ARE MASTER ######################")
		print("##################### ",event.type," ######################")
		if(event.type=="DELETED"):
			children=zk.get_children("/workerss")
			print(children,len(children))
			min_cid=float('inf')
			min_pid=float('inf')
			for i in children:
				data, stat=zk.get("/workerss/"+i)
				data=data.decode('utf-8')
				data=json.loads(data)
				if(data['type']=="slave" and data["pid"]<min_pid):
					min_cid=data["cid"]
					min_pid=data["pid"]
			print("New master:",min_pid,min_cid)
			connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
			channel = connection.channel()
			channel.exchange_declare(exchange="master_or_not", exchange_type="fanout")
			channel.basic_publish(exchange="master_or_not",routing_key='',body=min_cid)
			connection.close()
			zk.set("/workerss/node_"+str(min_cid),b"{\"type\":\"master\",\"pid\":"+str(min_pid).encode('utf-8')+b",\"cid\":\""+min_cid.encode('utf-8')+b"\"}")
			
	else:
		if((event.path not in zk.get_children("/workerss") and event.type=="DELETED") or json.loads(zk.get(event.path)[0].decode('utf-8'))["type"]=="master"):
			print("##################### YOU ARE SLAVE ######################")
			print("##################### ",event.type," ######################")
			m_cid=""
			for i in zk.get_children("/workerss/"):
				data,stat=zk.get("/workerss/"+i)
				data=json.loads(data.decode('utf-8'))
				m_cid=data["cid"]
				break
			print("M_cid:",m_cid)
			image=client.images.build(path=".",tag="new_container_image")
			x=client.containers.run("new_container_image:latest",["python","new_worker.py"],volumes={'/home/ubuntu/cc_final/':{'bind':'/code','mode':'rw'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock','mode':'ro'}}, network="ccfinal_default",detach=True,environment={"PYTHONUNBUFFERED":"True","PYTHONDONTWRITEBYTECODE":"True"},stdout=True,stderr=True)
			print("ID of newly created container = ",x.id[:12])
			time.sleep(1)
			data,stat=zk.get("/workerss/node_"+x.id[:12],watch=partial(election, "slave"))
			print("TO BE COPIED FROM = ",m_cid)
			db.engine.execute('ATTACH DATABASE \'db_'+m_cid+'.sqlite3\' AS Master')
			db.engine.execute('ATTACH DATABASE \'db_'+x.id[:12]+'.sqlite3\' AS Slave')
			print("Master data:")
			y=db.engine.execute('select * from Master.user')
			for i in y:
				print(i)
			db.engine.execute('Insert into Slave.user select * from Master.user');
			db.engine.execute('Insert into Slave.rides select * from Master.rides');
			db.engine.execute('Insert into Slave.share select * from Master.share');
			print("slave data:")
			y=db.engine.execute('select * from Slave.user')
			for i in y:
				print(i)
			db.session.commit()
			db.engine.execute('DETACH DATABASE Master')
			db.engine.execute('DETACH DATABASE Slave')
			db.session.commit()
			print("ID of newly created container = ",x.id[:12])
			new_master_cid=""
			for i in zk.get_children("/workerss/"):
				data,stat=zk.get("/workerss/"+i)
				data=json.loads(data.decode('utf-8'))
				if(data["type"]=="master"):
					new_master_cid=data["cid"]
					break
			if(event.type=="CHANGED" and json.loads(zk.get(event.path)[0].decode('utf-8'))["type"]=="master"):
				print("*************setting new watch for new master*******************:",m_cid)
				data,stat=zk.get("/workerss/node_"+new_master_cid,watch=partial(election, "master"))
			t1 = threading.Thread(target=container_logs, args=(x,))
			t1.start()


def resp_fn(ch, method, props, body):
	global response
	print("global corr_id = ",corr_id,"global props=",props.correlation_id)
	if(corr_id == props.correlation_id):
		response = body
		print(response)
		ch.basic_ack(delivery_tag=method.delivery_tag)

def scale():
	print("in scale")
	global counter
	client = docker.from_env()
	c=docker.APIClient()
	children=zk.get_children("/workerss")
	print("children in scale:",children)
	s=0
	for i in children:
		data,stat=zk.get('/workerss/'+i)
		data=json.loads(data.decode('utf-8'))
		if(data["type"]=="slave"):
			s+=1
	print("Number of slaves existing:",s)
	x=math.ceil((counter+0.00000001)/20)
	print("requests:", counter)
	print("Number of slaves that should be there:",x)
	scaling=1
	d=0
	if(s>x):
		scaling=0
		d=s-x
	else:
		scaling=1
		d=x-s
	print("Difference(d):",d)
	print("Scaling:",scaling)
	for i in range(d):
		if(scaling==0):
			children=zk.get_children("/workerss")
			for i in children:
				data,stat=zk.get('/workerss/'+i)
				data=json.loads(data.decode('utf-8'))
				if(data["type"]=="slave"):
					zk.set("/workerss/node_"+data["cid"],b"{\"type\":\"to be killed\"}")
					container=client.containers.get(data["cid"])
					print("Killed ",data["cid"])
					container.kill()
					os.system("rm db_"+data["cid"]+".sqlite3")
					break
		elif(scaling==1):
			children=zk.get_children("/workerss")
			for i in children:
				data,stat=zk.get('/workerss/'+i)
				data=json.loads(data.decode('utf-8'))
				if(data["type"]=="master"):
					m_cid=data["cid"]
					break
			image=client.images.build(path=".",tag="new_container_image")
			x=client.containers.run("new_container_image:latest",["python","new_worker.py"],volumes={'/home/ubuntu/cc_final/':{'bind':'/code','mode':'rw'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock','mode':'ro'}}, network="ccfinal_default",detach=True,environment={"PYTHONUNBUFFERED":"True","PYTHONDONTWRITEBYTECODE":"True"},stdout=True,stderr=True)
			print("ID of newly created container = ",x.id[:12])
			time.sleep(1)
			data,stat=zk.get("/workerss/node_"+x.id[:12],watch=partial(election,"slave"))
			db.engine.execute('ATTACH DATABASE \'db_'+m_cid+'.sqlite3\' AS Master')
			db.engine.execute('ATTACH DATABASE \'db_'+x.id[:12]+'.sqlite3\' AS Slave')
			print("Master data:")
			y=db.engine.execute('select * from Master.user')
			for i in y:
				print(i)
			db.engine.execute('Insert into Slave.user select * from Master.user');
			db.engine.execute('Insert into Slave.rides select * from Master.rides');
			db.engine.execute('Insert into Slave.share select * from Master.share');
			print("slave data:")
			y=db.engine.execute('select * from Slave.user')
			for i in y:
				print(i)
			db.session.commit()
			db.engine.execute('DETACH DATABASE Master')
			db.engine.execute('DETACH DATABASE Slave')
			db.session.commit()
			y=x.attach(stderr=True,stdout=True,logs=True,stream=True)#.decode('utf-8'))
			for i in y:
				print(i)
			print("ID of newly created container = ",x.id[:12])
	counter=0
	t=threading.Timer(120.0,scale)
	t.start()

zk = KazooClient(hosts='zoo:2181',timeout=1)
zk.start(timeout=1)
for i in zk.get_children("/workerss"):
	data,stat=zk.get("/workerss/"+i)
	if(json.loads(data.decode('utf-8'))["type"]=="master"):
		x,y=zk.get("/workerss/"+i,watch=partial(election, "master"))
	else:
		data,stat=zk.get("/workerss/"+i,watch=partial(election, "slave"))




corr_id=""
response=None

@app.route('/api/v1/crash/master',methods=['POST'])
def del_master():
	client = docker.from_env()
	children=zk.get_children("/workerss")
	print(children,len(children))
	master_pid=0
	master_cid=0
	for i in children:
		data, stat=zk.get("/workerss/"+i)
		data=data.decode('utf-8')
		data=json.loads(data)
		if(data['type']=="master"):
			master_cid=data["cid"]
			master_pid=data["pid"]
			break
	print(master_cid,master_pid)
	container=client.containers.get(master_cid)
	zk.delete('/workerss/node_'+master_cid)
	container.kill()
	os.system("rm db_"+master_cid+".sqlite3")
	print(zk.get_children("/workerss"))
	return json.dumps([master_pid]),200

@app.route('/api/v1/crash/slave',methods=['POST'])
def del_slave():
	client = docker.from_env()
	children=zk.get_children("/workerss")
	print(children,len(children))
	max_pid=0
	max_cid=0
	for i in children:
		data, stat=zk.get("/workerss/"+i)
		print(type(data.decode('utf-8')),data.decode('utf-8'),data)
		data=data.decode('utf-8')
		data=json.loads(data)
		print(type(data))
		if(data['type']=="slave" and data["pid"]>max_pid):
			max_cid=data["cid"]
			max_pid=data["pid"]
	print(max_cid,max_pid)
	container=client.containers.get(max_cid)
	zk.delete('/workerss/node_'+max_cid)
	container.kill()
	os.system("rm db_"+max_cid+".sqlite3")
	print(zk.get_children("/workerss"))
	return json.dumps([max_pid]),200

@app.route('/api/v1/worker/list',methods=['GET'])
def worker_list():
	client = docker.from_env()
	children=zk.get_children("/workerss")
	worker_list=[]
	for i in children:
		data, stat=zk.get("/workerss/"+i)
		data=data.decode('utf-8')
		data=json.loads(data)
		worker_list.append(data["pid"])
	worker_list.sort()
	print(worker_list)
	return json.dumps(worker_list),200


@app.route('/api/v1/db/read',methods=['POST'])
def readdb():
	global flag
	global counter
	if(flag==0):
		flag=1
		t = threading.Timer(120.0, scale)
		t.start()
	counter+=1
	global response
	global corr_id
	response=None
	corr_id = str(uuid.uuid4())
	print("local corr_id = ",corr_id)
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
	channel = connection.channel()
	channel.queue_declare(queue='responseQ',durable=True)
	channel.basic_consume(queue='responseQ', on_message_callback= resp_fn)#,auto_ack=True)
	print("response=",response)
	channel.queue_declare(queue='readQ',durable=True)
	x=request.get_json()
	print(x)
	x=x["message"]
	print(x)
	channel.basic_publish(exchange='', routing_key='readQ', body=x, properties=pika.BasicProperties(reply_to='responseQ', correlation_id=corr_id, delivery_mode=2))
	while response is None:
		connection.process_data_events()
	print("Read request successfully written onto the queue, will be serviced shortly.")
	connection.close()
	return jsonify(response.decode('utf-8'))

@app.route('/api/v1/db/write',methods=['POST'])
def writedb():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
	channel = connection.channel()
	channel.queue_declare(queue='writeQ',durable=True)
	x=request.get_json()
	print(x)
	x=x["message"]
	channel.basic_publish(exchange='', routing_key='writeQ', body=x,properties=pika.BasicProperties(delivery_mode=2))
	print("Write request successfully written onto the queue, will be serviced shortly.")
	connection.close()
	return x,200

@app.route('/api/v1/db/clear',methods=['POST'])
def clear_db():
	url='http://34.236.25.7/api/v1/db/write'
	obj = "{\"delete_data\":[],\"table\":\"User\"}"
	x=dict()
	x["message"]=obj
	response = requests.post(url, json=x)
	obj = "{\"delete_data\":[],\"table\":\"Rides\"}"
	x=dict()
	x["message"]=obj
	response = requests.post(url, json=x)
	obj = "{\"delete_data\":[],\"table\":\"Share\"}"
	x=dict()
	x["message"]=obj
	response = requests.post(url, json=x)
	return {},200

if __name__ == '__main__':
	app.debug=True
	app.run(host="0.0.0.0",debug=True, use_reloader=False)

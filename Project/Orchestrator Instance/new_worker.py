from flask import Flask, render_template,session, jsonify, request, abort, redirect
from flask_sqlalchemy import SQLAlchemy
from flask_session import Session
import random
import csv
import requests
import json
import time
import pika
import logging
import os
import subprocess
import threading
from functools import partial
import logging
import sys
import subprocess
import sqlite3
from kazoo.client import KazooClient
from kazoo.client import KazooState
from datetime import datetime

logging.basicConfig()

import docker
client = docker.from_env()
c=docker.APIClient()
print(client.containers.list())
container_id=os.popen('cat /etc/hostname').read()
container_id=container_id[:-1]

s="docker inspect --format \'{{.State.Pid}}\' "
pid=c.inspect_container(container_id)['State']['Pid']
print(container_id,"\t",type(pid),pid)


zk = KazooClient(hosts='zoo:2181',timeout=1.0)
zk.start(timeout=1)

zk.ensure_path("/workerss")
if zk.exists("/workerss/node_"+container_id):
    print(container_id,"\t","Node already exists")
else:
	slave_count=0
	for i in zk.get_children("/workerss"):
		data, stat=zk.get("/workerss/"+i)
		data=data.decode('utf-8')
		data=json.loads(data)
		if(data["type"]=="master"):
			break
		else:
			slave_count+=1
	if(len(zk.get_children("/workerss"))==0 or slave_count==len(zk.get_children("/workerss"))):
		zk.create("/workerss/node_"+container_id, b"{\"type\":\"master\",\"pid\":"+str(pid).encode('utf-8')+b",\"cid\":\""+container_id.encode('utf-8')+b"\"}",ephemeral=True)
	else:
		zk.create("/workerss/node_"+container_id, b"{\"type\":\"slave\",\"pid\":"+str(pid).encode('utf-8')+b",\"cid\":\""+container_id.encode('utf-8')+b"\"}",ephemeral=True)

children=zk.get_children("/workerss")
print(container_id,"\t","children : ",children)

data, stat = zk.get("/workerss/node_"+container_id)
print(container_id,"\t","Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

app=Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False
app.config['SQLALCHEMY_DATABASE_URI']='sqlite:///db_'+container_id+'.sqlite3'
db = SQLAlchemy(app)
print(db)
class User(db.Model):
    username = db.Column(db.String(80), unique=True,primary_key=True)
    password = db.Column(db.String(40), nullable=False)

class Rides(db.Model):
    RideID = db.Column(db.Integer, unique=True,primary_key=True)
    Created_by = db.Column(db.String(40), nullable=False)
    Timestamp = db.Column(db.String(20), nullable=False)
    Source = db.Column(db.Integer, nullable=False)
    Destination = db.Column(db.Integer, nullable=False)

class Share(db.Model):
	ID=db.Column(db.Integer, primary_key=True)
	RideID=db.Column(db.Integer,nullable=False)
	User=db.Column(db.String(40),nullable=False)

db.create_all()

data,stat=zk.get('/workerss/node_'+container_id)
data=json.loads(data.decode('utf-8'))
if(data["type"]=="master"):
    master=True
else:
    master=False

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))
channel = connection.channel()

if(master):
	channel.queue_declare(queue='writeQ', durable=True)
	channel.exchange_declare(exchange="sync", exchange_type="fanout")
else:
	channel.queue_declare(queue='readQ', durable=True) #exclusive?
	channel.queue_declare(queue='responseQ', durable=True)
	channel.exchange_declare(exchange="master_or_not", exchange_type="fanout")
	channel.exchange_declare(exchange="sync", exchange_type="fanout")
	result = channel.queue_declare(queue='', exclusive=True)
	channel.queue_bind(exchange='sync', queue=result.method.queue)
	master_result = channel.queue_declare(queue='', exclusive=True)
	channel.queue_bind(exchange='master_or_not', queue=master_result.method.queue)
print(container_id,"\t",' [*] Waiting for messages.')

def write_callback(ch, method, properties, body):
	global container_id
	x=json.loads(body)
	print(container_id,"\t", "[x] Received ", body)
	if("insert_data" in x):
		insert_data=x["insert_data"]
		tab=x["table"]
		newentry=eval(tab)()
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
	db.session.commit()
	ch.basic_ack(delivery_tag=method.delivery_tag)
	channel.basic_publish(exchange="sync",routing_key='',body=body)
	#needs to be written into syncQ
	print(container_id,"\t [x] Done")

def sync_callback(ch, method, properties, body):
	global container_id
	x=json.loads(body)
	print(container_id,"\t"," [x] Received %r" % body)
	if("insert_data" in x):
		insert_data=x["insert_data"]
		tab=x["table"]
		newentry=eval(tab)()
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
	db.session.commit()
	print(container_id,"\t"," [x] Done")
	#time.sleep(3)
	ch.basic_ack(delivery_tag=method.delivery_tag)

def master_callback(ch, method, properties, body):
	global container_id
	global master
	print("$$$$$$$$$$$$$$$$$$$$$ IN MASTER CALLBACK BODY: ",body," $$$$$$$$$$$$$$$$$$$$$$$$$")
	if(container_id==body.decode('utf-8')):
		master=True
		print(container_id,"\t I am the new master, I control everybody")
		ch.basic_cancel(consumer_tag="read"+container_id)
		ch.basic_cancel(consumer_tag="sync"+container_id)
		ch.basic_cancel(consumer_tag="master_result"+container_id)
		ch.basic_consume(queue='writeQ', on_message_callback=write_callback,consumer_tag="write"+container_id)
	print("*********** MASTER STOPPED LISTENING *****************************")
	ch.basic_ack(delivery_tag=method.delivery_tag)

def read_callback(ch, method, properties, body):
	global container_id
	x=json.loads(body)
	flag=0
	cols=x["columns"]
	tab=eval(x["table"])
	if(not(x["where"])):
		q=tab.query.all()
	else:
		if('&' in x["where"]):
			d_form="%d-%m-%Y:%S-%M-%H"
			curr_time=time.strftime(d_form)
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
				q=tab.query.filter((d1==rhs1)&(d2==rhs2)).all()
		else:
			lhs=x["where"].split("=")[0]
			d=getattr(tab,lhs)
			rhs=x["where"].split("=")[1]
			q=tab.query.filter(d==rhs).all()
	response={"info":[]}
	for j in q:
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
			temp[i]=getattr(j,i)
		if(temp):
			response["info"].append(temp)
	print(container_id,"\t",response)
	#response need to be written into responseQ
	channel.basic_publish(exchange='', routing_key='responseQ', body=str(response) ,properties=pika.BasicProperties(delivery_mode=2, correlation_id=properties.correlation_id))
	print(container_id,"\t"," [x] Done")
	ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
if(master):
	print("MASTER CONSUMER TAG = ",channel.basic_consume(queue='writeQ', on_message_callback=write_callback,consumer_tag="write"+container_id))
else:
	channel.basic_consume(queue=result.method.queue, on_message_callback=sync_callback,consumer_tag="sync"+container_id)
	channel.basic_consume(queue=master_result.method.queue, on_message_callback=master_callback,consumer_tag="master_result"+container_id)
	channel.basic_consume(queue='readQ', on_message_callback=read_callback,consumer_tag="read"+container_id)


channel.start_consuming()
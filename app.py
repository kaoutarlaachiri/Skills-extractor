import time
from flask import  Flask,request,render_template,jsonify
import traceback
from flask_cors import CORS
from utils import extract_from_text
import datetime
import requests as rq
import json
import mysql.connector

##

config = {
  'user': 'root',
  'password': 'mysql123',
  'host': '127.0.0.1',
  'database': 'skills',
  'raise_on_warnings': True
} 
def formatdata(data:dict):
    formattted_data = {"matched_tokens":[],"skills":[]}
    formattted_data["matched_tokens"]=list(data.keys())
    formattted_data["skills"]=sum(list(data.values()),[])
    return formattted_data

##
app=Flask(__name__)
CORS(app)

@app.route('/api/DPS', methods=['GET'])
def hello():
     return "hello there"


@app.route('/api/DPS', methods=['POST'])
def check_status():
     result="no text submitted"
     try:
          print("Skills extractor ☺")

          responded = request.get_json()
          print("response",responded["text"])

          if(responded["text"]):
               print("i m in")
               results= extract_from_text(responded["text"])
               data =results
               print("data",formatdata(data))
               print("here are the skills ",data)
               result=jsonify({
                   "response":formatdata(data)
               })
          print("response :", result)
          print("-----------------------------------------------------------------")
          return result

     except Exception as e:
         print( traceback.format_exc() )
         print(e)
     return jsonify({
         "response":"something went wrong"
     })


if __name__=='main' or __name__ == '__main__':
 
    app.run(host="0.0.0.0",debug=True,threaded=True)
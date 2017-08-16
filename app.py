

from flask import Flask
from flask import render_template
from flask import request, url_for
from flask_pymongo import PyMongo
from pymongo import MongoClient
import pprint
import json
import timeit
from operator import and_
from functools import reduce
from collections import defaultdict

app = Flask(__name__)
# mongo = PyMongo(app)
client  = MongoClient('mongodb://localhost:27017/')
# db = client.examples     name  = cs121Index.,  Search_Engine, 

db = client.searchengine



@app.route('/result',methods = ['POST', 'GET'])
def result():
	start = timeit.default_timer()
	if request.method == 'POST':
		searchText = request.form['searchText']
		optionText = request.form['option']
		terms = searchText.split()
		list_of_set = []
		sortedMap = defaultdict(int)
		for query in terms:
			tempSet  = set()
			for docid, tfidf in db.newIndex.distinct(query.lower()):
				tempSet.add(docid.encode("utf-8"))
				sortedMap[docid] += tfidf
			list_of_set.append(tempSet)
			tempSet = set()
		result = reduce((lambda x,y: x&y), list_of_set)
		
		resutlMap = dict()
		for docid in result:
			resutlMap[docid] = sortedMap[docid]#combined tfidf
		if optionText != "Show All":
			diction = sorted(resutlMap.items(), key = lambda k: k[1], reverse = True)[:int(optionText)]
			resultMessage = "showing top " + optionText + " results"
		else:
			diction = sorted(resutlMap.items(), key = lambda k: k[1], reverse = True)
			resultMessage =  "showing all results"
		
		bookKeeping = load_json_file('static/bookkeeping.json')
		length =  str(len(result)) + " Search Results  found  in "
		end = timeit.default_timer()
		time = str(round(end-start,4)) + " secs" 
		

		return render_template('result.html', diction=diction, length = length,time= time, bookKeeping = bookKeeping, number=resultMessage)

def load_json_file(filename):
    with open(filename) as json_data:
        d = json.load(json_data)
    return d

@app.route('/')
def handle_data():
	return render_template('result.html')


if __name__ == "__main__":

	app.debug = True
	app.run()

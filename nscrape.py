import json
import urllib
import sqlite3
import time
import traceback
from datetime import datetime

connection = sqlite3.connect('nmodel.sqlite')
cur = connection.cursor()

#cur.execute('DROP TABLE IF EXISTS Conversion_Rates')

#Create 4 tables for storing data extracted from USDA Nutrient Database
cur.execute('''CREATE TABLE IF NOT EXISTS Nutrients (
    primary_key INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    nutrient_id INTEGER UNIQUE,
    nutrient TEXT UNIQUE
    )''')
cur.execute('''CREATE TABLE IF NOT EXISTS Foods (
    primary_key INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    ndbno INTEGER UNIQUE,
    name TEXT UNIQUE
    )''')
cur.execute('''CREATE TABLE IF NOT EXISTS Food_Nutrition (
    ndbno_id INTEGER UNIQUE,
    nutrient_1 INTEGER,
    gm_1 INTEGER,
    nutrient_2 INTEGER,
    gm_2 INTEGER,
    nutrient_3 INTEGER,
    gm_3 INTEGER,
    nutrient_4 INTEGER,
    gm_4 INTEGER,
    nutrient_5 INTEGER,
    gm_5 INTEGER,
    nutrient_6 INTEGER,
    gm_6 INTEGER,
    nutrient_7 INTEGER,
    gm_7 INTEGER,
    nutrient_8 INTEGER,
    gm_8 INTEGER,
    nutrient_9 INTEGER,
    gm_9 INTEGER,
    nutrient_10 INTEGER,
    gm_10 INTEGER,
    nutrient_11 INTEGER,
    gm_11 INTEGER,
    nutrient_12 INTEGER,
    gm_12 INTEGER,
    nutrient_13 INTEGER,
    gm_13 INTEGER,
    nutrient_14 INTEGER,
    gm_14 INTEGER,
    nutrient_15 INTEGER,
    gm_15 INTEGER,
    nutrient_16 INTEGER,
    gm_16 INTEGER,
    nutrient_17 INTEGER,
    gm_17 INTEGER,
    nutrient_18 INTEGER,
    gm_18 INTEGER,
    nutrient_19 INTEGER,
    gm_19 INTEGER,
    nutrient_20 INTEGER,
    gm_20 INTEGER
    )''')
cur.execute('''CREATE TABLE IF NOT EXISTS Conversion_Rates (
    ndbno_id INTEGER,
    label_1 TEXT,
    equivalent_grams_1 INTEGER,
    label_2 TEXT,
    equivalent_grams_2 INTEGER,
    label_3 TEXT,
    equivalent_grams_3 INTEGER,
    label_4 TEXT,
    equivalent_grams_4 INTEGER,
    label_5 TEXT,
    equivalent_grams_5 INTEGER,
    label_6 TEXT,
    equivalent_grams_6 INTEGER,
    label_7 TEXT,
    equivalent_grams_7 INTEGER,
    label_8 TEXT,
    equivalent_grams_8 INTEGER,
    label_9 TEXT,
    equivalent_grams_9 INTEGER,
    label_10 TEXT,
    equivalent_grams_10 INTEGER
    )''')


###Adding data into the Foods table
#Starts at 1 past the maximum current primary key
#Pass this value into the API call to start at the next set of values (offset point)
def build_foods():    
	start = 0
	cur.execute('SELECT max(primary_key) FROM Foods')
	try:
		row = cur.fetchone()
		if row[0] is not None: 
			start = row[0]
	except:
		start = 0
		row = None
    
	print 'Foods DB starting at primary key: %s' % start

	food_list_url = 'https://api.nal.usda.gov/ndb/list?format=json&max=1500&offset=' + str(start) + '&lt=f&sort=id&api_key=TBNFI5Q52amnHxRxxos3LhneNKb0cmxtytvyZvDK'
	fhandle = urllib.urlopen(food_list_url).read()
	food_data = json.loads(fhandle)

	for entry in food_data['list']['item']:
		id = int(entry['id'])
		try:
			name = str(entry['name'])
		except:
			print 'Error entering the food %s, id %s into the Foods table' % (name, id)
    
		cur.execute('INSERT OR IGNORE INTO Foods (ndbno, name) VALUES ( ?, ? )', ( id, name))
    
	connection.commit()


###This function adds data into the Nutrients table using the same method
def build_nutrients():
	nutrients_start = 0
	cur.execute('SELECT max(primary_key) FROM Nutrients')
	try:
		row = cur.fetchone()
		if row[0] is not None:
			nutrients_start = row[0]
	except:
		nutrients_start = 0
		row = None   

	nutrient_list_url = 'https://api.nal.usda.gov/ndb/list?format=json&max=1500&offset=' + str(nutrients_start) + '&lt=n&sort=id&api_key=TBNFI5Q52amnHxRxxos3LhneNKb0cmxtytvyZvDK'
	nhandle = urllib.urlopen(nutrient_list_url).read()
	nutrients_data = json.loads(nhandle)

	for entry in nutrients_data['list']['item']:
		id = int(entry['id'])
		name = str(entry['name'])
    
		cur.execute('INSERT OR IGNORE INTO Nutrients (nutrient_id, nutrient) VALUES ( ?, ? )', ( id, name))
    
	connection.commit()


###This function builds a DB table with the nutrition information (per 100g) for each food.
#It crawls each food item by selecting the ndbno from the Foods table for the API call.
def build_food_nutrition():
	fn_start = 1001
	cur.execute('SELECT max(ndbno_id) FROM Food_Nutrition')
	try:
		row = cur.fetchone()
		if row[0] is not None: 
			fn_start = row[0]
	except:
		fn_start = 1001
		row = None    
    
	#Grab the PK from FOODS for the highest existing ndbno in FOOD_NUTRITION
	#Set a pk variable to iterate with and reference for grabbing ndbno information from FOODS
	cur.execute('SELECT primary_key FROM Foods WHERE ndbno = ? ', ( fn_start, ))
	pk = cur.fetchone()[0]

	print 'Food_Nutrition DB starting at primary key: %s' % pk

	#Set a counter to limit number of calls
	try:
		#input = raw_input('How many foods would you like to add? ')
		count = 490 #int(input)
	except:
		exit()

	while True:
		if count == 0: break
    
		cur.execute('SELECT ndbno FROM Foods WHERE primary_key = ? ', ( pk, ))
		url = None

		#Build the API call using the ndbno for the current food (referenced by pk)
		for row in cur:
			if len(str(row[0])) <= 4:
				url = 'https://api.nal.usda.gov/ndb/nutrients/?format=json&ndbno=0' + str(row[0]) + '&api_key=TBNFI5Q52amnHxRxxos3LhneNKb0cmxtytvyZvDK&nutrients=203&nutrients=204&nutrients=205&nutrients=208&nutrients=269&nutrients=291&nutrients=301&nutrients=303&nutrients=304&nutrients=306&nutrients=307&nutrients=318&nutrients=323&nutrients=324&nutrients=401&nutrients=415&nutrients=418&nutrients=601&nutrients=605&nutrients=606'
			else:
				url = 'https://api.nal.usda.gov/ndb/nutrients/?format=json&ndbno=' + str(row[0]) + '&api_key=TBNFI5Q52amnHxRxxos3LhneNKb0cmxtytvyZvDK&nutrients=203&nutrients=204&nutrients=205&nutrients=208&nutrients=269&nutrients=291&nutrients=301&nutrients=303&nutrients=304&nutrients=306&nutrients=307&nutrients=318&nutrients=323&nutrients=324&nutrients=401&nutrients=415&nutrients=418&nutrients=601&nutrients=605&nutrients=606'
			count -= 1
    
			#Open the url calling JSON format nutrient info for the current food/ndbno    
			handle = urllib.urlopen(url).read()
			data = json.loads(handle)
    
			#Add the ndbno to the table row.
			try:
				current_ndbno = int(data['report']['foods'][0]['ndbno'])
			except:
				print 'Encountered an error when indexing into primary key %s' % pk
				continue
			cur.execute('INSERT OR IGNORE INTO Food_Nutrition (ndbno_id) VALUES ( ? )', (current_ndbno, ))
    
			#Then, loop through the JSON data and add the nutrition information for the food.
			#The tracker is used to increment the column we are adding info to in FOOD_NUTRITION
			tracker = 1
			for entry in data['report']['foods'][0]['nutrients']:
				nutrient_id = entry['nutrient_id']
				gm = entry['gm']
				if isinstance(gm, unicode):
					gm = 0
        
				a = 'nutrient_' + str(tracker)
				b = 'gm_' + str(tracker)
        
				cur.execute('UPDATE Food_Nutrition SET {}=?, {}=? WHERE ndbno_id=?'.format(a, b), (nutrient_id, gm, current_ndbno))
				tracker += 1
		pk += 1
	connection.commit()

### The following section builds the database of conversion rates to grams of certain serving types of foods.
#Similar to the 'Food_Nutrition' table, it will make API calls using ndbnos from the 'Foods' table.
def build_conversion_rates():
	fn_start = 1001
	cur.execute('SELECT max(ndbno_id) FROM Conversion_Rates')
	try:
		row = cur.fetchone()
		if row[0] is not None:
			fn_start = row[0]
	except:
		fn_start = 1001
		row = None    
    
	#Grab the PK from 'Foods' for the highest existing ndbno in 'Conversion_Rates'
	#Set a pk variable to iterate with and reference for grabbing ndbno information from 'Foods'
	cur.execute('SELECT primary_key FROM Foods WHERE ndbno = ? ', ( fn_start, ))
	pk = cur.fetchone()[0]

	print 'Conversion_Rates DB starting at primary key: %s' % pk

	#Set a counter to limit number of calls
	try:
		#input = raw_input('How many foods would you like to add to the conversion rates table? ')
		count = 490 #int(input)
	except:
		exit()

	while True:
		if count == 0: break
    
		cur.execute('SELECT ndbno FROM Foods WHERE primary_key = ? ', ( pk, ))
		url = None

		#Build the API call using the ndbno for the current food (referenced by pk)
		for row in cur:
			#Account for 4 digit ndbnos that need the leading zero added for the call
			if len(str(row[0])) <= 4:
				url = 'https://api.nal.usda.gov/ndb/reports/?ndbno=0' + str(row[0]) + '&type=b&format=json&api_key=TBNFI5Q52amnHxRxxos3LhneNKb0cmxtytvyZvDK'
			else:
				url = 'https://api.nal.usda.gov/ndb/reports/?ndbno=' + str(row[0]) + '&type=b&format=json&api_key=TBNFI5Q52amnHxRxxos3LhneNKb0cmxtytvyZvDK'
			count -= 1
    
			#Open the url calling JSON format conversion rate info for the current food/ndbno    
			cr_handle = urllib.urlopen(url).read()
			data = json.loads(cr_handle)
    
			#Add the ndbno to the table row.
			try:
				current_ndbno = int(data['report']['food']['ndbno'])
			except:
				print 'Encountered an error when indexing into primary key %s' % pk
				continue
			cur.execute('INSERT OR IGNORE INTO Conversion_Rates (ndbno_id) VALUES ( ? )', (current_ndbno, ))
    
			#Then, loop through the JSON data and add the nutrition information for the food.
			#The tracker is used to increment the column we are adding info to in FOOD_NUTRITION
			tracker = 1
			for entry in data['report']['food']['nutrients'][0]['measures']:
				try:
					label = entry['label']
				except:
					print 'Error finding label for primary key %s' % pk
					continue
            
				conversion_rate = float(entry['eqv'])/100
				equivalent_grams = str(conversion_rate)
        
				a = 'label_' + str(tracker)
				b = 'equivalent_grams_' + str(tracker)
        
				try:
					cur.execute('UPDATE Conversion_Rates SET {}=?, {}=? WHERE ndbno_id=?'.format(a, b), (label, equivalent_grams, current_ndbno))
				except:
					print "Error adding conversion rate data into column for primary key %s" % pk
					traceback.print_exc()
					pass
					tracker += 1
		pk += 1
        
	connection.commit()
	
print 'Executing at: ' + str(datetime.now())	
build_foods()
#build_nutrients()
build_food_nutrition()
build_conversion_rates()	   
print 'Complete at: ' + str(datetime.now())
    










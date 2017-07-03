###The purpose of this program is to allow the user to search for foods to add to a meal.
###They will be able to specify the serving type and quantity.
###The system will convert the nutrients in the food using the serving type conversion rate and quantity
###The meal will be stored in the list 'meal'.
###They may save the meal to add it to their database of meals.

import sqlite3
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import string

foods = []
meal = {'name':None, 'foods':[], 'nutrients':None}

connection = sqlite3.connect('nmodel.sqlite')
cur = connection.cursor()

#cur.execute('DROP TABLE IF EXISTS Meals')

cur.execute('''CREATE TABLE IF NOT EXISTS Meals (
primary_key INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
user_id INTEGER,
name TEXT,
food_1 INTEGER,
serving_1 INTEGER,
quantity_1 INTEGER,
food_2 INTEGER,
serving_2 INTEGER,
quantity_2 INTEGER,
food_3 INTEGER,
serving_3 INTEGER,
quantity_3 INTEGER,
food_4 INTEGER,
serving_4 INTEGER,
quantity_4 INTEGER,
food_5 INTEGER,
serving_5 INTEGER,
quantity_5 INTEGER,
food_6 INTEGER,
serving_6 INTEGER,
quantity_6 INTEGER,
food_7 INTEGER,
serving_7 INTEGER,
quantity_7 INTEGER,
food_8 INTEGER,
serving_8 INTEGER,
quantity_8 INTEGER,
food_9 INTEGER,
serving_9 INTEGER,
quantity_9 INTEGER,
food_10 INTEGER,
serving_10 INTEGER,
quantity_10 INTEGER,
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

###This function handles the search for different foods.
###It takes user input, and compares it to each item in 'Foods' using the Levenshtein ratio.
###It will put the best match for the search in the list 'lst'.
###Next, the program will build dictionaries of the conversion rates and nutrient information of the 'best_match' food.
###These dictionaries will be used by the 'calculate_nutrition' function.
def build_meal():
    global foods, meal

    search = raw_input('What food would you like to add?').upper()

    cur.execute('SELECT max(primary_key) FROM Foods')
    count = cur.fetchone()[0]

    cur.execute('SELECT ndbno, name FROM Foods')
    lst = []

    for i in cur:
        if count == 0: break

        text = str(i[1]).upper()
        if ', UPC' in text:
            text = text.split(', UPC')
            text = text[0]
        if 'CHEESE, ' in text:
            text = text.split(', ')
            text = str(text[1]) + ' ' + str(text[0]) + ' ' + str(text[2:])
        text = text.translate(None, string.punctuation)

        ratio = fuzz.ratio(search, text)
        if ratio >= 50:
            tpl = (i[0], text)
            lst.append(tpl)
        count -= 1  

    best_match = process.extract(search, lst, limit=3)
    ndbno = best_match[0][0][0]

    conversion_dict = {ndbno: {'grams': .01, 'milligrams': .00001}}
    nutrients_dict = {'food': ndbno, 'nutrients': {}}

    tracker = 1
    for i in range(0,10):
        cur.execute('SELECT label_{}, equivalent_grams_{} FROM Conversion_Rates WHERE ndbno_id = {}'.format(tracker, tracker, ndbno))
        entry = cur.fetchall()[0]
        if entry[0] == None: break
    
        label = str(entry[0])
        equivalent_grams = entry[1]
        conversion_dict[ndbno][label] = equivalent_grams
    
        tracker += 1
    print conversion_dict

    tracker = 1
    for i in range(0,20):
        cur.execute('SELECT nutrient_{}, gm_{} FROM Food_Nutrition WHERE ndbno_id = {}'.format(tracker, tracker, ndbno))
        entry = cur.fetchall()[0]
        nutrient_id = entry[0]
        gm = entry[1]
        if isinstance(gm, unicode):
            gm = 0
    
        nutrients_dict['nutrients'][nutrient_id] = gm
    
        tracker += 1
    print nutrients_dict

    ###The following section will prompt the user to enter the serving size of the food item (global ndbno).
    ###Their input options are the labels/serving types in the DB or a direct entry of grams or milligrams.
    ###After they enter the serving type they will be prompted to indicate the quantity.
    ###Using the quantity and conversion rate, it will build update the food's nutrition dictionary (nutrients_dict).
    ###This nutrient_dict will be added to the 'meal' list.
    ###The program will ask the user if they want to continue to add more foods to the meal or stop.
    labels_list = []
    for i in conversion_dict[ndbno]: labels_list.append(i)
    
    for i in labels_list: print i

    try:
        serving_entry = raw_input('Which of the above serving types did you add? Enter grams or milligrams if none of them match: ').upper()
        labels_list = map(lambda x:x.upper(), labels_list)
        serving_match = process.extract(serving_entry, labels_list, limit=1)
        serving = serving_match[0][0]
    except:
        print 'Incorrect serving type entry. Try Again'
    quantity = float(raw_input('Enter the quantity here: '))
    
    nutrients_dict['serving'] = serving
    nutrients_dict['quantity'] = quantity
    
    print nutrients_dict
    
    conversion_rate = None
    for i in conversion_dict[ndbno]:
        if i.upper() == serving:
            conversion_rate = conversion_dict[ndbno][i]
    conversion_rate *= quantity
    
    for i in nutrients_dict['nutrients']:
        try:
            nutrients_dict['nutrients'][i] *= conversion_rate
        except:
            print 'Error converting nutrient %s in dictionary' % i
            
    print nutrients_dict

    foods.append(nutrients_dict)
    
    option = raw_input('Would you like to add another item to the meal? Say Yes or No: ').upper()
    
    if option == 'YES':
        build_meal()
        
    elif option == 'NO':
        save = raw_input('Would you like to save this meal? Say Yes or No: ').upper()
        
        if save == 'YES':
            meal_name = raw_input('What would you like to name the meal? ')
            meal['name'] = meal_name
            cur.execute('INSERT INTO Meals (name) VALUES ( ? )', (meal_name, ))
            connection.commit()
            cur.execute('SELECT max(primary_key) FROM Meals')
            pk = cur.fetchone()[0]
            count = 1
            for i in foods:
                meal['foods'].append({'ndbno':i['food'], 'serving':i['serving'], 'quantity':i['quantity']})
                if count == 1:
                    meal['nutrients'] = i['nutrients']
                    count += 1
                    
                elif count >= 2:
                    for key in i['nutrients']:
                        meal['nutrients'][key] += i['nutrients'][key]
            tracker = 1
            for i in meal['nutrients']:
                nutrient_id = i
                quantity = meal['nutrients'][i]
                
                cur.execute('UPDATE Meals SET nutrient_{}={}, gm_{}={} WHERE primary_key={}'.format(tracker, nutrient_id, tracker, quantity, pk))
                tracker += 1
            tracker = 1
            for i in meal['foods']:
                food = i['ndbno']
                serving = i['serving']    
                quantity = i['quantity']
                
                cur.execute('UPDATE Meals SET food_{}=?, serving_{}=?, quantity_{}=? WHERE primary_key=?'.format(tracker, tracker, tracker), (food, serving, quantity, pk))
                tracker += 1
                
        else:    
            print 'Meal complete, not saved.'
            new_meal = raw_input("Would you like to create a new meal? Say Yes or No: ").upper()
            if new_meal == 'YES':
                foods = []
                build_meal()
            elif new_meal == 'NO':
                print foods
                exit()
    else:
        exit()    
    
build_meal()
connection.commit()
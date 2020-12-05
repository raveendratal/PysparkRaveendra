# Databricks notebook source
df1 = spark.read.format("csv").load("dbfs:/FileStore/tables/emp.csv",header=True,inferSchema=True,sep=',')
display(df1)

# COMMAND ----------

df1.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dictionary
# MAGIC 
# MAGIC '''Dictionary in Python is an unordered collection of data values, used to store data values like a
# MAGIC    map, which unlike other Data Types that hold only single value as an element,It is a collection of key value pair , Each key is connected to a value , 
# MAGIC    value can be a number, string, list, another dictionary or an object'''

# COMMAND ----------

# MAGIC %md
# MAGIC [1. Initialization & Declaration](#h1)<br>
# MAGIC [2. Dictionary manipulation](#h2)<br>
# MAGIC > [2.1 modify a dict's value](#h2.1)<br>
# MAGIC [2.2 deleting a whole dict](#h2.2)<br>
# MAGIC [2.3 add items to a dict](#h2.3)<br>
# MAGIC [2.4 delete items from a dict](#h2.4)
# MAGIC [3. Looping through a dict](#h3)<br>
# MAGIC > [3.1 looping through values](#h3.1)<br>
# MAGIC [3.2 looping through keys](#h3.2)<br>
# MAGIC [3.3 looping through both(keys & values](#h3.3)
# MAGIC [4. Looping through a dict in ascending order](#h4)<br>
# MAGIC [5. Looping through all unique values in a dict](#h5)<br>
# MAGIC [6. Making a list of dict](#h6)<br>
# MAGIC > [6.1 accessing to each dict](#h6.1)<br>
# MAGIC [6.2 finding all values in a list of dict](#h6.2)<br>
# MAGIC [6.3 finding all keys in a list of dict](#h6.3)<br>
# MAGIC [6.4 print key value pair in simple form from a list of dict](#h6.4)<br>
# MAGIC [6.5 finding particular info from a list of dict](#h6.5)<br>
# MAGIC [6.6 finding particular value from all dicts in a list of dicts](#h6.6)<br>
# MAGIC [6.7 find index of particular value from a list of dict(using for loop)](#h6.7)<br>
# MAGIC [6.8 find index of particular value from a list of dict(using while loop)](#h6.8)
# MAGIC [7. Creating a list in a dict](#h7)<br>
# MAGIC >[7.1 accessing to a list of each dict's key](#h7.1)<br>
# MAGIC [7.2 finding first value of list in all keys](#h7.2)<br>
# MAGIC [7.3 accessing to a list of a key](#h7.3)<br>
# MAGIC [7.4 accessing to a value of a key](#h7.4)<br>
# MAGIC [7.5 accessing to all list values of a key one by one](#h7.5)<br>
# MAGIC [7.6 accessing all keys in a dict](#h7.6)<br>
# MAGIC [7.7 checking whether required key is available in dict](#h7.7)<br>
# MAGIC [7.8 checking whether required value is available in dict](#h7.8)<br>
# MAGIC [7.9 checking whether required key or value is available in dict](#h7.9)
# MAGIC [8. Dictionary in  dictionary](#h8)<br>
# MAGIC [9. Find if a key present in dict or not](#h9)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h1'></a>
# MAGIC ### 1- Initialization & Declaration

# COMMAND ----------


student_01 = {'name': "Reshwanth", 'roll_no': "2011-TR-123",'depart': "Software Engineering"}
student_01


# COMMAND ----------

# MAGIC %md
# MAGIC ### OR

# COMMAND ----------

student_02 = {}
student_02['name'] = "Vikranth" 
student_02['roll_no'] = "2015-SE-142"
student_02['depart'] = "SOftware Engineering"
student_02

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h2'></a>
# MAGIC ### 2- Dictionary Manipulation

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h2.1'></a>
# MAGIC #### 2.1 modifying a dict's value

# COMMAND ----------

student_01['depart'] = "Data Science"
student_01

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h2.2'></a>
# MAGIC #### 2.2 deleting whole dict

# COMMAND ----------

del(student_02)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h2.3'></a>
# MAGIC #### 2.3 add items to a dict

# COMMAND ----------

student_01['registration_no'] = "0153-TR-2015"
student_01

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h2.4'></a>
# MAGIC #### 2.4 delete items from a dict

# COMMAND ----------

del student_01['registration_no']
student_01

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note:
# MAGIC When you are defining a dictionary containing more than 2 or 3 items
# MAGIC    then it is a good idea to break the items into seperatye lines for readability

# COMMAND ----------

favourite_leaders = {
 'a': "Narendra Modi",
 'b': "Abdul Kalam",
 'c': "KCR",
 'd': "Y S JAGAN", #
}
favourite_leaders

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h3'></a>
# MAGIC ### 3- Looping through a dictionary

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h3.1'></a>
# MAGIC #### 3.1 looping through values

# COMMAND ----------

for value in student_01.values(): # value is a variable where all values are store one by one
    print(value)                  # .values() is a method for taking values from keys
    

# COMMAND ----------

for leader in favourite_leaders.values():
    print(leader)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h3.2'></a>
# MAGIC #### 3.2 Looping through keys

# COMMAND ----------

for key in student_01.keys(): # key is a variable where all keys are store one by one
    print(key)              # .keys() is a method for taking keys from dict


# COMMAND ----------

for key in favourite_leaders.keys():
    print(key) # print all keys in a dictionary

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h3.3'></a>
# MAGIC #### 3.3 Looping through both (keys & values)

# COMMAND ----------

for key,value in student_01.items(): # .items is a method that return both keys and value
    print(key+": "+value) # print key and its value in simple form           


# COMMAND ----------

for key,value in favourite_leaders.items():
    print(key+": "+value)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h4'></a>
# MAGIC ### 4- Looping through a dict in ascending order

# COMMAND ----------

'''If we want to loop through dictionary in order we have to first sort the keys'''
for key in sorted(favourite_leaders):
    print(key)
    

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h5'></a>
# MAGIC ### 5- Looping through all unique values in a dict
# MAGIC #### Note
# MAGIC In dictionary repeated values of a single keys can come so we use sets to overcome this situation

# COMMAND ----------

favourite_leaders['e'] = 'Rajini Kanth'
favourite_leaders # here repetition of value occur(Imran noname comes 2 time with key 'a' and with key 'e')

#TO overcome above situation we use sets(In set value(Imran noname) comes only one time'''
for leader in set(favourite_leaders.values()): 
    print(leader)

# COMMAND ----------

favourite_leaders['c'] = "Sonia" 
favourite_leaders

# COMMAND ----------

for k,v in set(favourite_leaders.items()): # set wala funda sirf or sirf "value" par chata h 
    print(k+": "+v)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6'></a>
# MAGIC ### 6- Making a list of dictionaries

# COMMAND ----------

students = [  # in list of dictionaries,we not assign name of different dictionaries
    {
    'name': "Reshwanth",
    'roll_no': "2015-TR-143",
    'depart': "Software Engineering",
},
    {
    'name': "Vikranth",
    'roll_no': "2015-VK-142",
    'depart': "Computer Engineering",
},
    {
    'name': "Srini ",
    'roll_no': "2019-SR-123",
    'depart': "Mathematics",
},
]
students

# COMMAND ----------

# MAGIC %md
# MAGIC    # OR

# COMMAND ----------

'''initialising dictionaries and appending them in a list'''
users = []
user_1 = {
    'first_name': "Reshwanth",
    'last_name': "TR",
    'e_mail': "reshwanth.tr@gmail.com",
    'password': "123",
}
user_2 = {
    'first_name': "Vikranth",
    'last_name': "TR",
    'e_mail': "vikranth.tr@gmail.com",
    'password': "321",
}
user_3 = {
    'first_name': "KAMAL",
    'last_name': "HASAN",
    'e_mail': "kamal.hasan@hotmail.com",
    'password': "245",
}
users.append(user_1)   
users.append(user_2)  
users.append(user_3)
users

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6.1'></a>
# MAGIC #### 6.1 Accesing to each dict

# COMMAND ----------

students[0] # dictionary at index 0

# COMMAND ----------

students[1] # dictionary at index 1

# COMMAND ----------

students[2] # dictionary at index 2

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6.2'></a>
# MAGIC #### 6.2 finding all values in a list of dict 

# COMMAND ----------

for student in students: # looping through list
    for value in student.values(): # looping through values in dictionary
        print(value)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6.3'></a>
# MAGIC #### 6.3 finding all keys in a list of dict

# COMMAND ----------

for student in students:
    for key in student.keys():
        print(key)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6.4'></a>
# MAGIC #### 6.4 print key value pair in simple form from a list of dict

# COMMAND ----------

for student in students:
    for key,value in student.items():
        print(key+": "+value)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6.5'></a>
# MAGIC #### 6.5 finding particular info from a list of dict

# COMMAND ----------

# MAGIC %md
# MAGIC Q. In list of dictionaries, a group of dictionbaries hava no name, so how to get particular inforamation from a dict<br>
# MAGIC Ans. 2 things are used, 1st index of list, 2nd value of dict
# MAGIC      we should know index of required value of particular dict

# COMMAND ----------

print(users[0]['e_mail'])

# COMMAND ----------

print(users[1]['e_mail'])

# COMMAND ----------

print(users[2]['password'])

# COMMAND ----------

print(users[1]['first_name'])

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6.6'></a>
# MAGIC #### 6.6 finding particular value from all dicts in a list of dicts

# COMMAND ----------

for user in users:
    print(user['e_mail'])

# COMMAND ----------

for user in users:
    full_name = user['first_name']+" "+user['last_name']
    print(full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6.7'></a>
# MAGIC #### 6.7 find index of particular value from a list of dict (using for loop)

# COMMAND ----------

i = 0
for user in users:
    if user['first_name'] == 'KAMAL':
        break
    else:
        i+= 1
print("index of required value's dictionary is "+str(i))

# COMMAND ----------

print(users[i]['first_name'])  

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h6.8'></a>
# MAGIC #### 6.8 find index of particular value from a list of dict (using while loop)

# COMMAND ----------

i = 0
while users:
    if users[i]['first_name'] == 'Reshwanth':
        break
    else:
        i+= 1
print("index of required value's dictionary is "+str(i))

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7'></a>
# MAGIC ### 7. Creating a list in a dictionary

# COMMAND ----------

teachers = {
    'programming Fundamentals': ['Prakash','DADA','SRINI'],
    'MATHS': ['Vinod','Khadar','Bala'],
    'SC': ['Venkat','Kamal' ],
}
print(teachers)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7.1'></a>
# MAGIC #### 7.1 Accessing to a list of each dict's key

# COMMAND ----------

sub_no = 0
for sub,teacher in teachers.items():
    sub_no+= 1
    print("          "+"Subject"+str(sub_no)+": "+sub)
    no = 0
    for i in teacher:
        no+= 1
        print(str(no)+"- "+i)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7.2'></a>
# MAGIC #### 7.2 finding first value of a list in all keys

# COMMAND ----------

'''for finding first teacher of all subjects'''
for teacher in teachers.values():
    print(teacher[0])

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7.3'></a>
# MAGIC #### 7.3 accessing to list of a key

# COMMAND ----------

print(teachers['SC'])

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7.4'></a>
# MAGIC #### 7.4 accessing to a list value of a key

# COMMAND ----------

print(teachers['MATHS'][0])

# COMMAND ----------

print(teachers['MATHS'][1])

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7.5'></a>
# MAGIC ### 7.5 accessing to all list values of a key one by one

# COMMAND ----------

'''print all terachers of programming fundamentals'''
i = 0
for teacher in teachers.values():
    print(str(i+1)+"- "+teachers['programming Fundamentals'][i])
    i+= 1

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7.6'></a>
# MAGIC #### 7.6 accessing all keys in a dict

# COMMAND ----------

'''print all available subjects'''
i = 0
for sub in teachers.keys():
    print(str(i+1)+"- "+sub)
    i+= 1
    

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7.7'></a>
# MAGIC #### 7.7 checking whether reqired key is available in dict

# COMMAND ----------

if 'DES' in teachers.keys():
    print('Yes')

# COMMAND ----------

if 'Maths' in teachers.keys():
    print('Yes')
else:
    print('No')

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='h7.8'></a>
# MAGIC #### 7.8 checking whether required value is available in dict

# COMMAND ----------

if 'Srini' in teachers.values():  
    print('Yes')
else:
    print('No')

# COMMAND ----------

for v in teachers.values():
    if 'Prakash' in v:
        print('Yes')

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='7.9'></a>
# MAGIC #### 7.9 checking whether reqired key or value in dict

# COMMAND ----------

for k,v in teachers.items():
    if 'Vikranth' in v:
        print("Subject: "+k)

# COMMAND ----------

for k,v in teachers.items():
    if 'DADA' in v:
        print("Subject: "+k)

# COMMAND ----------

for k,v in teachers.items():
    if 'Reshwanth' in v:
        print("Subject: "+k)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='8'></a>
# MAGIC ### 8. Dictionary in a dictionary

# COMMAND ----------

users = {
    'Venkat': {
    'first_name': "Raju",
    'last_name': "aRaju",
    'e_mail': "aaRaju94@gmail.com",
    'password': "123",
},
    'mohan 123': {
    'first_name': "mohan",
    'last_name': "noname",
    'e_mail': "mohannoname123@gmail.com",
    'password': "321",
},
     'Bilal490': {
    'first_name': "Kamal Hasan",
    'last_name': "Hasan",
    'e_mail': "bilalHasan@hotmail.com",
    'password': "245",
},

}
users

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Note
# MAGIC We can call dictionary in a dictionary is a nested dictionary. So all the operations in a dictionary can be applied in a nested dict in a nested way.

# COMMAND ----------

for key in users.keys():
    print(key)

# COMMAND ----------

for value in users.values():
    print(value)

# COMMAND ----------

for value in users.values():
    for val in value.values():
        print(val)

# COMMAND ----------

for value in users.values():
    for key in value.keys():
        print(key)

# COMMAND ----------

i = 0
for key,value in users.items():
    print("\n")
    print("User_"+str(i+1)+": "+key)
    i+= 1
    j = 0
    for k,v in value.items():
        print(""+str(j+1)+"- "+k+": "+v)
        j+= 1

# COMMAND ----------

'''find user through e_mail'''
for key,value in users.items():
    for k,v in value.items():
        if 'bilalHasan@hotmail.com' in v:
            print(key)

# COMMAND ----------

'''find user through e_mail'''
for key,value in users.items():
    for k,v in value.items():
        if 'mohannoname123@gmail.com' in v:
            print(key)

# COMMAND ----------

'''for finding information of particular user in the form of dictionary'''
for key,value in users.items():
    if 'Venkat' in key:
        print(value)

# COMMAND ----------

print(users['Venkat'])

# COMMAND ----------

print(users['Venkat']['e_mail'])

# COMMAND ----------

'''for finding information of particular user in simple form'''
i = 0
for key,value in users.items():
    if 'mohan 123' in key:
        for k,v in value.items():
            i+= 1
            print(str(i)+"- "+k+": "+v)

# COMMAND ----------

'''for finding particular information of user'''
for key,value in users.items():
    print("UserName: "+key)
    for k,val in value.items():
        if 'e_mail' in k:
            print("Email: "+val)
        
        

# COMMAND ----------

'''for finding particular information of user'''
for key,value in users.items():
    print("UserName: "+key)
    for k,val in value.items():
        if 'password' in k:
            print("Password: "+str(val))
        
        

# COMMAND ----------

'''Another easy and short way'''
for key in users.keys():
    print("UserName: "+key)
    print("Email: "+users[key]['e_mail'])

# COMMAND ----------

'''Another easy and short way'''
for key in users.keys():
    print("UserName: "+key)
    print("Password: "+users[key]['password'])

# COMMAND ----------

'''for finding particular information of particular user'''
for key,value in users.items():
    if 'Venkat' in key:
        for k,v in value.items():
            if 'first_name' in k:
                first_name = v
            if 'last_name' in k:
                last_name = v
print("Full Name: "+first_name+" "+last_name)

# COMMAND ----------

'''for finding particular information of particular user'''
for key,value in users.items():
    if 'mohan 123' in key:
        for k,v in value.items():
            if 'first_name' in k:
                first_name = v
            if 'last_name' in k:
                last_name = v
print("Full Name: "+first_name+" "+last_name)

# COMMAND ----------

'''Another way'''
full_name = users['Venkat']['first_name'] + " " + users['Venkat']['last_name']
print(full_name)

# COMMAND ----------

'''Another way'''
full_name = users['mohan 123']['first_name'] + " " + users['mohan 123']['last_name']
print(full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC <a id='9'></a>
# MAGIC ### 9. Find if a key present in dict or not

# COMMAND ----------

dict = {1: 10, 2: 20, 3: 30, 4: 40, 5: 50, 6: 60}
if 1 in dict: 
    print("yes")  
else:
    print("no")

# COMMAND ----------

dict = {1: 10, 2: 20, 3: 30, 4: 40, 5: 50, 6: 60}
if 10 in dict: 
    print("yes")  
else:
    print("no")   
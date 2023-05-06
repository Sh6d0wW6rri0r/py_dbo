#    main.py - example and testing program.
#    Copyright (C) 2023 Olivier Moulin.
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    any later version.

#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.

#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import psycopg2
from psycopg2 import Error

from DBModel import DBModel


connection=None
try:
    connection = psycopg2.connect(user="pydbo",
                                  password="pydbo",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="pyDBO")
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

model = DBModel(connection)
model.initDatabase()
print("Creating Person and Job objects definition :")
model.createObject("person")
model.addObjectProperty("person","name","varchar",255)
model.addObjectProperty("person","age","varchar",255)
model.addObjectCalculatedProperty("person","job_count","varchar",255,"Select COUNT(DSTID) FROM works_as WHERE ORIID=@ID@")
model.createObject("job")
model.addObjectProperty("job","name","varchar",255)
model.addObjectProperty("job","salary","varchar",255)
print("Displaying Person object definition :")
for r in model.getObjectDescription("person"):
    print(r)
print("Instanciating John Smith person object :")
john = model.instanciateObject("person")
john.setProperty("name","John Smith")
john.setProperty("age","40")
print("Retrieving instance of John Smith person object :")
john2 = model.retrieveObject("person",0)
print("Print John Smith values :")
for r in john2.getObjectProperties():
    print(r[0]," -> ",r[1])
print("Instanciating Programmer job object :")
programmer = model.instanciateObject("job")
programmer.setProperty("name","Programmer")
programmer.setProperty("salary","80 K USD")
print("Linking John Smith object to Programmer object :")
model.createLink("works_as","person","job")
john.linkObject("works_as",0)
print("job_count : ",john.getProperty("job_count"))
print("Listing all jobs linked to John Smith with works_as relation :")
lst = john.getListLinked("works_as")
for l in lst:
    print(l.getObjectName())
    for r in l.getObjectProperties():
        print(r[0]," -> ",r[1])
print("Testing inheritage :")
model.createObject("animal")
model.addObjectProperty("animal","name","varchar",255)
model.addObjectProperty("animal","weight","varchar",255)
model.createObject("dog","animal")
model.addObjectProperty("dog","bark","varchar",255)
model.createObject("farm_animal","animal")
model.addObjectProperty("farm_animal","type","varchar",255)
model.createObject("cow","farm_animal")
model.addObjectProperty("cow","milk","varchar",255)
print("Animal object :")
for r in model.getObjectDescription("animal"):
    print(r)
print("Dog object :")
for r in model.getObjectDescription("dog"):
    print(r)
print("Farm animal object :")
for r in model.getObjectDescription("farm_animal"):
    print(r)
print("Cow object :")
for r in model.getObjectDescription("cow"):
    print(r)
print("Adding height property to animal class")
model.addObjectProperty("animal","height","varchar",255)
print("Adding farm_location property to farm_animal class")
model.addObjectProperty("farm_animal","farm_location","varchar",255)
print("Display the object after modification :")
print("Animal object :")
for r in model.getObjectDescription("animal"):
    print(r)
print("Dog object :")
for r in model.getObjectDescription("dog"):
    print(r)
print("Farm animal object :")
for r in model.getObjectDescription("farm_animal"):
    print(r)
print("Cow object :")
for r in model.getObjectDescription("cow"):
    print(r)

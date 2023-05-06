#    DBModel.py - implementation of the database Model.
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

from DBObject import DBObject

class DBModel:
    def __init__ (self,connection):
        self.connection = connection


    def initDatabase(self):
        if (self.connection):
            cursor=self.connection.cursor()
            cursor.execute("drop table if exists dbo_objects")
            cursor.execute("create table dbo_objects (OBJname varchar(255),inherit varchar(255),instance int)")
            cursor.execute("drop table if exists dbo_properties")
            cursor.execute("create table dbo_properties (OBJname varchar(255),PRPname varchar(255),PRPtype varchar(255),PRPsize int,inherited varchar(255),calculation varchar(4000))")
            cursor.execute("drop table if exists dbo_links")
            cursor.execute("create table dbo_links (LNKname varchar(255),ORIname varchar(255),DSTname varchar(255))")
            cursor.close()
            self.connection.commit()
        else:
            print("ERROR - Database not connected.")

    def createObject(self,objname,inherit="None"):
        cursor=self.connection.cursor()
        cursor.execute("INSERT INTO dbo_objects VALUES('"+objname+"','"+inherit+"',0)")
        cursor.execute("INSERT INTO dbo_properties VALUES('"+objname+"','ID','varchar',255,'Local','None')")
        cursor.execute("CREATE TABLE "+objname+" (ID varchar(255))")
        cursor.close()
        self.connection.commit()
        if inherit!="None":
            for r in self.getObjectDescription(inherit):
                if r[0]!="ID":
                    self.addObjectProperty(objname,r[0],r[1],r[2],"Inherited") 

    def addObjectProperty(self,objname, propname,proptype,propsize,inherit="Local"):
        cursor=self.connection.cursor()
        cursor.execute("INSERT INTO dbo_properties VALUES('"+objname+"','"+propname+"','"+proptype+"',"+str(propsize)+",'"+inherit+"','None')")
        cursor.execute("ALTER TABLE "+objname+" ADD COLUMN "+propname+" "+proptype+"("+str(propsize)+")")
        cursor.close()
        self.connection.commit()
        cursor=self.connection.cursor()
        cursor.execute("select OBJname from dbo_objects WHERE inherit='"+objname+"'")
        rec = cursor.fetchall()
        for r in rec:
            self.addObjectProperty(r[0],propname,proptype,propsize,"Inherited") 
        cursor.close()

    def addObjectCalculatedProperty(self,objname, propname,proptype,propsize,calculation,inherit="Local"):
        cursor=self.connection.cursor()
        cursor.execute("INSERT INTO dbo_properties VALUES('"+objname+"','"+propname+"','"+proptype+"',"+str(propsize)+",'"+inherit+"','"+calculation+"')")
        cursor.execute("ALTER TABLE "+objname+" ADD COLUMN "+propname+" "+proptype+"("+str(propsize)+")")
        cursor.close()
        self.connection.commit()
        cursor=self.connection.cursor()
        cursor.execute("select OBJname from dbo_objects WHERE inherit='"+objname+"'")
        rec = cursor.fetchall()
        for r in rec:
            self.addObjectCalculatedProperty(r[0],propname,proptype,propsize,calculation,"Inherited") 
        cursor.close()

    def removeObjectProperty(self,objname, propname,force=False):
        cursor=self.connection.cursor()
        cursor.execute("select inherit from dbo_properties WHERE OBJname='"+objname+"' and PRPname='"+propname+"'")
        r = cursor.fetchone()[0]
        cursor.close()
        if r=="Local" or force==True:       
            cursor=self.connection.cursor()
            cursor.execute("DELETE FROM dbo_properties WHERE OBJname='"+objname+"' and PRPname='"+propname+"'")
            cursor.execute("ALTER TABLE "+objname+" DROP COLUMN "+propname)
            cursor.close()
            self.connection.commit()
            cursor=self.connection.cursor()
            cursor.execute("select OBJname from dbo_objects WHERE inherit='"+objname+"'")
            rec = cursor.fetchall()
            for r in rec:
                self.removeObjectProperty(r[0],propname,force=True)
            cursor.close()
            

    def removeObject(self,objname):
        cursor=self.connection.cursor()
        cursor.execute("DELETE FROM dbo_properties WHERE OBJname='"+objname+"'")
        cursor.execute("DELETE FROM dbo_objects WHERE OBJname='"+objname+"'")
        cursor.execute("DROP TABLE "+objname)
        cursor.close()
        self.connection.commit()

    def createLink(self,lnkname,oriname,dstname):
        cursor=self.connection.cursor()
        cursor.execute("INSERT INTO dbo_links VALUES('"+lnkname+"','"+oriname+"','"+dstname+"')")
        cursor.execute("CREATE TABLE "+lnkname+" (ORIID varchar(255),DSTID varchar(255))")
        cursor.close()
        self.connection.commit()

    def removeLink(self,lnkname):
        cursor=self.connection.cursor()
        cursor.execute("DELETE FROM dbo_links WHERE LNKname='"+lnkname+"'")
        cursor.execute("DROP TABLE "+lnkname)
        cursor.close()
        self.connection.commit()


    def getObjectDescription(self,objname):
        cursor=self.connection.cursor()
        cursor.execute("select PRPName,PRPtype,PRPsize,inherited,calculation from dbo_properties WHERE OBJname='"+objname+"'")
        records = cursor.fetchall()
        cursor.close()
        return(records)
    
    def getObjectPropertyDescription(self,objname,propname):
        cursor=self.connection.cursor()
        cursor.execute("select PRPName,PRPtype,PRPsize,inherited,calculation from dbo_properties WHERE OBJname='"+objname+"' and PRPname='"+propname+"'")
        record = cursor.fetchone()
        cursor.close()
        return(record)
        
    def getInstanceId(self,objname):
        cursor=self.connection.cursor()
        cursor.execute("select instance from dbo_objects WHERE OBJname='"+objname+"'")
        instance = cursor.fetchone()
        cursor.execute("update dbo_objects set instance="+str(instance[0]+1)+" WHERE OBJname='"+objname+"'")
        cursor.close()
        self.connection.commit()
        return(instance[0])

    def instanciateObject(self,objname):
        obj = DBObject(objname,None,self)
        return obj
    
    def retrieveObject(self,objname,id):
        obj = DBObject(objname,id,self)
        return obj
    
    def retriveListObjects(self,objname,filters):
        cursor=self.connection.cursor()
        cursor.execute("select ID from "+objname)
        records = cursor.fetchall()
        objlist=[]
        for r in records:
            objlist.append(self.retrieveObject(self,objname,r))
        cursor.close()
        return(objlist)





#    DBObject.py - implementation of the database Objects.
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


class DBObject:
    def __init__ (self,objname,id,model):
        self.model = model
        self.objname = objname
        self.id=id
        self.desc = {}
        if self.id==None:
            self.id=self.model.getInstanceId(self.objname)
            cursor=self.model.connection.cursor()
            query="INSERT INTO "+self.objname+" VALUES('"+str(self.id)+"'"
            for i in range(0,len(self.desc)-1):
                query=query+",NULL"
            query=query+")"
            cursor.execute(query)
            cursor.close()
            self.model.connection.commit()

        rec=self.model.getObjectDescription(objname)
        for r in rec:
            self.desc[r[0]]=r[1]

    def getObjectName(self):
        return(self.objname+"-"+str(self.id))

    def getProperty(self,propname):
        res=""
        rec = self.model.getObjectPropertyDescription(self.objname,propname)
        print(rec)
        if rec[4]!="None":
            cursor = self.model.connection.cursor()
            query=""
            qtmp = rec[4]
            i=0
            while i<len(qtmp):
                if qtmp[i]!='@':
                    query=query+qtmp[i]
                else:
                    token=""
                    j=0
                    while qtmp[i+1+j]!='@':
                        token=token+qtmp[i+1+j]
                        j+=1 
                    i=i+2+j
                    print(token)
                    cursor.execute("SELECT "+token+" FROM "+self.objname+" WHERE ID='"+str(self.id)+"'")
                    r = cursor.fetchone()[0]
                    query=query+"'"+r+"'"
                i+=1
            try:
                cursor.execute(query)
                res = cursor.fetchone()[0]
            except:
                res="ERROR"
            cursor.close()
        else:
            cursor = self.model.connection.cursor()
            cursor.execute("SELECT "+propname+" FROM "+self.objname+" WHERE ID='"+str(self.id)+"'")
            res = cursor.fetchone()[0]
            cursor.close()
        self.model.connection.commit()
        return(res)

    def setProperty(self,propname,val):
        rec = self.model.getObjectPropertyDescription(self.objname,propname)
        if rec[4]=="None":
            cursor = self.model.connection.cursor()
            cursor.execute("UPDATE "+self.objname+" SET "+propname+"='"+val+"' WHERE ID='"+str(self.id)+"'")
            cursor.close()
            self.model.connection.commit()

    def getObjectProperties(self):
        res=[]
        for f in self.desc:
            res.append((f,self.getProperty(f)))
        return(res)

    def getListProperties(self):
        return(self.model.getObjectProperties(self.objname))

    def linkObject(self,lnkname,dstid):
        cursor = self.model.connection.cursor()
        cursor.execute("INSERT into "+lnkname+" VALUES('"+str(self.id)+"','"+str(dstid)+"')")
        cursor.close()
        self.model.connection.commit()
    
    def unlinkObject(self,lnkname,dstid):
        cursor = self.model.connection.cursor()
        cursor.execute("DELETE FROM "+lnkname+" WHERE ORIID='"+str(self.id)+"' and DSTID='"+str(dstid)+"'")
        cursor.close()
        self.model.connection.commit()

    def getListLinked(self,lnkname):        
        cursor = self.model.connection.cursor()
        cursor.execute("SELECT dstname FROM dbo_links WHERE lnkname='"+lnkname+"'")
        objname = cursor.fetchone()[0]
        cursor.execute("SELECT DSTID FROM "+lnkname+" WHERE ORIID='"+str(self.id)+"'")
        records = cursor.fetchall()
        objlist=[]
        for r in records:
            objlist.append(self.model.retrieveObject(objname,r[0]))
        cursor.close()
        return(objlist)


import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class optimization(dml.Algorithm):
    contributor = 'Jinghang_Yuan'
    reads = ['Jinghang_Yuan.ZIPCounter']
    writes = ['Jinghang_Yuan.optimization']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('Jinghang_Yuan', 'Jinghang_Yuan')

        data = list(repo['Jinghang_Yuan.ZIPCounter'].find())

        # select and project all the zip with more or one center,policeStation,centerPool and school
        # the constraint is the four number should be larger than 0
        res=[]
        i=0
        for a,b,c,d,e,f,g in data:
            if(data[i][b]!="0None" and data[i][d]>0 and data[i][e]>0 and data[i][f]>0 and data[i][g]>0):
                res.append({"ZIP":data[i][b],'val_avg':data[i][c],'centerNum':data[i][d],'centerPoolNum':data[i][e],'policeStationNum': data[i][f], 'schoolNum':data[i][g]})
            i+=1
        # print(res)

        # define the metric function
        def metric(r):
            return r['val_avg']/r['centerNum']+r['centerPoolNum']+r['policeStationNum']+r['schoolNum']
        #solve optimization problem, to find the zip code with most public places and lowest average property value
        o = min(res, key=metric)
        print(o)

        repo.dropCollection("optimization")
        repo.createCollection("optimization")
        repo['Jinghang_Yuan.optimization'].insert_many([o])

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('Jinghang_Yuan', 'Jinghang_Yuan')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')


        this_script = doc.agent('alg:Jinghang_Yuan#center',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8 w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_center = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)



        doc.wasAssociatedWith(get_center, this_script)
        doc.usage(get_center, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': 'FID,OBJECTID,SITE,PHONE,FAX,STREET,NEIGH,ZIP'
                   }
                  )

        center = doc.entity('dat:Jinghang_Yuan#center',
                          {prov.model.PROV_LABEL: 'center', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(center, this_script)
        doc.wasGeneratedBy(center, get_center, endTime)
        doc.wasDerivedFrom(resource, center, get_center, get_center, get_center)

        repo.logout()

        return doc

optimization.execute()
#optimization.provenance()
# doc = optimization.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof

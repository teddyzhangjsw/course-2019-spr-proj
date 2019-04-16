import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np

from random import shuffle
from math import sqrt

class correlation(dml.Algorithm):
    contributor = 'Jinghang_Yuan'
    reads = ['Jinghang_Yuan.ZIPCounter']
    writes = []

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('Jinghang_Yuan', 'Jinghang_Yuan')

        data=repo['Jinghang_Yuan.ZIPCounter'].find()

        ave_val = []
        centerNum = []
        centerPoolNum = []
        policeStationNum = []
        schoolNum = []

        for i in data:
            ave_val += [i["val_avg"]]
            centerNum += [i["centerNum"]]
            centerPoolNum += [i["centerPoolNum"]]
            policeStationNum += [i["policeStationNum"]]
            schoolNum += [i["schoolNum"]]

        def permute(x):
            shuffled = [xi for xi in x]
            shuffle(shuffled)
            return shuffled

        def avg(x):  # Average
            return sum(x) / len(x)

        def stddev(x):  # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

        def cov(x, y):  # Covariance.
            return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

        def corr(x, y):  # Correlation coefficient.
            if stddev(x) * stddev(y) != 0:
                return cov(x, y) / (stddev(x) * stddev(y))

        print("avg_value vs centerNum")
        corr_avg_value_centerNum = corr(ave_val,centerNum)
        print(corr_avg_value_centerNum)
        print("----------")

        print("avg_value vs centerPoolNum")
        corr_avg_value_centerPoolNum = corr(ave_val, centerPoolNum)
        print(corr_avg_value_centerPoolNum)
        print("----------")

        print("avg_value vs policeStationNum")
        corr_avg_value_policeStationNum = corr(ave_val, policeStationNum)
        print(corr_avg_value_policeStationNum)
        print("----------")

        print("avg_value vs schoolNum")
        corr_avg_value_schoolNum = corr(ave_val, schoolNum)
        print(corr_avg_value_schoolNum)
        print("----------")

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

        this_script = doc.agent('alg:Jinghang_Yuan#school',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_school = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_school, this_script)
        doc.usage(get_school, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': 'X,Y,OBJECTID_1,OBJECTID,SCHID,NAME,ADDRESS,TOWN_MAIL,TOWN,STATE,ZIP,PRINCIPAL,PHONE,FAX,GRADES,TYPE'
                   }
                  )
        school = doc.entity('dat:Jinghang_Yuan#school',
                            {prov.model.PROV_LABEL: 'school', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(school, this_script)
        doc.wasGeneratedBy(school, get_school, endTime)
        doc.wasDerivedFrom(resource, school, get_school, get_school, get_school)

        repo.logout()

        return doc

correlation.execute()
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
cities  = dict()
sp = dict()
yrs = dict()
pin_code = dict()


search0 = es.search(index='a', body={"aggs":{"genres":{"terms":{"field":"city.keyword"}}}})
search_res0 = search0['aggregations']['genres']['buckets']

for i in search_res0:
    for key in i:
        cities[i['key']] = i['doc_count']

print (cities)
search1 = es.search(index='a', body={"aggs":{"genres":{"terms":{"field":"Speciality.keyword"}}}})
search_res1 = search1['aggregations']['genres']['buckets']
for i in search_res1:
    for key in i:
        sp[i['key']]=i['doc_count']
print (sp)

search2 = es.search(index='a', body={"aggs":{"genres":{"terms":{"field":"Years in practice.keyword"}}}})
search_res2 = search2['aggregations']['genres']['buckets']

for i in search_res2: 
    for key in i:
        yrs[i['key']] = i['doc_count']
print (yrs)





report = {
    'Doctor by cities': cities,
    'Doctors by specialist': sp,
    'Doctors by years of experiance':yrs,
    #'Doctors by Location pin code':pin_code
    }
print( report)


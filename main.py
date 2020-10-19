from pyspark import SparkContext, SparkConf
import json
import re
import gzip
import simplejson
import sys

print(sys.argv)

def parse(filename, total):
  IGNORE_FIELDS = ['Total items']
  f = gzip.open(filename, 'r')
  entry = {}
  categories = []
  reviews = []
  similar_items = []
  
  for line in f:
    line = line.strip().decode("UTF-8")
    colonPos = line.find(":")

    if line.startswith("Id"):
      if reviews:
        entry["reviews"] = reviews
      if categories:
        entry["categories"] = categories
      yield entry
      entry = {}
      categories = []
      reviews = []
      rest = line[colonPos+2:]
      entry["id"] = rest.strip()
      
    elif line.startswith("similar"):
      similar_items = line.split()[2:]
      entry['similar_items'] = similar_items

    # "cutomer" is typo of "customer" in original data
    elif line.find("cutomer:") != -1:
      review_info = line.split()
      reviews.append({'date': review_info[0],
                      'customer_id': review_info[2], 
                      'rating': int(review_info[4]), 
                      'votes': int(review_info[6]), 
                      'helpful': int(review_info[8])})

    elif line.startswith("|"):
      categories.append(line)

    elif colonPos != -1:
      eName = line[:colonPos]
      rest = line[colonPos+2:]

      if not eName in IGNORE_FIELDS:
        entry[eName] = rest.strip()

  if reviews:
    entry["reviews"] = reviews
  if categories:
    entry["categories"] = categories
    
  yield entry

def questionA(sc, path_file, ASIN):
    rdd = sc.textFile(path_file)

    rdd2 = rdd.map(lambda x : json.loads(x))

    rdd3 = rdd2.filter(lambda x : 'reviews' in x and 'ASIN' in x and x['ASIN'] == ASIN)

    rdd4 = rdd3.map(lambda x : x['reviews'])

    rdd5 = sc.parallelize(rdd4.collect()[0])

    rdd6 = rdd5.sortBy( lambda x : x['helpful'], ascending=False).top(10, lambda x: x['helpful'])

    print("\n##MAIS UTEIS MENOR RATING##")

    rdd7 = sc.parallelize(rdd6).sortBy( lambda x : x['rating']).top(5, lambda x: x['rating'])
    print(rdd7)

    print("\n##MAIS UTEIS MAIOR RATING##")

    rdd8 = sc.parallelize(rdd6).sortBy( lambda x : x['rating'], ascending=False).top(5, lambda x: x['rating'])
    print(rdd8)

def questionB(sc, path_file, ASIN):
    rdd = sc.textFile(path_file)

    rdd2 = rdd.map(lambda x : json.loads(x))

    rdd3 = rdd2.filter(lambda x: 'ASIN' in x and 'salesrank' in x and 'similar_items' in x)


    rdd3.cache()

    similarsRdd = rdd3.filter(lambda x : x['ASIN'] == ASIN).flatMap(lambda x: [(similar, x['salesrank']) for similar in x['similar_items']])

    #print( similarsRdd.collect())

    itemsRdd = rdd3.map( lambda x: (x['ASIN'], x['salesrank'])) 

    #print( itemsRdd.collect())

    joinedRdd = similarsRdd.join(itemsRdd)

    filteredRdd = joinedRdd.filter( lambda x : int(x[1][0]) > int(x[1][1]))

    print(filteredRdd.collect())

def questionC(sc, path_file, ASIN):
    rdd = sc.textFile(path_file)

    rdd2 = rdd.map(lambda x : json.loads(x))

    rdd3 = rdd2.filter(lambda x: 'ASIN' in x and 'reviews' in x and x['ASIN'] == ASIN)

    reviewsRdd = rdd3.map(lambda x : (x['ASIN'], x['reviews']))

    newRdd = reviewsRdd.flatMap(lambda x: [(item['date'], item['rating']) for item in x[1]])

    print( newRdd.collect())

def questionD(sc, path_file):
    rdd = sc.textFile(path_file)

    rdd2 = rdd.map(lambda x : json.loads(x))

    rdd3 = rdd2.filter(lambda x: 'ASIN' in x and 'salesrank' in x and 'categories' in x)

    rdd4 = rdd3.map(lambda x : (x['ASIN'], x['categories']))

    rdd5 = rdd4.flatMap( lambda x : [(x[0], item) for item in x[1]])

    rdd6 = rdd5.groupBy(lambda x: x[1])

    rdd7 = rdd6.map(lambda x: (x[0], list(x[1])))

    print(rdd7.collect())

def questionE(sc, path_file):
    rdd = sc.textFile(path_file)

    rdd2 = rdd.map(lambda x : json.loads(x))

    rdd3 = rdd2.filter(lambda x: 'ASIN' in x and 'group' in x)

    rdd4 = rdd3.map(lambda x : (x['ASIN'], x['group']))

    rdd5 = rdd4.groupBy(lambda x: x[1]).map(lambda x : (x[0], list(x[1])))

    print(rdd5.collect())

def questionF(sc, path_file):
    pass

def questionG(sc, path_file):
    pass

path = "output.txt"
f = open(path, "w")
file_path = sys.argv[1]

line_num = 1000

for e in parse(file_path, total=line_num):
    if e:
        f.write(simplejson.dumps(e) + '\n')

f.close()

conf = SparkConf().setAppName("trab-bd").setMaster("local")
    
sc = SparkContext(conf=conf)

questionA(sc, path, "0231118597")







find sp_diabetes identify all ICD-9 associated with them and find highest frequency
1> reproduce sp_diabetics algorithm to verify navigating dataset correctly
pat:125k out of 344k are coded as sp_diabetes '1'!
ip:two places: ADMTNG_ICD9_DGNS_CD or ICD9_DGNS_CD (array)
import pymongo
pat = pymongoMongoClient('mongodb://localhost',27017)synPUFpatient
carrier = pymongoMongoClient('mongodb://localhost',27017)synPUFcarrier
ip = pymongoMongoClient('mongodb://localhost',27017)synPUFip
op = pymongoMongoClient('mongodb://localhost',27017)synPUFop
pde = pymongoMongoClient('mongodb://localhost',27017)synPUFpde
>>> for p in ipfind({u'ICD9_DGNS_CD':u'24900'})limit(5):  p

for p in ipfind({u'ICD9_DGNS_CD':u'24900',u'ICD9_DGNS_CD':u'24901'},{u'ICD9_DGNS_CD':1})limit(5):  p
???only 2 conditions allowed???

db.ip.ensureIndex({ICD9_DGNS_CD:1})
db.op.ensureIndex({ICD9_DGNS_CD:1})
db.carrier.ensureIndex({ICD9_DGNS_CD:1})

import pymongo
pat = pymongo.MongoClient('mongodb://localhost',27017).synPUF.patient
carrier = pymongo.MongoClient('mongodb://localhost',27017).synPUF.carrier
ip = pymongo.MongoClient('mongodb://localhost',27017).synPUF.ip
op = pymongo.MongoClient('mongodb://localhost',27017).synPUF.op
pde = pymongo.MongoClient('mongodb://localhost',27017).synPUF.pde
aggr = pymongo.MongoClient('mongodb://localhost',27017).synPUF.aggr
aggr_ip = pymongo.MongoClient('mongodb://localhost',27017).synPUF.aggr_ip
aggr_op = pymongo.MongoClient('mongodb://localhost',27017).synPUF.aggr_op
aggr_ca = pymongo.MongoClient('mongodb://localhost',27017).synPUF.aggr_ca
icd9_diabetes = [ \
    '24900', '24901', '24910', '24911', '24920', '24921',\
    '24930', '24931', '24940', '24941', '24950', '24951', '24960', \
    '24961', '24970', '24971', '24980', '24981', '24990', '24991', \
    '25000', '25001', '25002', '25003', '25010', '25011', '25012', '25013', \
    '25020', '25021', '25022', '25023', '25030', '25031', '25032', '25033', \
    '25040', '25041', '25042', '25043', '25050', '25051', '25052', '25053', \
    '25060', '25061', '25062', '25063', '25070', '25071', '25072', '25073', \
    '25080', '25081', '25082', '25083', '25090', '25091', '25092', '25093', \
    '3572',  '36201', '36202', '36641']

def calc_diabetes1():
  icd9_diabetes_freq_ip = {'_id':'icd9_diabetes_freq_ip'}
  icd9_diabetes_freq_op = {'_id':'icd9_diabetes_freq_op'}
  icd9_diabetes_freq_ca = {'_id':'icd9_diabetes_freq_ca'}
  
  for i in icd9_diabetes:
    icd9_diabetes_freq_ip[i] = ip.find({'ICD9_DGNS_CD': i}).count()
    icd9_diabetes_freq_op[i] = op.find({'ICD9_DGNS_CD': i}).count()
    icd9_diabetes_freq_ca[i] = carrier.find({'ICD9_DGNS_CD': i}).count()
  
  object_id = aggr.save(icd9_diabetes_freq_ip)
  object_id = aggr.save(icd9_diabetes_freq_op)
  object_id = aggr.save(icd9_diabetes_freq_ca)

def flatten_icd9():
  # build up matrix by ID with code, type, year(thruDate)
  for i in ip.find({},{'_id':0,'DESYNPUF_ID':1,'ICD9_DGNS_CD':1,'CLM_THRU_DT':1}):
    for j in i['ICD9_DGNS_CD']:
      result = aggr_ip.update({'DESYNPUF_ID':i['DESYNPUF_ID'], 'ICD9_DGNS_CD':j, \
          'YEAR':i['CLM_THRU_DT'][:4]},{'$inc':{'claim_count':1}},upsert=True)
  for i in op.find({},{'_id':0,'DESYNPUF_ID':1,'ICD9_DGNS_CD':1,'CLM_THRU_DT':1}):
    for j in i['ICD9_DGNS_CD']:
      result = aggr_op.update({'DESYNPUF_ID':i['DESYNPUF_ID'], 'ICD9_DGNS_CD':j, \
          'YEAR':i['CLM_THRU_DT'][:4]},{'$inc':{'claim_count':1}},upsert=True)
  for i in carrier.find({},{'_id':0,'DESYNPUF_ID':1,'ICD9_DGNS_CD':1,'CLM_THRU_DT':1}):
    for j in i['ICD9_DGNS_CD']:
      result = aggr_ca.update({'DESYNPUF_ID':i['DESYNPUF_ID'], 'ICD9_DGNS_CD':j, \
          'YEAR':i['CLM_THRU_DT'][:4]},{'$inc':{'claim_count':1}},upsert=True)



weight frequency by cost

adjust for where the ICD-9 occurs eg by position, inpatient over outpatient

find out variation in cost for same profile based on similarity of ICD-9 (adjust for age, state, county, )

association with DRG codes

From synPUF_CodebookPDF
BEN-19
Label: DESYNPUF: Chronic Condition: Diabetes Variable Name: SP_DIABETES
Type: Num
Categories
Summary Statistics
■ Range: [1, 2]
■ Unique values: 2
■ Missing: 0
■ Valid: 2,326,856 in 2008; 2,291,320 in 2009; 2,255,098 in 2010
Source
■ File: 2008 – 2010 DE-SynPUF: inpatient, outpatient, and Carrier files
■ Coding scheme: Please see disease algorithm in the Appendix 1

Disease Algorithm: Diabetes
Reference time period: 1 year
Valid ICD-9/CPT4/HCPCS Code: DX 24900, 24901, 24910, 24911, 24920, 24921,
  24930, 24931, 24940, 24941, 24950, 24951, 24960, 24961, 24970, 24971,
  24980, 24981, 24990, 24991,
  25000, 25001, 25002, 25003, 25010, 25011, 25012, 25013,
  25020, 25021, 25022, 25023, 25030, 25031, 25032, 25033,
  25040, 25041, 25042, 25043, 25050, 25051, 25052, 25053,
  25060, 25061, 25062, 25063, 25070, 25071, 25072, 25073,
  25080, 25081, 25082, 25083, 25090, 25091, 25092, 25093,
  3572, 36201, 36202, 36641 (any DX on the claim)
Number/Type of claims to qualify: At least 1 inpatient or 2 HOP or Carrier claim
    with DX codes during the yearly period

Miscodings for breast cancer (Gender Male=1, however codes in Appendix 1 is for Female only)

Which type of cancer treatment is more expensive (mean and variance)


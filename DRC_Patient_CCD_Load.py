#!/usr/bin/python
# Program to take demographics data from drc CCD and load into mongodb
# Copyright 2013 Raj N Manickam
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

import xmltodict
import simplejson as json
import pymongo
import sys
import csv
import glob

## Load patient demo
def load_patient():
  pat = pymongo.MongoClient('mongodb://localhost',27017).drc.patient
  print "Starting count is " + str(pat.count())
  pat_file = file("/Users/rm/drctrial/patient_demographics.csv", "rU")
  reader = csv.DictReader( pat_file, fieldnames = ("_id","Suffix","PatientID","ProblemList", \
    "MedicationList","AllergyList","DrugInteractions","LastName","FirstName","FullName", \
    "NickName","HomePhone","WorkPhone","WorkPhoneExtension","CellPhone","Email", \
    "StreetAddress","City","State","Zip","DateofBirth","LastVisitDate","Gender","SSN", \
    "SignOnFile","InsuranceCompany","InsuranceID","InsuranceGroupID", \
    "InsuranceSubscriberName","InsuranceSubscriberDOB","InsuranceSubscriberSSN", \
    "SecondaryInsurance","SecondaryInsuranceID","Secondary Insurance Group Number", \
    "Secondary Insurance Subscriber Name","Secondary Insurance Subscriber Date of Birth", \
    "SecondaryInsuranceSubscriberSSN","Doctor","DateCreatedAsSortableNumber"))
  for row in reader:
    pat_object_id = pat.save(row)
  print "Ending count is " + str(pat.count())


## Load CCD XML files from a sub-directory
def load_ccd():
  ccd = pymongo.MongoClient('mongodb://localhost',27017).drc.ccd
  print "Starting count is " + str(ccd.count())
  for xml_file in glob.glob("/Users/rm/drctrial/ccd/*.xml"):
    input_file = file(xml_file, "rU")
    ccd_data = json.loads(json.dumps(xmltodict.parse(input_file.read())))
    ccd_data['_id'] = \
      ccd_data['ClinicalDocument']['recordTarget']['patientRole']['id']['@extension'] + \
      ':' + ccd_data['ClinicalDocument']['effectiveTime']['@value']
    ccd_object_id = ccd.save(ccd_data)
  print "Ending count is " + str(ccd.count())

## TODO convert timestamp into UTC so can be used in any timezone
## TODO consider all new for now; need to handle updates if already present
## TODO Next Steps - Sample MongoDB query:
##      Identify all patients 45 to 65 years old; count how many of them have DM (based on 3-digit ICD9 codes)

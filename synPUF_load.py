#!/usr/bin/python
# Copyright 2013 Raj N Manickam
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0
# Program to take beneficiary data from Medicare and load into mongodb
# Why mongodb?  Because it can store and retrieve JSON documents
# Why JSON?  This dataset is in multiple files (7), with multiple horizontal
#   partitions (20), with multiple multi-valued fields (e.g. ICD-9 8x)
#   Saw need for hierarchical, sparse, multi-valued, arbitrary depth - JSON!

import xmltodict
import simplejson as json
import pymongo
import sys
import csv
import glob

## Load beneficiary (patient)
def load_patient():
  pat = pymongo.MongoClient('mongodb://localhost',27017).synPUF.patient
  print "Starting count is " + str(pat.count())
  for file_year in ('2008', '2009', '2010'):
    file_name = '/Users/rm/Downloads/synPUF/DE1_0_' + file_year + '_Beneficiary_Summary_File_Sample_1.csv'
    pat_file = file(file_name, "rU")
    reader = csv.DictReader( pat_file, fieldnames = ("DESYNPUF_ID","BENE_BIRTH_DT",\
      "BENE_DEATH_DT","BENE_SEX_IDENT_CD","BENE_RACE_CD","BENE_ESRD_IND","SP_STATE_CODE",\
      "BENE_COUNTY_CD","BENE_HI_CVRAGE_TOT_MONS","BENE_SMI_CVRAGE_TOT_MONS",\
      "BENE_HMO_CVRAGE_TOT_MONS","PLAN_CVRG_MOS_NUM","SP_ALZHDMTA","SP_CHF","SP_CHRNKIDN",\
      "SP_CNCR","SP_COPD","SP_DEPRESSN","SP_DIABETES","SP_ISCHMCHT","SP_OSTEOPRS","SP_RA_OA",\
      "SP_STRKETIA","MEDREIMB_IP","BENRES_IP","PPPYMT_IP","MEDREIMB_OP","BENRES_OP",\
      "PPPYMT_OP","MEDREIMB_CAR","BENRES_CAR","PPPYMT_CAR"))
    
    for line in reader:
      if line['DESYNPUF_ID'] <> 'DESYNPUF_ID':  # ignore header line
        line['_id'] = line['DESYNPUF_ID'] + '_' + file_year
        line['YEAR'] = file_year
        object_id = pat.save(line)
    print "Ending count is " + str(pat.count())

## Load Carrier Data
#  for file_name in ('/Users/rm/Downloads/synPUF/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv', \
#                    '/Users/rm/Downloads/synPUF/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv'):
  for file_name in ('/Users/rm/Downloads/synPUF/DE1_0_CAR.csv'):

def load_carrier(file_name):
  carrier = pymongo.MongoClient('mongodb://localhost',27017).synPUF.carrier
  print "Starting count is " + str(carrier.count())
  carrier_file = file(file_name, "rU")
  reader = csv.DictReader( carrier_file, fieldnames = ("DESYNPUF_ID","CLM_ID",\
    "CLM_FROM_DT","CLM_THRU_DT","ICD9_DGNS_CD_1","ICD9_DGNS_CD_2","ICD9_DGNS_CD_3",\
    "ICD9_DGNS_CD_4","ICD9_DGNS_CD_5","ICD9_DGNS_CD_6","ICD9_DGNS_CD_7","ICD9_DGNS_CD_8",\
    "PRF_PHYSN_NPI_1","PRF_PHYSN_NPI_2","PRF_PHYSN_NPI_3","PRF_PHYSN_NPI_4",\
    "PRF_PHYSN_NPI_5","PRF_PHYSN_NPI_6","PRF_PHYSN_NPI_7","PRF_PHYSN_NPI_8",\
    "PRF_PHYSN_NPI_9","PRF_PHYSN_NPI_10","PRF_PHYSN_NPI_11","PRF_PHYSN_NPI_12",\
    "PRF_PHYSN_NPI_13","TAX_NUM_1","TAX_NUM_2","TAX_NUM_3","TAX_NUM_4","TAX_NUM_5",\
    "TAX_NUM_6","TAX_NUM_7","TAX_NUM_8","TAX_NUM_9","TAX_NUM_10","TAX_NUM_11",\
    "TAX_NUM_12","TAX_NUM_13","HCPCS_CD_1","HCPCS_CD_2","HCPCS_CD_3","HCPCS_CD_4",\
    "HCPCS_CD_5","HCPCS_CD_6","HCPCS_CD_7","HCPCS_CD_8","HCPCS_CD_9","HCPCS_CD_10",\
    "HCPCS_CD_11","HCPCS_CD_12","HCPCS_CD_13","LINE_NCH_PMT_AMT_1","LINE_NCH_PMT_AMT_2",\
    "LINE_NCH_PMT_AMT_3","LINE_NCH_PMT_AMT_4","LINE_NCH_PMT_AMT_5","LINE_NCH_PMT_AMT_6",\
    "LINE_NCH_PMT_AMT_7","LINE_NCH_PMT_AMT_8","LINE_NCH_PMT_AMT_9",\
    "LINE_NCH_PMT_AMT_10","LINE_NCH_PMT_AMT_11","LINE_NCH_PMT_AMT_12",\
    "LINE_NCH_PMT_AMT_13","LINE_BENE_PTB_DDCTBL_AMT_1","LINE_BENE_PTB_DDCTBL_AMT_2",\
    "LINE_BENE_PTB_DDCTBL_AMT_3","LINE_BENE_PTB_DDCTBL_AMT_4","LINE_BENE_PTB_DDCTBL_AMT_5",\
    "LINE_BENE_PTB_DDCTBL_AMT_6","LINE_BENE_PTB_DDCTBL_AMT_7","LINE_BENE_PTB_DDCTBL_AMT_8",\
    "LINE_BENE_PTB_DDCTBL_AMT_9","LINE_BENE_PTB_DDCTBL_AMT_10","LINE_BENE_PTB_DDCTBL_AMT_11",\
    "LINE_BENE_PTB_DDCTBL_AMT_12","LINE_BENE_PTB_DDCTBL_AMT_13","LINE_BENE_PRMRY_PYR_PD_AMT_1",\
    "LINE_BENE_PRMRY_PYR_PD_AMT_2","LINE_BENE_PRMRY_PYR_PD_AMT_3","LINE_BENE_PRMRY_PYR_PD_AMT_4",\
    "LINE_BENE_PRMRY_PYR_PD_AMT_5","LINE_BENE_PRMRY_PYR_PD_AMT_6","LINE_BENE_PRMRY_PYR_PD_AMT_7",\
    "LINE_BENE_PRMRY_PYR_PD_AMT_8","LINE_BENE_PRMRY_PYR_PD_AMT_9","LINE_BENE_PRMRY_PYR_PD_AMT_10",\
    "LINE_BENE_PRMRY_PYR_PD_AMT_11","LINE_BENE_PRMRY_PYR_PD_AMT_12","LINE_BENE_PRMRY_PYR_PD_AMT_13",\
    "LINE_COINSRNC_AMT_1","LINE_COINSRNC_AMT_2","LINE_COINSRNC_AMT_3","LINE_COINSRNC_AMT_4",\
    "LINE_COINSRNC_AMT_5","LINE_COINSRNC_AMT_6","LINE_COINSRNC_AMT_7","LINE_COINSRNC_AMT_8",\
    "LINE_COINSRNC_AMT_9","LINE_COINSRNC_AMT_10","LINE_COINSRNC_AMT_11","LINE_COINSRNC_AMT_12",\
    "LINE_COINSRNC_AMT_13","LINE_ALOWD_CHRG_AMT_1","LINE_ALOWD_CHRG_AMT_2","LINE_ALOWD_CHRG_AMT_3",\
    "LINE_ALOWD_CHRG_AMT_4","LINE_ALOWD_CHRG_AMT_5","LINE_ALOWD_CHRG_AMT_6","LINE_ALOWD_CHRG_AMT_7",\
    "LINE_ALOWD_CHRG_AMT_8","LINE_ALOWD_CHRG_AMT_9","LINE_ALOWD_CHRG_AMT_10",\
    "LINE_ALOWD_CHRG_AMT_11","LINE_ALOWD_CHRG_AMT_12","LINE_ALOWD_CHRG_AMT_13",\
    "LINE_PRCSG_IND_CD_1","LINE_PRCSG_IND_CD_2","LINE_PRCSG_IND_CD_3","LINE_PRCSG_IND_CD_4",\
    "LINE_PRCSG_IND_CD_5","LINE_PRCSG_IND_CD_6","LINE_PRCSG_IND_CD_7","LINE_PRCSG_IND_CD_8",\
    "LINE_PRCSG_IND_CD_9","LINE_PRCSG_IND_CD_10","LINE_PRCSG_IND_CD_11","LINE_PRCSG_IND_CD_12",\
    "LINE_PRCSG_IND_CD_13","LINE_ICD9_DGNS_CD_1","LINE_ICD9_DGNS_CD_2","LINE_ICD9_DGNS_CD_3",\
    "LINE_ICD9_DGNS_CD_4","LINE_ICD9_DGNS_CD_5","LINE_ICD9_DGNS_CD_6","LINE_ICD9_DGNS_CD_7",\
    "LINE_ICD9_DGNS_CD_8","LINE_ICD9_DGNS_CD_9","LINE_ICD9_DGNS_CD_10","LINE_ICD9_DGNS_CD_11",\
    "LINE_ICD9_DGNS_CD_12","LINE_ICD9_DGNS_CD_13"))
    
  for line in reader:
      if line['DESYNPUF_ID'] <> 'DESYNPUF_ID':  # ignore header line
        line['_id'] = line['DESYNPUF_ID'] + '_' + line['CLM_ID']
        ICD9_DGNS_CD = []
        if line['ICD9_DGNS_CD_1']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_1'])
        if line['ICD9_DGNS_CD_2']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_2'])
        if line['ICD9_DGNS_CD_3']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_3'])
        if line['ICD9_DGNS_CD_4']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_4'])
        if line['ICD9_DGNS_CD_5']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_5'])
        if line['ICD9_DGNS_CD_6']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_6'])
        if line['ICD9_DGNS_CD_7']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_7'])
        if line['ICD9_DGNS_CD_8']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_8'])
        line['ICD9_DGNS_CD'] = ICD9_DGNS_CD
        del line['ICD9_DGNS_CD_1']
        del line['ICD9_DGNS_CD_2']
        del line['ICD9_DGNS_CD_3']
        del line['ICD9_DGNS_CD_4']
        del line['ICD9_DGNS_CD_5']
        del line['ICD9_DGNS_CD_6']
        del line['ICD9_DGNS_CD_7']
        del line['ICD9_DGNS_CD_8']
        PRF_PHYSN_NPI = []
        if line['PRF_PHYSN_NPI_1']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_1'])
        if line['PRF_PHYSN_NPI_2']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_2'])
        if line['PRF_PHYSN_NPI_3']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_3'])
        if line['PRF_PHYSN_NPI_4']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_4'])
        if line['PRF_PHYSN_NPI_5']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_5'])
        if line['PRF_PHYSN_NPI_6']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_6'])
        if line['PRF_PHYSN_NPI_7']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_7'])
        if line['PRF_PHYSN_NPI_8']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_8'])
        if line['PRF_PHYSN_NPI_9']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_9'])
        if line['PRF_PHYSN_NPI_10']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_10'])
        if line['PRF_PHYSN_NPI_11']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_11'])
        if line['PRF_PHYSN_NPI_12']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_12'])
        if line['PRF_PHYSN_NPI_13']:
          PRF_PHYSN_NPI.append(line['PRF_PHYSN_NPI_13'])
        line['PRF_PHYSN_NPI'] = PRF_PHYSN_NPI
        del line['PRF_PHYSN_NPI_1']
        del line['PRF_PHYSN_NPI_2']
        del line['PRF_PHYSN_NPI_3']
        del line['PRF_PHYSN_NPI_4']
        del line['PRF_PHYSN_NPI_5']
        del line['PRF_PHYSN_NPI_6']
        del line['PRF_PHYSN_NPI_7']
        del line['PRF_PHYSN_NPI_8']
        del line['PRF_PHYSN_NPI_9']
        del line['PRF_PHYSN_NPI_10']
        del line['PRF_PHYSN_NPI_11']
        del line['PRF_PHYSN_NPI_12']
        del line['PRF_PHYSN_NPI_13']
        TAX_NUM = []
        if line['TAX_NUM_1']:
          TAX_NUM.append(line['TAX_NUM_1'])
        if line['TAX_NUM_2']:
          TAX_NUM.append(line['TAX_NUM_2'])
        if line['TAX_NUM_3']:
          TAX_NUM.append(line['TAX_NUM_3'])
        if line['TAX_NUM_4']:
          TAX_NUM.append(line['TAX_NUM_4'])
        if line['TAX_NUM_5']:
          TAX_NUM.append(line['TAX_NUM_5'])
        if line['TAX_NUM_6']:
          TAX_NUM.append(line['TAX_NUM_6'])
        if line['TAX_NUM_7']:
          TAX_NUM.append(line['TAX_NUM_7'])
        if line['TAX_NUM_8']:
          TAX_NUM.append(line['TAX_NUM_8'])
        if line['TAX_NUM_9']:
          TAX_NUM.append(line['TAX_NUM_9'])
        if line['TAX_NUM_10']:
          TAX_NUM.append(line['TAX_NUM_10'])
        if line['TAX_NUM_11']:
          TAX_NUM.append(line['TAX_NUM_11'])
        if line['TAX_NUM_12']:
          TAX_NUM.append(line['TAX_NUM_12'])
        if line['TAX_NUM_13']:
          TAX_NUM.append(line['TAX_NUM_13'])
        line['TAX_NUM'] = TAX_NUM
        del line['TAX_NUM_1']
        del line['TAX_NUM_2']
        del line['TAX_NUM_3']
        del line['TAX_NUM_4']
        del line['TAX_NUM_5']
        del line['TAX_NUM_6']
        del line['TAX_NUM_7']
        del line['TAX_NUM_8']
        del line['TAX_NUM_9']
        del line['TAX_NUM_10']
        del line['TAX_NUM_11']
        del line['TAX_NUM_12']
        del line['TAX_NUM_13']
        HCPCS_CD = []
        if line['HCPCS_CD_1']:
          HCPCS_CD.append(line['HCPCS_CD_1'])
        if line['HCPCS_CD_2']:
          HCPCS_CD.append(line['HCPCS_CD_2'])
        if line['HCPCS_CD_3']:
          HCPCS_CD.append(line['HCPCS_CD_3'])
        if line['HCPCS_CD_4']:
          HCPCS_CD.append(line['HCPCS_CD_4'])
        if line['HCPCS_CD_5']:
          HCPCS_CD.append(line['HCPCS_CD_5'])
        if line['HCPCS_CD_6']:
          HCPCS_CD.append(line['HCPCS_CD_6'])
        if line['HCPCS_CD_7']:
          HCPCS_CD.append(line['HCPCS_CD_7'])
        if line['HCPCS_CD_8']:
          HCPCS_CD.append(line['HCPCS_CD_8'])
        if line['HCPCS_CD_9']:
          HCPCS_CD.append(line['HCPCS_CD_9'])
        if line['HCPCS_CD_10']:
          HCPCS_CD.append(line['HCPCS_CD_10'])
        if line['HCPCS_CD_11']:
          HCPCS_CD.append(line['HCPCS_CD_11'])
        if line['HCPCS_CD_12']:
          HCPCS_CD.append(line['HCPCS_CD_12'])
        if line['HCPCS_CD_13']:
          HCPCS_CD.append(line['HCPCS_CD_13'])
        line['HCPCS_CD'] = HCPCS_CD
        del line['HCPCS_CD_1']
        del line['HCPCS_CD_2']
        del line['HCPCS_CD_3']
        del line['HCPCS_CD_4']
        del line['HCPCS_CD_5']
        del line['HCPCS_CD_6']
        del line['HCPCS_CD_7']
        del line['HCPCS_CD_8']
        del line['HCPCS_CD_9']
        del line['HCPCS_CD_10']
        del line['HCPCS_CD_11']
        del line['HCPCS_CD_12']
        del line['HCPCS_CD_13']
        LINE_NCH_PMT_AMT = []
        if line['LINE_NCH_PMT_AMT_1']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_1'])
        if line['LINE_NCH_PMT_AMT_2']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_2'])
        if line['LINE_NCH_PMT_AMT_3']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_3'])
        if line['LINE_NCH_PMT_AMT_4']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_4'])
        if line['LINE_NCH_PMT_AMT_5']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_5'])
        if line['LINE_NCH_PMT_AMT_6']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_6'])
        if line['LINE_NCH_PMT_AMT_7']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_7'])
        if line['LINE_NCH_PMT_AMT_8']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_8'])
        if line['LINE_NCH_PMT_AMT_9']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_9'])
        if line['LINE_NCH_PMT_AMT_10']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_10'])
        if line['LINE_NCH_PMT_AMT_11']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_11'])
        if line['LINE_NCH_PMT_AMT_12']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_12'])
        if line['LINE_NCH_PMT_AMT_13']:
          LINE_NCH_PMT_AMT.append(line['LINE_NCH_PMT_AMT_13'])
        line['LINE_NCH_PMT_AMT'] = LINE_NCH_PMT_AMT
        del line['LINE_NCH_PMT_AMT_1']
        del line['LINE_NCH_PMT_AMT_2']
        del line['LINE_NCH_PMT_AMT_3']
        del line['LINE_NCH_PMT_AMT_4']
        del line['LINE_NCH_PMT_AMT_5']
        del line['LINE_NCH_PMT_AMT_6']
        del line['LINE_NCH_PMT_AMT_7']
        del line['LINE_NCH_PMT_AMT_8']
        del line['LINE_NCH_PMT_AMT_9']
        del line['LINE_NCH_PMT_AMT_10']
        del line['LINE_NCH_PMT_AMT_11']
        del line['LINE_NCH_PMT_AMT_12']
        del line['LINE_NCH_PMT_AMT_13']
        LINE_BENE_PTB_DDCTBL_AMT = []
        if line['LINE_BENE_PTB_DDCTBL_AMT_1']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_1'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_2']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_2'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_3']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_3'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_4']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_4'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_5']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_5'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_6']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_6'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_7']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_7'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_8']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_8'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_9']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_9'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_10']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_10'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_11']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_11'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_12']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_12'])
        if line['LINE_BENE_PTB_DDCTBL_AMT_13']:
          LINE_BENE_PTB_DDCTBL_AMT.append(line['LINE_BENE_PTB_DDCTBL_AMT_13'])
        line['LINE_BENE_PTB_DDCTBL_AMT'] = LINE_BENE_PTB_DDCTBL_AMT
        del line['LINE_BENE_PTB_DDCTBL_AMT_1']
        del line['LINE_BENE_PTB_DDCTBL_AMT_2']
        del line['LINE_BENE_PTB_DDCTBL_AMT_3']
        del line['LINE_BENE_PTB_DDCTBL_AMT_4']
        del line['LINE_BENE_PTB_DDCTBL_AMT_5']
        del line['LINE_BENE_PTB_DDCTBL_AMT_6']
        del line['LINE_BENE_PTB_DDCTBL_AMT_7']
        del line['LINE_BENE_PTB_DDCTBL_AMT_8']
        del line['LINE_BENE_PTB_DDCTBL_AMT_9']
        del line['LINE_BENE_PTB_DDCTBL_AMT_10']
        del line['LINE_BENE_PTB_DDCTBL_AMT_11']
        del line['LINE_BENE_PTB_DDCTBL_AMT_12']
        del line['LINE_BENE_PTB_DDCTBL_AMT_13']
        LINE_BENE_PRMRY_PYR_PD_AMT = []
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_1']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_1'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_2']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_2'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_3']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_3'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_4']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_4'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_5']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_5'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_6']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_6'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_7']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_7'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_8']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_8'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_9']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_9'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_10']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_10'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_11']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_11'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_12']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_12'])
        if line['LINE_BENE_PRMRY_PYR_PD_AMT_13']:
          LINE_BENE_PRMRY_PYR_PD_AMT.append(line['LINE_BENE_PRMRY_PYR_PD_AMT_13'])
        line['LINE_BENE_PRMRY_PYR_PD_AMT'] = LINE_BENE_PRMRY_PYR_PD_AMT
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_1']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_2']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_3']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_4']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_5']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_6']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_7']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_8']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_9']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_10']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_11']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_12']
        del line['LINE_BENE_PRMRY_PYR_PD_AMT_13']
        LINE_COINSRNC_AMT = []
        if line['LINE_COINSRNC_AMT_1']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_1'])
        if line['LINE_COINSRNC_AMT_2']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_2'])
        if line['LINE_COINSRNC_AMT_3']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_3'])
        if line['LINE_COINSRNC_AMT_4']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_4'])
        if line['LINE_COINSRNC_AMT_5']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_5'])
        if line['LINE_COINSRNC_AMT_6']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_6'])
        if line['LINE_COINSRNC_AMT_7']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_7'])
        if line['LINE_COINSRNC_AMT_8']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_8'])
        if line['LINE_COINSRNC_AMT_9']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_9'])
        if line['LINE_COINSRNC_AMT_10']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_10'])
        if line['LINE_COINSRNC_AMT_11']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_11'])
        if line['LINE_COINSRNC_AMT_12']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_12'])
        if line['LINE_COINSRNC_AMT_13']:
          LINE_COINSRNC_AMT.append(line['LINE_COINSRNC_AMT_13'])
        line['LINE_COINSRNC_AMT'] = LINE_COINSRNC_AMT
        del line['LINE_COINSRNC_AMT_1']
        del line['LINE_COINSRNC_AMT_2']
        del line['LINE_COINSRNC_AMT_3']
        del line['LINE_COINSRNC_AMT_4']
        del line['LINE_COINSRNC_AMT_5']
        del line['LINE_COINSRNC_AMT_6']
        del line['LINE_COINSRNC_AMT_7']
        del line['LINE_COINSRNC_AMT_8']
        del line['LINE_COINSRNC_AMT_9']
        del line['LINE_COINSRNC_AMT_10']
        del line['LINE_COINSRNC_AMT_11']
        del line['LINE_COINSRNC_AMT_12']
        del line['LINE_COINSRNC_AMT_13']
        LINE_ALOWD_CHRG_AMT = []
        if line['LINE_ALOWD_CHRG_AMT_1']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_1'])
        if line['LINE_ALOWD_CHRG_AMT_2']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_2'])
        if line['LINE_ALOWD_CHRG_AMT_3']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_3'])
        if line['LINE_ALOWD_CHRG_AMT_4']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_4'])
        if line['LINE_ALOWD_CHRG_AMT_5']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_5'])
        if line['LINE_ALOWD_CHRG_AMT_6']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_6'])
        if line['LINE_ALOWD_CHRG_AMT_7']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_7'])
        if line['LINE_ALOWD_CHRG_AMT_8']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_8'])
        if line['LINE_ALOWD_CHRG_AMT_9']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_9'])
        if line['LINE_ALOWD_CHRG_AMT_10']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_10'])
        if line['LINE_ALOWD_CHRG_AMT_11']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_11'])
        if line['LINE_ALOWD_CHRG_AMT_12']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_12'])
        if line['LINE_ALOWD_CHRG_AMT_13']:
          LINE_ALOWD_CHRG_AMT.append(line['LINE_ALOWD_CHRG_AMT_13'])
        line['LINE_ALOWD_CHRG_AMT'] = LINE_ALOWD_CHRG_AMT
        del line['LINE_ALOWD_CHRG_AMT_1']
        del line['LINE_ALOWD_CHRG_AMT_2']
        del line['LINE_ALOWD_CHRG_AMT_3']
        del line['LINE_ALOWD_CHRG_AMT_4']
        del line['LINE_ALOWD_CHRG_AMT_5']
        del line['LINE_ALOWD_CHRG_AMT_6']
        del line['LINE_ALOWD_CHRG_AMT_7']
        del line['LINE_ALOWD_CHRG_AMT_8']
        del line['LINE_ALOWD_CHRG_AMT_9']
        del line['LINE_ALOWD_CHRG_AMT_10']
        del line['LINE_ALOWD_CHRG_AMT_11']
        del line['LINE_ALOWD_CHRG_AMT_12']
        del line['LINE_ALOWD_CHRG_AMT_13']
        LINE_PRCSG_IND_CD = []
        if line['LINE_PRCSG_IND_CD_1']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_1'])
        if line['LINE_PRCSG_IND_CD_2']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_2'])
        if line['LINE_PRCSG_IND_CD_3']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_3'])
        if line['LINE_PRCSG_IND_CD_4']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_4'])
        if line['LINE_PRCSG_IND_CD_5']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_5'])
        if line['LINE_PRCSG_IND_CD_6']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_6'])
        if line['LINE_PRCSG_IND_CD_7']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_7'])
        if line['LINE_PRCSG_IND_CD_8']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_8'])
        if line['LINE_PRCSG_IND_CD_9']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_9'])
        if line['LINE_PRCSG_IND_CD_10']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_10'])
        if line['LINE_PRCSG_IND_CD_11']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_11'])
        if line['LINE_PRCSG_IND_CD_12']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_12'])
        if line['LINE_PRCSG_IND_CD_13']:
          LINE_PRCSG_IND_CD.append(line['LINE_PRCSG_IND_CD_13'])
        line['LINE_PRCSG_IND_CD'] = LINE_PRCSG_IND_CD
        del line['LINE_PRCSG_IND_CD_1']
        del line['LINE_PRCSG_IND_CD_2']
        del line['LINE_PRCSG_IND_CD_3']
        del line['LINE_PRCSG_IND_CD_4']
        del line['LINE_PRCSG_IND_CD_5']
        del line['LINE_PRCSG_IND_CD_6']
        del line['LINE_PRCSG_IND_CD_7']
        del line['LINE_PRCSG_IND_CD_8']
        del line['LINE_PRCSG_IND_CD_9']
        del line['LINE_PRCSG_IND_CD_10']
        del line['LINE_PRCSG_IND_CD_11']
        del line['LINE_PRCSG_IND_CD_12']
        del line['LINE_PRCSG_IND_CD_13']
        LINE_ICD9_DGNS_CD = []
        if line['LINE_ICD9_DGNS_CD_1']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_1'])
        if line['LINE_ICD9_DGNS_CD_2']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_2'])
        if line['LINE_ICD9_DGNS_CD_3']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_3'])
        if line['LINE_ICD9_DGNS_CD_4']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_4'])
        if line['LINE_ICD9_DGNS_CD_5']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_5'])
        if line['LINE_ICD9_DGNS_CD_6']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_6'])
        if line['LINE_ICD9_DGNS_CD_7']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_7'])
        if line['LINE_ICD9_DGNS_CD_8']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_8'])
        if line['LINE_ICD9_DGNS_CD_9']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_9'])
        if line['LINE_ICD9_DGNS_CD_10']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_10'])
        if line['LINE_ICD9_DGNS_CD_11']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_11'])
        if line['LINE_ICD9_DGNS_CD_12']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_12'])
        if line['LINE_ICD9_DGNS_CD_13']:
          LINE_ICD9_DGNS_CD.append(line['LINE_ICD9_DGNS_CD_13'])
        line['LINE_ICD9_DGNS_CD'] = LINE_ICD9_DGNS_CD
        del line['LINE_ICD9_DGNS_CD_1']
        del line['LINE_ICD9_DGNS_CD_2']
        del line['LINE_ICD9_DGNS_CD_3']
        del line['LINE_ICD9_DGNS_CD_4']
        del line['LINE_ICD9_DGNS_CD_5']
        del line['LINE_ICD9_DGNS_CD_6']
        del line['LINE_ICD9_DGNS_CD_7']
        del line['LINE_ICD9_DGNS_CD_8']
        del line['LINE_ICD9_DGNS_CD_9']
        del line['LINE_ICD9_DGNS_CD_10']
        del line['LINE_ICD9_DGNS_CD_11']
        del line['LINE_ICD9_DGNS_CD_12']
        del line['LINE_ICD9_DGNS_CD_13']
        # print line
        object_id = carrier.save(line)
  print "Ending count is " + str(carrier.count())

## Load Inpatient Data
def load_ip():
  ip = pymongo.MongoClient('mongodb://localhost',27017).synPUF.ip
  print "Starting count is " + str(ip.count())
  file_name = '/Users/rm/Downloads/synPUF/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv'
  ip_file = file(file_name, "rU")
  reader = csv.DictReader( ip_file, fieldnames = ("DESYNPUF_ID","CLM_ID","SEGMENT","CLM_FROM_DT","CLM_THRU_DT","PRVDR_NUM","CLM_PMT_AMT","NCH_PRMRY_PYR_CLM_PD_AMT","AT_PHYSN_NPI","OP_PHYSN_NPI","OT_PHYSN_NPI","CLM_ADMSN_DT","ADMTNG_ICD9_DGNS_CD","CLM_PASS_THRU_PER_DIEM_AMT","NCH_BENE_IP_DDCTBL_AMT","NCH_BENE_PTA_COINSRNC_LBLTY_AM","NCH_BENE_BLOOD_DDCTBL_LBLTY_AM","CLM_UTLZTN_DAY_CNT","NCH_BENE_DSCHRG_DT","CLM_DRG_CD","ICD9_DGNS_CD_1","ICD9_DGNS_CD_2","ICD9_DGNS_CD_3","ICD9_DGNS_CD_4","ICD9_DGNS_CD_5","ICD9_DGNS_CD_6","ICD9_DGNS_CD_7","ICD9_DGNS_CD_8","ICD9_DGNS_CD_9","ICD9_DGNS_CD_10","ICD9_PRCDR_CD_1","ICD9_PRCDR_CD_2","ICD9_PRCDR_CD_3","ICD9_PRCDR_CD_4","ICD9_PRCDR_CD_5","ICD9_PRCDR_CD_6","HCPCS_CD_1","HCPCS_CD_2","HCPCS_CD_3","HCPCS_CD_4","HCPCS_CD_5","HCPCS_CD_6","HCPCS_CD_7","HCPCS_CD_8","HCPCS_CD_9","HCPCS_CD_10","HCPCS_CD_11","HCPCS_CD_12","HCPCS_CD_13","HCPCS_CD_14","HCPCS_CD_15","HCPCS_CD_16","HCPCS_CD_17","HCPCS_CD_18","HCPCS_CD_19","HCPCS_CD_20","HCPCS_CD_21","HCPCS_CD_22","HCPCS_CD_23","HCPCS_CD_24","HCPCS_CD_25","HCPCS_CD_26","HCPCS_CD_27","HCPCS_CD_28","HCPCS_CD_29","HCPCS_CD_30","HCPCS_CD_31","HCPCS_CD_32","HCPCS_CD_33","HCPCS_CD_34","HCPCS_CD_35","HCPCS_CD_36","HCPCS_CD_37","HCPCS_CD_38","HCPCS_CD_39","HCPCS_CD_40","HCPCS_CD_41","HCPCS_CD_42","HCPCS_CD_43","HCPCS_CD_44","HCPCS_CD_45"))
  
  for line in reader:
    if line['DESYNPUF_ID'] <> 'DESYNPUF_ID':  # ignore header line
        line['_id'] = line['DESYNPUF_ID'] + '_' + line['CLM_ID'] + '_' + line['SEGMENT']
        ICD9_DGNS_CD = []
        if line['ICD9_DGNS_CD_1']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_1'])
        if line['ICD9_DGNS_CD_2']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_2'])
        if line['ICD9_DGNS_CD_3']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_3'])
        if line['ICD9_DGNS_CD_4']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_4'])
        if line['ICD9_DGNS_CD_5']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_5'])
        if line['ICD9_DGNS_CD_6']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_6'])
        if line['ICD9_DGNS_CD_7']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_7'])
        if line['ICD9_DGNS_CD_8']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_8'])
        if line['ICD9_DGNS_CD_9']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_9'])
        if line['ICD9_DGNS_CD_10']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_10'])
        line['ICD9_DGNS_CD'] = ICD9_DGNS_CD
        del line['ICD9_DGNS_CD_1']
        del line['ICD9_DGNS_CD_2']
        del line['ICD9_DGNS_CD_3']
        del line['ICD9_DGNS_CD_4']
        del line['ICD9_DGNS_CD_5']
        del line['ICD9_DGNS_CD_6']
        del line['ICD9_DGNS_CD_7']
        del line['ICD9_DGNS_CD_8']
        del line['ICD9_DGNS_CD_9']
        del line['ICD9_DGNS_CD_10']
        ICD9_PRCDR_CD = []
        if line['ICD9_PRCDR_CD_1']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_1'])
        if line['ICD9_PRCDR_CD_2']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_2'])
        if line['ICD9_PRCDR_CD_3']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_3'])
        if line['ICD9_PRCDR_CD_4']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_4'])
        if line['ICD9_PRCDR_CD_5']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_5'])
        if line['ICD9_PRCDR_CD_6']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_6'])
        line['ICD9_PRCDR_CD'] = ICD9_PRCDR_CD
        del line['ICD9_PRCDR_CD_1']
        del line['ICD9_PRCDR_CD_2']
        del line['ICD9_PRCDR_CD_3']
        del line['ICD9_PRCDR_CD_4']
        del line['ICD9_PRCDR_CD_5']
        del line['ICD9_PRCDR_CD_6']
        HCPCS_CD = []
        if line['HCPCS_CD_1']:
          HCPCS_CD.append(line['HCPCS_CD_1'])
        if line['HCPCS_CD_2']:
          HCPCS_CD.append(line['HCPCS_CD_2'])
        if line['HCPCS_CD_3']:
          HCPCS_CD.append(line['HCPCS_CD_3'])
        if line['HCPCS_CD_4']:
          HCPCS_CD.append(line['HCPCS_CD_4'])
        if line['HCPCS_CD_5']:
          HCPCS_CD.append(line['HCPCS_CD_5'])
        if line['HCPCS_CD_6']:
          HCPCS_CD.append(line['HCPCS_CD_6'])
        if line['HCPCS_CD_7']:
          HCPCS_CD.append(line['HCPCS_CD_7'])
        if line['HCPCS_CD_8']:
          HCPCS_CD.append(line['HCPCS_CD_8'])
        if line['HCPCS_CD_9']:
          HCPCS_CD.append(line['HCPCS_CD_9'])
        if line['HCPCS_CD_10']:
          HCPCS_CD.append(line['HCPCS_CD_10'])
        if line['HCPCS_CD_11']:
          HCPCS_CD.append(line['HCPCS_CD_11'])
        if line['HCPCS_CD_12']:
          HCPCS_CD.append(line['HCPCS_CD_12'])
        if line['HCPCS_CD_13']:
          HCPCS_CD.append(line['HCPCS_CD_13'])
        if line['HCPCS_CD_14']:
          HCPCS_CD.append(line['HCPCS_CD_14'])
        if line['HCPCS_CD_15']:
          HCPCS_CD.append(line['HCPCS_CD_15'])
        if line['HCPCS_CD_16']:
          HCPCS_CD.append(line['HCPCS_CD_16'])
        if line['HCPCS_CD_17']:
          HCPCS_CD.append(line['HCPCS_CD_17'])
        if line['HCPCS_CD_18']:
          HCPCS_CD.append(line['HCPCS_CD_18'])
        if line['HCPCS_CD_19']:
          HCPCS_CD.append(line['HCPCS_CD_19'])
        if line['HCPCS_CD_20']:
          HCPCS_CD.append(line['HCPCS_CD_20'])
        if line['HCPCS_CD_21']:
          HCPCS_CD.append(line['HCPCS_CD_21'])
        if line['HCPCS_CD_22']:
          HCPCS_CD.append(line['HCPCS_CD_22'])
        if line['HCPCS_CD_23']:
          HCPCS_CD.append(line['HCPCS_CD_23'])
        if line['HCPCS_CD_24']:
          HCPCS_CD.append(line['HCPCS_CD_24'])
        if line['HCPCS_CD_25']:
          HCPCS_CD.append(line['HCPCS_CD_25'])
        if line['HCPCS_CD_26']:
          HCPCS_CD.append(line['HCPCS_CD_26'])
        if line['HCPCS_CD_27']:
          HCPCS_CD.append(line['HCPCS_CD_27'])
        if line['HCPCS_CD_28']:
          HCPCS_CD.append(line['HCPCS_CD_28'])
        if line['HCPCS_CD_29']:
          HCPCS_CD.append(line['HCPCS_CD_29'])
        if line['HCPCS_CD_30']:
          HCPCS_CD.append(line['HCPCS_CD_30'])
        if line['HCPCS_CD_31']:
          HCPCS_CD.append(line['HCPCS_CD_31'])
        if line['HCPCS_CD_32']:
          HCPCS_CD.append(line['HCPCS_CD_32'])
        if line['HCPCS_CD_33']:
          HCPCS_CD.append(line['HCPCS_CD_33'])
        if line['HCPCS_CD_34']:
          HCPCS_CD.append(line['HCPCS_CD_34'])
        if line['HCPCS_CD_35']:
          HCPCS_CD.append(line['HCPCS_CD_35'])
        if line['HCPCS_CD_36']:
          HCPCS_CD.append(line['HCPCS_CD_36'])
        if line['HCPCS_CD_37']:
          HCPCS_CD.append(line['HCPCS_CD_37'])
        if line['HCPCS_CD_38']:
          HCPCS_CD.append(line['HCPCS_CD_38'])
        if line['HCPCS_CD_39']:
          HCPCS_CD.append(line['HCPCS_CD_39'])
        if line['HCPCS_CD_40']:
          HCPCS_CD.append(line['HCPCS_CD_40'])
        if line['HCPCS_CD_41']:
          HCPCS_CD.append(line['HCPCS_CD_41'])
        if line['HCPCS_CD_42']:
          HCPCS_CD.append(line['HCPCS_CD_42'])
        if line['HCPCS_CD_43']:
          HCPCS_CD.append(line['HCPCS_CD_43'])
        if line['HCPCS_CD_44']:
          HCPCS_CD.append(line['HCPCS_CD_44'])
        if line['HCPCS_CD_45']:
          HCPCS_CD.append(line['HCPCS_CD_45'])
        line['HCPCS_CD'] = HCPCS_CD
        del line['HCPCS_CD_1']
        del line['HCPCS_CD_2']
        del line['HCPCS_CD_3']
        del line['HCPCS_CD_4']
        del line['HCPCS_CD_5']
        del line['HCPCS_CD_6']
        del line['HCPCS_CD_7']
        del line['HCPCS_CD_8']
        del line['HCPCS_CD_9']
        del line['HCPCS_CD_10']
        del line['HCPCS_CD_11']
        del line['HCPCS_CD_12']
        del line['HCPCS_CD_13']
        del line['HCPCS_CD_14']
        del line['HCPCS_CD_15']
        del line['HCPCS_CD_16']
        del line['HCPCS_CD_17']
        del line['HCPCS_CD_18']
        del line['HCPCS_CD_19']
        del line['HCPCS_CD_20']
        del line['HCPCS_CD_21']
        del line['HCPCS_CD_22']
        del line['HCPCS_CD_23']
        del line['HCPCS_CD_24']
        del line['HCPCS_CD_25']
        del line['HCPCS_CD_26']
        del line['HCPCS_CD_27']
        del line['HCPCS_CD_28']
        del line['HCPCS_CD_29']
        del line['HCPCS_CD_30']
        del line['HCPCS_CD_31']
        del line['HCPCS_CD_32']
        del line['HCPCS_CD_33']
        del line['HCPCS_CD_34']
        del line['HCPCS_CD_35']
        del line['HCPCS_CD_36']
        del line['HCPCS_CD_37']
        del line['HCPCS_CD_38']
        del line['HCPCS_CD_39']
        del line['HCPCS_CD_40']
        del line['HCPCS_CD_41']
        del line['HCPCS_CD_42']
        del line['HCPCS_CD_43']
        del line['HCPCS_CD_44']
        del line['HCPCS_CD_45']
        object_id = ip.save(line)
  print "Ending count is " + str(ip.count())

## Load Outpatient Data
def load_op():
  op = pymongo.MongoClient('mongodb://localhost',27017).synPUF.op
  print "Starting count is " + str(op.count())
  file_name = '/Users/rm/Downloads/synPUF/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv'
  op_file = file(file_name, "rU")
  reader = csv.DictReader( op_file, fieldnames = ("DESYNPUF_ID","CLM_ID","SEGMENT",\
    "CLM_FROM_DT","CLM_THRU_DT","PRVDR_NUM","CLM_PMT_AMT","NCH_PRMRY_PYR_CLM_PD_AMT",\
    "AT_PHYSN_NPI","OP_PHYSN_NPI","OT_PHYSN_NPI","NCH_BENE_BLOOD_DDCTBL_LBLTY_AM",\
    "ICD9_DGNS_CD_1","ICD9_DGNS_CD_2","ICD9_DGNS_CD_3","ICD9_DGNS_CD_4","ICD9_DGNS_CD_5",\
    "ICD9_DGNS_CD_6","ICD9_DGNS_CD_7","ICD9_DGNS_CD_8","ICD9_DGNS_CD_9",\
    "ICD9_DGNS_CD_10","ICD9_PRCDR_CD_1","ICD9_PRCDR_CD_2","ICD9_PRCDR_CD_3",\
    "ICD9_PRCDR_CD_4","ICD9_PRCDR_CD_5","ICD9_PRCDR_CD_6","NCH_BENE_PTB_DDCTBL_AMT",\
    "NCH_BENE_PTB_COINSRNC_AMT","ADMTNG_ICD9_DGNS_CD","HCPCS_CD_1","HCPCS_CD_2",\
    "HCPCS_CD_3","HCPCS_CD_4","HCPCS_CD_5","HCPCS_CD_6","HCPCS_CD_7","HCPCS_CD_8",\
    "HCPCS_CD_9","HCPCS_CD_10","HCPCS_CD_11","HCPCS_CD_12","HCPCS_CD_13","HCPCS_CD_14",\
    "HCPCS_CD_15","HCPCS_CD_16","HCPCS_CD_17","HCPCS_CD_18","HCPCS_CD_19","HCPCS_CD_20",\
    "HCPCS_CD_21","HCPCS_CD_22","HCPCS_CD_23","HCPCS_CD_24","HCPCS_CD_25","HCPCS_CD_26",\
    "HCPCS_CD_27","HCPCS_CD_28","HCPCS_CD_29","HCPCS_CD_30","HCPCS_CD_31","HCPCS_CD_32",\
    "HCPCS_CD_33","HCPCS_CD_34","HCPCS_CD_35","HCPCS_CD_36","HCPCS_CD_37","HCPCS_CD_38",\
    "HCPCS_CD_39","HCPCS_CD_40","HCPCS_CD_41","HCPCS_CD_42","HCPCS_CD_43","HCPCS_CD_44",\
    "HCPCS_CD_45"))
  
  for line in reader:
    if line['DESYNPUF_ID'] <> 'DESYNPUF_ID':  # ignore header line
        line_new = {}
        line_new['_id'] = line['DESYNPUF_ID'] + '_' + line['CLM_ID'] + '_' + line['SEGMENT']
        line_new['DESYNPUF_ID'] = line['DESYNPUF_ID']
        line_new['CLM_ID'] = line['CLM_ID']
        line_new['SEGMENT'] = line['SEGMENT']
        line_new['CLM_FROM_DT'] = line['CLM_FROM_DT']
        line_new['CLM_THRU_DT'] = line['CLM_THRU_DT']
        line_new['NCH_PRMRY_PYR_CLM_PD_AMT'] = line['NCH_PRMRY_PYR_CLM_PD_AMT']
        line_new['CLM_PMT_AMT'] = line['CLM_PMT_AMT']
        line_new['AT_PHYSN_NPI'] = line['AT_PHYSN_NPI']
        line_new['OP_PHYSN_NPI'] = line['OP_PHYSN_NPI']
        line_new['OT_PHYSN_NPI'] = line['OT_PHYSN_NPI']
        line_new['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM'] = line['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']
        line_new['NCH_BENE_PTB_DDCTBL_AMT'] = line['NCH_BENE_PTB_DDCTBL_AMT']
        line_new['NCH_BENE_PTB_COINSRNC_AMT'] = line['NCH_BENE_PTB_COINSRNC_AMT']
        line_new['ADMTNG_ICD9_DGNS_CD'] = line['ADMTNG_ICD9_DGNS_CD']
        ICD9_DGNS_CD = []
        if line['ICD9_DGNS_CD_1']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_1'])
        if line['ICD9_DGNS_CD_2']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_2'])
        if line['ICD9_DGNS_CD_3']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_3'])
        if line['ICD9_DGNS_CD_4']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_4'])
        if line['ICD9_DGNS_CD_5']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_5'])
        if line['ICD9_DGNS_CD_6']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_6'])
        if line['ICD9_DGNS_CD_7']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_7'])
        if line['ICD9_DGNS_CD_8']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_8'])
        if line['ICD9_DGNS_CD_9']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_9'])
        if line['ICD9_DGNS_CD_10']:
          ICD9_DGNS_CD.append(line['ICD9_DGNS_CD_10'])
        line_new['ICD9_DGNS_CD'] = ICD9_DGNS_CD
        ICD9_PRCDR_CD = []
        if line['ICD9_PRCDR_CD_1']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_1'])
        if line['ICD9_PRCDR_CD_2']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_2'])
        if line['ICD9_PRCDR_CD_3']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_3'])
        if line['ICD9_PRCDR_CD_4']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_4'])
        if line['ICD9_PRCDR_CD_5']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_5'])
        if line['ICD9_PRCDR_CD_6']:
          ICD9_PRCDR_CD.append(line['ICD9_PRCDR_CD_6'])
        line_new['ICD9_PRCDR_CD'] = ICD9_PRCDR_CD
        HCPCS_CD = []
        if line['HCPCS_CD_1']:
          HCPCS_CD.append(line['HCPCS_CD_1'])
        if line['HCPCS_CD_2']:
          HCPCS_CD.append(line['HCPCS_CD_2'])
        if line['HCPCS_CD_3']:
          HCPCS_CD.append(line['HCPCS_CD_3'])
        if line['HCPCS_CD_4']:
          HCPCS_CD.append(line['HCPCS_CD_4'])
        if line['HCPCS_CD_5']:
          HCPCS_CD.append(line['HCPCS_CD_5'])
        if line['HCPCS_CD_6']:
          HCPCS_CD.append(line['HCPCS_CD_6'])
        if line['HCPCS_CD_7']:
          HCPCS_CD.append(line['HCPCS_CD_7'])
        if line['HCPCS_CD_8']:
          HCPCS_CD.append(line['HCPCS_CD_8'])
        if line['HCPCS_CD_9']:
          HCPCS_CD.append(line['HCPCS_CD_9'])
        if line['HCPCS_CD_10']:
          HCPCS_CD.append(line['HCPCS_CD_10'])
        if line['HCPCS_CD_11']:
          HCPCS_CD.append(line['HCPCS_CD_11'])
        if line['HCPCS_CD_12']:
          HCPCS_CD.append(line['HCPCS_CD_12'])
        if line['HCPCS_CD_13']:
          HCPCS_CD.append(line['HCPCS_CD_13'])
        if line['HCPCS_CD_14']:
          HCPCS_CD.append(line['HCPCS_CD_14'])
        if line['HCPCS_CD_15']:
          HCPCS_CD.append(line['HCPCS_CD_15'])
        if line['HCPCS_CD_16']:
          HCPCS_CD.append(line['HCPCS_CD_16'])
        if line['HCPCS_CD_17']:
          HCPCS_CD.append(line['HCPCS_CD_17'])
        if line['HCPCS_CD_18']:
          HCPCS_CD.append(line['HCPCS_CD_18'])
        if line['HCPCS_CD_19']:
          HCPCS_CD.append(line['HCPCS_CD_19'])
        if line['HCPCS_CD_20']:
          HCPCS_CD.append(line['HCPCS_CD_20'])
        if line['HCPCS_CD_21']:
          HCPCS_CD.append(line['HCPCS_CD_21'])
        if line['HCPCS_CD_22']:
          HCPCS_CD.append(line['HCPCS_CD_22'])
        if line['HCPCS_CD_23']:
          HCPCS_CD.append(line['HCPCS_CD_23'])
        if line['HCPCS_CD_24']:
          HCPCS_CD.append(line['HCPCS_CD_24'])
        if line['HCPCS_CD_25']:
          HCPCS_CD.append(line['HCPCS_CD_25'])
        if line['HCPCS_CD_26']:
          HCPCS_CD.append(line['HCPCS_CD_26'])
        if line['HCPCS_CD_27']:
          HCPCS_CD.append(line['HCPCS_CD_27'])
        if line['HCPCS_CD_28']:
          HCPCS_CD.append(line['HCPCS_CD_28'])
        if line['HCPCS_CD_29']:
          HCPCS_CD.append(line['HCPCS_CD_29'])
        if line['HCPCS_CD_30']:
          HCPCS_CD.append(line['HCPCS_CD_30'])
        if line['HCPCS_CD_31']:
          HCPCS_CD.append(line['HCPCS_CD_31'])
        if line['HCPCS_CD_32']:
          HCPCS_CD.append(line['HCPCS_CD_32'])
        if line['HCPCS_CD_33']:
          HCPCS_CD.append(line['HCPCS_CD_33'])
        if line['HCPCS_CD_34']:
          HCPCS_CD.append(line['HCPCS_CD_34'])
        if line['HCPCS_CD_35']:
          HCPCS_CD.append(line['HCPCS_CD_35'])
        if line['HCPCS_CD_36']:
          HCPCS_CD.append(line['HCPCS_CD_36'])
        if line['HCPCS_CD_37']:
          HCPCS_CD.append(line['HCPCS_CD_37'])
        if line['HCPCS_CD_38']:
          HCPCS_CD.append(line['HCPCS_CD_38'])
        if line['HCPCS_CD_39']:
          HCPCS_CD.append(line['HCPCS_CD_39'])
        if line['HCPCS_CD_40']:
          HCPCS_CD.append(line['HCPCS_CD_40'])
        if line['HCPCS_CD_41']:
          HCPCS_CD.append(line['HCPCS_CD_41'])
        if line['HCPCS_CD_42']:
          HCPCS_CD.append(line['HCPCS_CD_42'])
        if line['HCPCS_CD_43']:
          HCPCS_CD.append(line['HCPCS_CD_43'])
        if line['HCPCS_CD_44']:
          HCPCS_CD.append(line['HCPCS_CD_44'])
        if line['HCPCS_CD_45']:
          HCPCS_CD.append(line['HCPCS_CD_45'])
        line_new['HCPCS_CD'] = HCPCS_CD
        object_id = op.save(line_new)
  print "Ending count is " + str(op.count())

## Load PDE Data
def load_pde():
  pde = pymongo.MongoClient('mongodb://localhost',27017).synPUF.pde
  print "Starting count is " + str(pde.count())
  file_name = '/Users/rm/Downloads/synPUF/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.csv'
  pde_file = file(file_name, "rU")
  reader = csv.DictReader( pde_file, fieldnames = ("DESYNPUF_ID","PDE_ID","SRVC_DT",\
    "PROD_SRVC_ID","QTY_DSPNSD_NUM","DAYS_SUPLY_NUM","PTNT_PAY_AMT","TOT_RX_CST_AMT"))
  
  for line in reader:
    if line['DESYNPUF_ID'] <> 'DESYNPUF_ID':  # ignore header line
        line['_id'] = line['DESYNPUF_ID'] + '_' + line['PDE_ID']
        object_id = pde.save(line)
  print "Ending count is " + str(pde.count())

def full()
  load_patient()
  load_carrier('/Users/rm/Downloads/synPUF/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv')
  load_carrier('/Users/rm/Downloads/synPUF/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv')
  load_ip()
  load_op()
  load_pde()

'''
1. Drop first line in each file - header
2. DESYNPUF_ID may be duplicated across files; MongoDB just overwrites record (?!)
116353 total patients in 2008
1814 patients only in 2008
1784 patients only in 2008 and 2009
112755 patients in 2010
Confusing - Venn diagrams may not align - so create compound ID with DESYNPUF_ID and YEAR

Inpatient 66773 records
Outpatient 790790 records
Carrier - 2370667 records in 1A - 13.17GB
Look to use multi-values
Assumption - there are no gaps within sequence
Some duplicates with PK of DESYNPUF_ID + CLM_ID ~480

Final counts (from mongodb shell):
> show collections
carrier
ip
op
patient
pde
system.indexes
> db.ip.count()
66773
> db.op.count()
790790
> db.pde.count()
5552421
> db.carrier.count()
4741335
> db.patient.count()
343644

Test Case:
00016F745862898F,887713388064954,20090731,20090731,2788,25000,42769,2469,,,,,8723812670,8723812670,8723812670,8723812670,8723812670,8723812670,8723812670,8723812670,8723812670,8723812670,8723812670,,,418381668,418381668,418381668,418381668,418381668,418381668,418381668,418381668,418381668,418381668,418381668,,,80061,G0103,84439,85025,80048,83036,83036,80053,85025,84443,84460,,,0.00,10.00,20.00,30.00,20.00,20.00,0.00,0.00,10.00,0.00,20.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,10.00,10.00,0.00,30.00,20.00,20.00,10.00,10.00,10.00,0.00,10.00,0.00,0.00,O,A,A,O,N,O,A,A,A,A,A,,,2729,2729,4019,78862,2729,2729,2729,2729,2729,2729,2729,,
Loaded as:
> db.carrier.find({_id:'00016F745862898F_887713388064954'})
{ "_id" : "00016F745862898F_887713388064954", "LINE_ICD9_DGNS_CD" : [ 	"2729", 	"2729", 	"4019", 	"78862", 	"2729", 	"2729", 	"2729", 	"2729", 	"2729", 	"2729", 	"2729" ], "CLM_THRU_DT" : "20090731", "CLM_ID" : "887713388064954", "LINE_BENE_PTB_DDCTBL_AMT" : [ 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00" ], "LINE_PRCSG_IND_CD" : [ "O", "A", "A", "O", "N", "O", "A", "A", "A", "A", "A" ], "LINE_COINSRNC_AMT" : [ 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00" ], "CLM_FROM_DT" : "20090731", "LINE_NCH_PMT_AMT" : [ 	"0.00", 	"10.00", 	"20.00", 	"30.00", 	"20.00", 	"20.00", 	"0.00", 	"0.00", 	"10.00", 	"0.00", 	"20.00", 	"0.00", 	"0.00" ], "LINE_BENE_PRMRY_PYR_PD_AMT" : [ 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00", 	"0.00" ], "ICD9_DGNS_CD" : [ "2788", "25000", "42769", "2469" ], "DESYNPUF_ID" : "00016F745862898F", "LINE_ALOWD_CHRG_AMT" : [ 	"10.00", 	"10.00", 	"0.00", 	"30.00", 	"20.00", 	"20.00", 	"10.00", 	"10.00", 	"10.00", 	"0.00", 	"10.00", 	"0.00", 	"0.00" ], "PRF_PHYSN_NPI" : [ 	"8723812670", 	"8723812670", 	"8723812670", 	"8723812670", 	"8723812670", 	"8723812670", 	"8723812670", 	"8723812670", 	"8723812670", 	"8723812670", 	"8723812670" ], "TAX_NUM" : [ 	"418381668", 	"418381668", 	"418381668", 	"418381668", 	"418381668", 	"418381668", 	"418381668", 	"418381668", 	"418381668", 	"418381668", 	"418381668" ], "HCPCS_CD" : [ 	"80061", 	"G0103", 	"84439", 	"85025", 	"80048", 	"83036", 	"83036", 	"80053", 	"85025", 	"84443", 	"84460" ] }
pprint as:
{'CLM_FROM_DT': '20090731',
 'CLM_ID': '887713388064954',
 'CLM_THRU_DT': '20090731',
 'DESYNPUF_ID': '00016F745862898F',
 'HCPCS_CD': ['80061',
              'G0103',
              '84439',
              '85025',
              '80048',
              '83036',
              '83036',
              '80053',
              '85025',
              '84443',
              '84460'],
 'ICD9_DGNS_CD': ['2788', '25000', '42769', '2469'],
 'LINE_ALOWD_CHRG_AMT': ['10.00',
                         '10.00',
                         '0.00',
                         '30.00',
                         '20.00',
                         '20.00',
                         '10.00',
                         '10.00',
                         '10.00',
                         '0.00',
                         '10.00',
                         '0.00',
                         '0.00'],
 'LINE_BENE_PRMRY_PYR_PD_AMT': ['0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00',
                                '0.00'],
 'LINE_BENE_PTB_DDCTBL_AMT': ['0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00',
                              '0.00'],
 'LINE_COINSRNC_AMT': ['0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00',
                       '0.00'],
 'LINE_ICD9_DGNS_CD': ['2729',
                       '2729',
                       '4019',
                       '78862',
                       '2729',
                       '2729',
                       '2729',
                       '2729',
                       '2729',
                       '2729',
                       '2729'],
 'LINE_NCH_PMT_AMT': ['0.00',
                      '10.00',
                      '20.00',
                      '30.00',
                      '20.00',
                      '20.00',
                      '0.00',
                      '0.00',
                      '10.00',
                      '0.00',
                      '20.00',
                      '0.00',
                      '0.00'],
 'LINE_PRCSG_IND_CD': ['O', 'A', 'A', 'O', 'N', 'O', 'A', 'A', 'A', 'A', 'A'],
 'PRF_PHYSN_NPI': ['8723812670',
                   '8723812670',
                   '8723812670',
                   '8723812670',
                   '8723812670',
                   '8723812670',
                   '8723812670',
                   '8723812670',
                   '8723812670',
                   '8723812670',
                   '8723812670'],
 'TAX_NUM': ['418381668',
             '418381668',
             '418381668',
             '418381668',
             '418381668',
             '418381668',
             '418381668',
             '418381668',
             '418381668',
             '418381668',
             '418381668'],
 '_id': '00016F745862898F_887713388064954'}
'''

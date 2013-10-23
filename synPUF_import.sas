libname raj 'Y:\User725\synPUFsaslib';

options validvarname=upcase
        compress=binary
        mprint
        ls=100 ps=60;

PROC IMPORT
  datafile='Y:\DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv'
  out=raj.bene2008
  dbms=dlm
  replace;
delimiter=',';
getnames=yes;

PROC IMPORT
  datafile='Y:\DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv'
  out=raj.bene2009
  dbms=dlm
  replace;
delimiter=',';
getnames=yes;

PROC IMPORT
  datafile='Y:\DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv'
  out=raj.bene2010
  dbms=dlm
  replace;
delimiter=',';
getnames=yes;

PROC IMPORT
  datafile='Y:\DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv'
  out=raj.ip
  dbms=dlm
  replace;
delimiter=',';
getnames=yes;

PROC IMPORT
  datafile='Y:\DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv'
  out=raj.op
  dbms=dlm
  replace;
delimiter=',';
getnames=yes;


       data raj.bene2008;
         infile 'Y:\DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv' delimiter=',' MISSOVER DSD firstobs=2 LRECL=32760;

      attrib DESYNPUF_ID     length=$16 format=$16.       label='DESYNPUF: Beneficiary Code'
             BENE_BIRTH_DT   length=4 format=YYMMDDN8. informat=yymmdd8.  label='DESYNPUF: Date of birth'
             BENE_DEATH_DT   length=4 format=YYMMDDN8. informat=yymmdd8.  label='DESYNPUF: Date of death'
             BENE_SEX_IDENT_CD  length=$1 format=$1.      label='DESYNPUF: Sex'
             BENE_RACE_CD    length=$1 format=$1.         label='DESYNPUF: Beneficiary Race Code'
             BENE_ESRD_IND   length=$1 format=$1.         label='DESYNPUF: End stage renal disease Indicator'
             SP_STATE_CODE      length=$2 format=$2.         label='DESYNPUF: State Code'
             BENE_COUNTY_CD  length=$3 format=$3.         label='DESYNPUF: County Code'
             BENE_HI_CVRAGE_TOT_MONS  length=3 format=2.  label='DESYNPUF: Total number of months of part A coverage for the beneficiary.'
             BENE_SMI_CVRAGE_TOT_MONS length=3 format=2.  label='DESYNPUF: Total number of months of part B coverage for the beneficiary.'
             BENE_HMO_CVRAGE_TOT_MONS length=3 format=2.  label='DESYNPUF: Total number of months of HMO coverage for the beneficiary.'
             PLAN_CVRG_MOS_NUM  length=$2 format=$2.      label='DESYNPUF: Total number of months of part D plan coverage for the beneficiary.'
             SP_ALZHDMTA     length=3 format=1.           label='DESYNPUF: Chronic Condition: Alzheimer or related disorders or senile'
             SP_CHF          length=3 format=1.           label='DESYNPUF: Chronic Condition: Heart Failure'
             SP_CHRNKIDN     length=3 format=1.           label='DESYNPUF: Chronic Condition: Chronic Kidney Disease'
             SP_CNCR         length=3 format=1.           label='DESYNPUF: Chronic Condition: Cancer'
             SP_COPD         length=3 format=1.           label='DESYNPUF: Chronic Condition: Chronic Obstructive Pulmonary Disease'
             SP_DEPRESSN     length=3 format=1.           label='DESYNPUF: Chronic Condition: Depression'
             SP_DIABETES     length=3 format=1.           label='DESYNPUF: Chronic Condition: Diabetes'
             SP_ISCHMCHT     length=3 format=1.           label='DESYNPUF: Chronic Condition: Ischemic Heart Disease'
             SP_OSTEOPRS     length=3 format=1.           label='DESYNPUF: Chronic Condition: Osteoporosis'
             SP_RA_OA        length=3 format=1.           label='DESYNPUF: Chronic Condition: RA/OA'
             SP_STRKETIA     length=3 format=1.           label='DESYNPUF: Chronic Condition: Stroke/transient Ischemic Attack'
             MEDREIMB_IP     length=8 format=10.2         label='DESYNPUF: Inpatient annual Medicare reimbursement amount'
             BENRES_IP       length=8 format=10.2         label='DESYNPUF: Inpatient annual beneficiary responsibility amount'
             PPPYMT_IP       length=8 format=10.2         label='DESYNPUF: Inpatient annual primary payer reimbursement amount'
             MEDREIMB_OP     length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual Medicare reimbursement amount'
             BENRES_OP       length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual beneficiary responsibility amount'
             PPPYMT_OP       length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual primary payer reimbursement amount'
             MEDREIMB_CAR    length=8 format=10.2         label='DESYNPUF: Carrier annual Medicare reimbursement amount'
             BENRES_CAR      length=8 format=10.2         label='DESYNPUF: Carrier annual beneficiary responsibility amount'
             PPPYMT_CAR      length=8 format=10.2         label='DESYNPUF: Carrier annual primary payer reimbursement amount'
          ;

      input DESYNPUF_ID
            BENE_BIRTH_DT
            BENE_DEATH_DT
            BENE_SEX_IDENT_CD
            BENE_RACE_CD
            BENE_ESRD_IND
            SP_STATE_CODE
            BENE_COUNTY_CD
            BENE_HI_CVRAGE_TOT_MONS
            BENE_SMI_CVRAGE_TOT_MONS
            BENE_HMO_CVRAGE_TOT_MONS
            PLAN_CVRG_MOS_NUM
            SP_ALZHDMTA
            SP_CHF
            SP_CHRNKIDN
            SP_CNCR
            SP_COPD
            SP_DEPRESSN
            SP_DIABETES
            SP_ISCHMCHT
            SP_OSTEOPRS
            SP_RA_OA
            SP_STRKETIA
            MEDREIMB_IP
            BENRES_IP
            PPPYMT_IP
            MEDREIMB_OP
            BENRES_OP
            PPPYMT_OP
            MEDREIMB_CAR
            BENRES_CAR
            PPPYMT_CAR;

    run;


       data raj.bene2009;
         infile 'Y:\DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv' delimiter=',' MISSOVER DSD firstobs=2 LRECL=32760;

      attrib DESYNPUF_ID     length=$16 format=$16.       label='DESYNPUF: Beneficiary Code'
             BENE_BIRTH_DT   length=4 format=YYMMDDN8. informat=yymmdd8.  label='DESYNPUF: Date of birth'
             BENE_DEATH_DT   length=4 format=YYMMDDN8. informat=yymmdd8.  label='DESYNPUF: Date of death'
             BENE_SEX_IDENT_CD  length=$1 format=$1.      label='DESYNPUF: Sex'
             BENE_RACE_CD    length=$1 format=$1.         label='DESYNPUF: Beneficiary Race Code'
             BENE_ESRD_IND   length=$1 format=$1.         label='DESYNPUF: End stage renal disease Indicator'
             SP_STATE_CODE      length=$2 format=$2.         label='DESYNPUF: State Code'
             BENE_COUNTY_CD  length=$3 format=$3.         label='DESYNPUF: County Code'
             BENE_HI_CVRAGE_TOT_MONS  length=3 format=2.  label='DESYNPUF: Total number of months of part A coverage for the beneficiary.'
             BENE_SMI_CVRAGE_TOT_MONS length=3 format=2.  label='DESYNPUF: Total number of months of part B coverage for the beneficiary.'
             BENE_HMO_CVRAGE_TOT_MONS length=3 format=2.  label='DESYNPUF: Total number of months of HMO coverage for the beneficiary.'
             PLAN_CVRG_MOS_NUM  length=$2 format=$2.      label='DESYNPUF: Total number of months of part D plan coverage for the beneficiary.'
             SP_ALZHDMTA     length=3 format=1.           label='DESYNPUF: Chronic Condition: Alzheimer or related disorders or senile'
             SP_CHF          length=3 format=1.           label='DESYNPUF: Chronic Condition: Heart Failure'
             SP_CHRNKIDN     length=3 format=1.           label='DESYNPUF: Chronic Condition: Chronic Kidney Disease'
             SP_CNCR         length=3 format=1.           label='DESYNPUF: Chronic Condition: Cancer'
             SP_COPD         length=3 format=1.           label='DESYNPUF: Chronic Condition: Chronic Obstructive Pulmonary Disease'
             SP_DEPRESSN     length=3 format=1.           label='DESYNPUF: Chronic Condition: Depression'
             SP_DIABETES     length=3 format=1.           label='DESYNPUF: Chronic Condition: Diabetes'
             SP_ISCHMCHT     length=3 format=1.           label='DESYNPUF: Chronic Condition: Ischemic Heart Disease'
             SP_OSTEOPRS     length=3 format=1.           label='DESYNPUF: Chronic Condition: Osteoporosis'
             SP_RA_OA        length=3 format=1.           label='DESYNPUF: Chronic Condition: RA/OA'
             SP_STRKETIA     length=3 format=1.           label='DESYNPUF: Chronic Condition: Stroke/transient Ischemic Attack'
             MEDREIMB_IP     length=8 format=10.2         label='DESYNPUF: Inpatient annual Medicare reimbursement amount'
             BENRES_IP       length=8 format=10.2         label='DESYNPUF: Inpatient annual beneficiary responsibility amount'
             PPPYMT_IP       length=8 format=10.2         label='DESYNPUF: Inpatient annual primary payer reimbursement amount'
             MEDREIMB_OP     length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual Medicare reimbursement amount'
             BENRES_OP       length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual beneficiary responsibility amount'
             PPPYMT_OP       length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual primary payer reimbursement amount'
             MEDREIMB_CAR    length=8 format=10.2         label='DESYNPUF: Carrier annual Medicare reimbursement amount'
             BENRES_CAR      length=8 format=10.2         label='DESYNPUF: Carrier annual beneficiary responsibility amount'
             PPPYMT_CAR      length=8 format=10.2         label='DESYNPUF: Carrier annual primary payer reimbursement amount'
          ;

      input DESYNPUF_ID
            BENE_BIRTH_DT
            BENE_DEATH_DT
            BENE_SEX_IDENT_CD
            BENE_RACE_CD
            BENE_ESRD_IND
            SP_STATE_CODE
            BENE_COUNTY_CD
            BENE_HI_CVRAGE_TOT_MONS
            BENE_SMI_CVRAGE_TOT_MONS
            BENE_HMO_CVRAGE_TOT_MONS
            PLAN_CVRG_MOS_NUM
            SP_ALZHDMTA
            SP_CHF
            SP_CHRNKIDN
            SP_CNCR
            SP_COPD
            SP_DEPRESSN
            SP_DIABETES
            SP_ISCHMCHT
            SP_OSTEOPRS
            SP_RA_OA
            SP_STRKETIA
            MEDREIMB_IP
            BENRES_IP
            PPPYMT_IP
            MEDREIMB_OP
            BENRES_OP
            PPPYMT_OP
            MEDREIMB_CAR
            BENRES_CAR
            PPPYMT_CAR;

    run;


       data raj.bene2010;
         infile 'Y:\DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv' delimiter=',' MISSOVER DSD firstobs=2 LRECL=32760;

      attrib DESYNPUF_ID     length=$16 format=$16.       label='DESYNPUF: Beneficiary Code'
             BENE_BIRTH_DT   length=4 format=YYMMDDN8. informat=yymmdd8.  label='DESYNPUF: Date of birth'
             BENE_DEATH_DT   length=4 format=YYMMDDN8. informat=yymmdd8.  label='DESYNPUF: Date of death'
             BENE_SEX_IDENT_CD  length=$1 format=$1.      label='DESYNPUF: Sex'
             BENE_RACE_CD    length=$1 format=$1.         label='DESYNPUF: Beneficiary Race Code'
             BENE_ESRD_IND   length=$1 format=$1.         label='DESYNPUF: End stage renal disease Indicator'
             SP_STATE_CODE      length=$2 format=$2.         label='DESYNPUF: State Code'
             BENE_COUNTY_CD  length=$3 format=$3.         label='DESYNPUF: County Code'
             BENE_HI_CVRAGE_TOT_MONS  length=3 format=2.  label='DESYNPUF: Total number of months of part A coverage for the beneficiary.'
             BENE_SMI_CVRAGE_TOT_MONS length=3 format=2.  label='DESYNPUF: Total number of months of part B coverage for the beneficiary.'
             BENE_HMO_CVRAGE_TOT_MONS length=3 format=2.  label='DESYNPUF: Total number of months of HMO coverage for the beneficiary.'
             PLAN_CVRG_MOS_NUM  length=$2 format=$2.      label='DESYNPUF: Total number of months of part D plan coverage for the beneficiary.'
             SP_ALZHDMTA     length=3 format=1.           label='DESYNPUF: Chronic Condition: Alzheimer or related disorders or senile'
             SP_CHF          length=3 format=1.           label='DESYNPUF: Chronic Condition: Heart Failure'
             SP_CHRNKIDN     length=3 format=1.           label='DESYNPUF: Chronic Condition: Chronic Kidney Disease'
             SP_CNCR         length=3 format=1.           label='DESYNPUF: Chronic Condition: Cancer'
             SP_COPD         length=3 format=1.           label='DESYNPUF: Chronic Condition: Chronic Obstructive Pulmonary Disease'
             SP_DEPRESSN     length=3 format=1.           label='DESYNPUF: Chronic Condition: Depression'
             SP_DIABETES     length=3 format=1.           label='DESYNPUF: Chronic Condition: Diabetes'
             SP_ISCHMCHT     length=3 format=1.           label='DESYNPUF: Chronic Condition: Ischemic Heart Disease'
             SP_OSTEOPRS     length=3 format=1.           label='DESYNPUF: Chronic Condition: Osteoporosis'
             SP_RA_OA        length=3 format=1.           label='DESYNPUF: Chronic Condition: RA/OA'
             SP_STRKETIA     length=3 format=1.           label='DESYNPUF: Chronic Condition: Stroke/transient Ischemic Attack'
             MEDREIMB_IP     length=8 format=10.2         label='DESYNPUF: Inpatient annual Medicare reimbursement amount'
             BENRES_IP       length=8 format=10.2         label='DESYNPUF: Inpatient annual beneficiary responsibility amount'
             PPPYMT_IP       length=8 format=10.2         label='DESYNPUF: Inpatient annual primary payer reimbursement amount'
             MEDREIMB_OP     length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual Medicare reimbursement amount'
             BENRES_OP       length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual beneficiary responsibility amount'
             PPPYMT_OP       length=8 format=10.2         label='DESYNPUF: Outpatient Institutional annual primary payer reimbursement amount'
             MEDREIMB_CAR    length=8 format=10.2         label='DESYNPUF: Carrier annual Medicare reimbursement amount'
             BENRES_CAR      length=8 format=10.2         label='DESYNPUF: Carrier annual beneficiary responsibility amount'
             PPPYMT_CAR      length=8 format=10.2         label='DESYNPUF: Carrier annual primary payer reimbursement amount'
          ;

      input DESYNPUF_ID
            BENE_BIRTH_DT
            BENE_DEATH_DT
            BENE_SEX_IDENT_CD
            BENE_RACE_CD
            BENE_ESRD_IND
            SP_STATE_CODE
            BENE_COUNTY_CD
            BENE_HI_CVRAGE_TOT_MONS
            BENE_SMI_CVRAGE_TOT_MONS
            BENE_HMO_CVRAGE_TOT_MONS
            PLAN_CVRG_MOS_NUM
            SP_ALZHDMTA
            SP_CHF
            SP_CHRNKIDN
            SP_CNCR
            SP_COPD
            SP_DEPRESSN
            SP_DIABETES
            SP_ISCHMCHT
            SP_OSTEOPRS
            SP_RA_OA
            SP_STRKETIA
            MEDREIMB_IP
            BENRES_IP
            PPPYMT_IP
            MEDREIMB_OP
            BENRES_OP
            PPPYMT_OP
            MEDREIMB_CAR
            BENRES_CAR
            PPPYMT_CAR;

    run;
    
      data raj.IP(label="DE1_0_2008_to_2010_Inpatient_Claims_Sample_1");
    infile 'Y:\DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv' dsd dlm=',' lrecl=1300 firstobs=2 missover;

    attrib DESYNPUF_ID   length=$16 format=$16. label='DESYNPUF: Beneficiary Code'
           CLM_ID        length=$15 format=$15. label='DESYNPUF: Claim ID'
           SEGMENT       length=3   format=2. label='DESYNPUF: Claim Line Segment'
           CLM_FROM_DT   length=4   informat=yymmdd8. format=yymmddn8. label='DESYNPUF: Claims start date'
           CLM_THRU_DT   length=4   informat=yymmdd8. format=yymmddn8. label='DESYNPUF: Claims end date'
           PRVDR_NUM     length=$6  format=$6. label='DESYNPUF: Provider Institution'
           CLM_PMT_AMT   length=8   format=12.2 label='DESYNPUF: Claim Payment Amount'
           NCH_PRMRY_PYR_CLM_PD_AMT length=8 format=12.2 label='DESYNPUF: NCH Primary Payer Claim Paid Amount'
           AT_PHYSN_NPI  length=$10 format=$10.          label='DESYNPUF: Attending Physician - National Provider Identifier Number'
           OP_PHYSN_NPI  length=$10 format=$10.          label='DESYNPUF: Operating Physician - National Provider Identifier Number'
           OT_PHYSN_NPI  length=$10 format=$10.          label='DESYNPUF: Other Physician - - National Provider Identifier Number'
           CLM_ADMSN_DT  length=4   informat=yymmdd8. format=yymmddn8. label='DESYNPUF: Inpatient admission date'
           ADMTNG_ICD9_DGNS_CD      length=$5 format=$5. label='DESYNPUF: Claim Admitting Diagnosis Code'
           CLM_PASS_THRU_PER_DIEM_AMT length=8 format=12.2 label='DESYNPUF: Claim Pass Thru Per Diem Amount'
           NCH_BENE_IP_DDCTBL_AMT     length=8 format=12.2 label='DESYNPUF: NCH Beneficiary Inpatient Deductible Amount'
           NCH_BENE_PTA_COINSRNC_LBLTY_AM length=8 format=12.2 label='DESYNPUF: NCH Beneficiary Part A Coinsurance Liability Amount'
           NCH_BENE_BLOOD_DDCTBL_LBLTY_AM length=8 format=12.2 label='DESYNPUF: NCH Beneficiary Blood Deductible Liability Amount'
           CLM_UTLZTN_DAY_CNT length=3 format=3. label='DESYNPUF: Claim Utilization Day Count'
           NCH_BENE_DSCHRG_DT length=4 informat=yymmdd8. format=yymmddn8. label='DESYNPUF: Inpatient discharged date'
           CLM_DRG_CD    length=$3  format=$3. label='DESYNPUF: Claim Diagnosis Related Group Code'
           %addseqattrib(varname=ICD9_DGNS_CD_,nvars=10,varlength=$5,varformat=$5.,labeltext=DESYNPUF: Claim Diagnosis Code)
           %addseqattrib(varname=ICD9_PRCDR_CD_,nvars=6,varlength=$5,varformat=$5.,labeltext=DESYNPUF: Claim Procedure Code)
           %addseqattrib(varname=HCPCS_CD_,nvars=45,varlength=$5,varformat=$5.,labeltext=DESYNPUF: Revenue Center HCFA Common Procedure Coding System)

           ;;

    input DESYNPUF_ID
          CLM_ID
          SEGMENT
          CLM_FROM_DT
          CLM_THRU_DT
          PRVDR_NUM
          CLM_PMT_AMT
          NCH_PRMRY_PYR_CLM_PD_AMT
          AT_PHYSN_NPI
          OP_PHYSN_NPI
          OT_PHYSN_NPI
          CLM_ADMSN_DT
          ADMTNG_ICD9_DGNS_CD
          CLM_PASS_THRU_PER_DIEM_AMT
          NCH_BENE_IP_DDCTBL_AMT
          NCH_BENE_PTA_COINSRNC_LBLTY_AM
          NCH_BENE_BLOOD_DDCTBL_LBLTY_AM
          CLM_UTLZTN_DAY_CNT
          NCH_BENE_DSCHRG_DT
          CLM_DRG_CD
          ICD9_DGNS_CD_1-ICD9_DGNS_CD_10
          ICD9_PRCDR_CD_1-ICD9_PRCDR_CD_6
          HCPCS_CD_1-HCPCS_CD_45
        ;
  run;
    
    
  data raj.OP(label="DE1_0_2008_to_2010_Outpatient_Claims_Sample_&filenumber");
    infile 'Y:\DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv' dsd dlm=',' lrecl=1200 firstobs=2 missover;

    attrib DESYNPUF_ID   length=$16 format=$16. label='DESYNPUF: Beneficiary Code'
           CLM_ID        length=$15 format=$15. label='DESYNPUF: Claim ID'
           SEGMENT       length=3   format=2. label='DESYNPUF: Claim Line Segment'
           CLM_FROM_DT   length=4   informat=yymmdd8. format=yymmddn8. label='DESYNPUF: Claims start date'
           CLM_THRU_DT   length=4   informat=yymmdd8. format=yymmddn8. label='DESYNPUF: Claims end date'
           PRVDR_NUM     length=$6  format=$6. label='DESYNPUF: Provider Institution'
           CLM_PMT_AMT   length=8   format=12.2 label='DESYNPUF: Claim Payment Amount'
           NCH_PRMRY_PYR_CLM_PD_AMT length=8 format=12.2 label='DESYNPUF: NCH Primary Payer Claim Paid Amount'
           AT_PHYSN_NPI  length=$10 format=$10.          label='DESYNPUF: Attending Physician - National Provider Identifier Number'
           OP_PHYSN_NPI  length=$10 format=$10.          label='DESYNPUF: Operating Physician - National Provider Identifier Number'
           OT_PHYSN_NPI  length=$10 format=$10.          label='DESYNPUF: Other Physician - - National Provider Identifier Number'
           NCH_BENE_BLOOD_DDCTBL_LBLTY_AM length=8 format=12.2  label='DESYNPUF: NCH Beneficiary Blood Deductible Liability Amount'
           %addseqattrib(varname=ICD9_DGNS_CD_,nvars=10,varlength=$5,varformat=$5.,labeltext=DESYNPUF: Claim Diagnosis Code)
           %addseqattrib(varname=ICD9_PRCDR_CD_,nvars=6,varlength=$5,varformat=$5.,labeltext=DESYNPUF: Claim Procedure Code)
           NCH_BENE_PTB_DDCTBL_AMT   length=8 format=12.2  label='DESYNPUF: NCH Beneficiary Part B Deductible Amount'
           NCH_BENE_PTB_COINSRNC_AMT length=8 format=12.2  label='DESYNPUF: NCH Beneficiary Part B Coinsurance Amount'
           ADMTNG_ICD9_DGNS_CD length=$5 format=$5. label='DESYNPUF: Claim Admitting Diagnosis Code'
           %addseqattrib(varname=HCPCS_CD_,nvars=45,varlength=$5,varformat=$5.,labeltext=DESYNPUF:Revenue Center HCFA Common Procedure Coding System)
           ;;

    input DESYNPUF_ID
          CLM_ID
          SEGMENT
          CLM_FROM_DT
          CLM_THRU_DT
          PRVDR_NUM
          CLM_PMT_AMT
          NCH_PRMRY_PYR_CLM_PD_AMT
          AT_PHYSN_NPI
          OP_PHYSN_NPI
          OT_PHYSN_NPI
          NCH_BENE_BLOOD_DDCTBL_LBLTY_AM
          ICD9_DGNS_CD_1 - ICD9_DGNS_CD_10
          ICD9_PRCDR_CD_1 - ICD9_PRCDR_CD_6
          NCH_BENE_PTB_DDCTBL_AMT
          NCH_BENE_PTB_COINSRNC_AMT
          ADMTNG_ICD9_DGNS_CD
          HCPCS_CD_1 - HCPCS_CD_45
       ;;;

  run;
    
    
  /*********************************************************************/
  /* Examine new data set                                              */
  /*********************************************************************/
    title "Processing DE1_0_2008_to_2010_Inpatient_Claims_Sample_1";
  proc contents data=raj.IP;
  run;
  proc means data=raj.IP;
  run;
  proc freq data=raj.IP;
    table clm_from_dt clm_thru_dt / missing;
    format clm_from_dt clm_thru_dt year4.;
  run;
  
  title "Processing DE1_0_2008_to_2010_Outpatient_Claims_Sample_1";
  proc contents data=raj.OP;
  run;
  proc means data=raj.OP;
    title2 'Simple Means';
  run;
  proc freq data=raj.OP;
    title2 'Simple Frequencies';
    table clm_from_dt clm_thru_dt / missing;
    format clm_from_dt clm_thru_dt year4.;
  run;

libname raj 'Y:\User725\synPUFsaslib';

options validvarname=upcase
        compress=binary
        mprint
        ls=100 ps=60;

/* Beneficiaries enrolled in all 3 years */
DATA raj.BENEall;
MERGE
 raj.BENE2008 (IN=in2008)
 raj.BENE2009 (IN=in2009)
 raj.BENE2010 (IN=in2010);
 BY DESYNPUF_ID;
 
IF in2008 AND in2009 AND in2010;
RUN;
/* After MERGE, if variables are named same, then value from first dataset is retained */
/*
NOTE: 116352 observations were read from "RAJ.BENE2008"
NOTE: 114538 observations were read from "RAJ.BENE2009"
NOTE: 112754 observations were read from "RAJ.BENE2010"
NOTE: Data set "RAJ.BENEall" has 112754 observation(s) and 32 variable(s)
*/

/* Beneficiaries enrolled in all 3 years and have at least one IP claim in any of 3 years */
DATA raj.IPany3;
MERGE
 raj.BENEall (IN=inall3)
 raj.IP (IN=inip);
 BY DESYNPUF_ID;

IF inall3 AND inip;
RUN;
/*
NOTE: 112754 observations were read from "RAJ.BENEall"
NOTE: 66773 observations were read from "RAJ.IP"
NOTE: Data set "RAJ.IPany3" has 65955 observation(s) and 112 variable(s)
*/
/* 66773 - 65955 of the BENEall beneficiaries did not have any IP claims in any of the 3 years */

/* Beneficiaries enrolled in all 3 years and have at least one IP claim in *all* 3 years */
PROC SQL;
CREATE TABLE raj.IPall3 AS
 SELECT A.DESYNPUF_ID
 FROM raj.BENEall A
 INNER JOIN raj.IP B ON A.DESYNPUF_ID = B.DESYNPUF_ID
 INNER JOIN raj.IP C ON A.DESYNPUF_ID = C.DESYNPUF_ID
 INNER JOIN raj.IP D ON A.DESYNPUF_ID = D.DESYNPUF_ID
 WHERE YEAR(B.CLM_THRU_DT) = 2008
   AND YEAR(C.CLM_THRU_DT) = 2009
   AND YEAR(D.CLM_THRU_DT) = 2010
 ORDER BY DESYNPUF_ID;
/*
NOTE: Data set "RAJ.IPall3" has 4593 observation(s) and 1 variable(s)
*/
/* Very small population enrolled in all 3 years and have IP claims in all 3 years! */

/* Can SAS detect correlation between SP_DIABETES and specific ICD9 codes? */
PROC LOGISTIC DATA=raj.IPany3;
MODEL SP_DIABETES = ICD9_DGNS_CD_1 - ICD9_DGNS_CD_10;
RUN;

/* Reproduce Truven Analysis on Top 20 DRG Codes */
PROC FREQ DATA=raj.IP;
  TABLE CLM_DRG_CD;
RUN;

PROC SQL;
CREATE TABLE raj.IP_BY_DRG AS
SELECT CLM_DRG_CD, COUNT(CLM_DRG_CD) AS Discharges, SUM(CLM_PMT_AMT) AS Total_Reimbursement, AVG(CLM_PMT_AMT) AS Average_Reimbursement
FROM raj.IP
WHERE YEAR(CLM_THRU_DT) = 2010
GROUP BY CLM_DRG_CD
ORDER BY Discharges DESCENDING;

PROC SQL OUTOBS=20;
SELECT CLM_DRG_CD, Discharges, Average_Reimbursement
FROM raj.IP_BY_DRG
ORDER BY Discharges DESC;

PROC SQL OUTOBS=20;
SELECT CLM_DRG_CD, Discharges, Total_Reimbursement
FROM raj.IP_BY_DRG
ORDER BY Discharges DESC;

PROC SQL OUTOBS=20;
SELECT CLM_DRG_CD, Discharges, Total_Reimbursement FORMAT 12., Average_Reimbursement FORMAT 12.
FROM raj.IP_BY_DRG
ORDER BY Total_Reimbursement DESC;

/* DRG is a composite of location, age, type of hospital, diagnoses, procedures and complications/co-morbidities.
Looking to see pattern of same patient has more than one DRG over the three years; i.e. whether progressive (monotonically increasing numeric DRG. */

PROC SQL;
CREATE TABLE raj.IP_SUPER_UTILZ AS
SELECT DESYNPUF_ID, COUNT(*) AS DISCHARGE_COUNT
FROM RAJ.IP
GROUP BY DESYNPUF_ID
HAVING DISCHARGE_COUNT > 7
ORDER BY DISCHARGE_COUNT DESC; /* 176 records */

PROC SORT DATA = raj.ip OUT = raj.ip2;
BY DESYNPUF_ID CLM_THRU_DT;
RUN;

PROC SQL;
CREATE TABLE RAJ.IP3 AS
SELECT DISTINCT DESYNPUF_ID, CLM_DRG_CD FROM RAJ.IP2
ORDER BY DESYNPUF_ID, CLM_THRU_DT, CLM_DRG_CD
;
/* 66593 obs in IP3 down from 66773 in IP2 - few duplicate DRG for same patient */

DATA raj.ip4;
SET raj.ip3;
* keep desynpuf_id drg_count updir_count;

IF DESYNPUF_ID = LAG(DESYNPUF_ID) THEN DO;
  IF CLM_DRG_CD < LAG(CLM_DRG_CD) THEN
    DRG_SEQ = -1;
  ELSE IF CLM_DRG_CD = LAG(CLM_DRG_CD) THEN
    DRG_SEQ = 0;
  ELSE IF CLM_DRG_CD > LAG(CLM_DRG_CD) THEN
    DRG_SEQ = 1;
  ELSE DRG_SEQ = 2;
END;
ELSE DRG_SEQ = -2;
RUN;

/* Subset IP4 to focus on super utilizers */
PROC SQL;
CREATE TABLE RAJ.IP5 AS
SELECT * FROM RAJ.IP4
WHERE DESYNPUF_ID IN (SELECT DESYNPUF_ID FROM RAJ.IP_SUPER_UTILZ);
/* 66593 -> 1514 records for 176 patients */

/* Do similarity analysis among these 176 patients to see if same doctor / same facility / same geoloc */

/* Repeat analysis based on total amount billed , rather than # of discharges */

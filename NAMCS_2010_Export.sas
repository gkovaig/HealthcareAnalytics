* SAS program to ingest National Ambulatory Medical Care Survey (NAMCS) data for 2010 from cdc.gov
* Stub of SAS program to Export NAMCS Data from SAS Dataset to CSV File
* Copyright 2013 Raj N Manickam
* Licensed under the Apache License, Version 2.0
* http://www.apache.org/licenses/LICENSE-2.0

libname namcs ‘/Users/rm/health_data/namcs/sas_data’;

FILENAME nam10 ‘/Users/rm/health_data/namcs/NAMCS2010.txt’;
FILENAME nam10inp ‘/Users/rm/health_data/namcs/nam10inp.txt’;
FILENAME nam10for ‘/Users/rm/health_data/namcs/nam10for.txt’;
FILENAME nam10lab ‘/Users/rm/health_data/namcs/nam10lab.txt’;
FILENAME nam10exp ‘/Users/rm/health_data/namcs/nam10exp.csv’;

/* WPS – LABEL and FORMAT-apply works only in subsequent DATA steps, not as part of original read */

DATA nam2010;
INFILE nam10 MISSOVER LRECL=9999;
%INC nam10inp;
%INC nam10for;
;

DATA namcs.nam2010;
SET nam2010;
%INC nam10lab;
;

PROC EXPORT data=namcs.nam2010 OUTFILE=nam10exp DBMS=csv REPLACE
;

RUN;

* SAS program to ingest National Ambulatory Medical Care Survey (NAMCS) data for 2010 from cdc.gov
* Copyright 2013 Raj N Manickam
* Licensed under the Apache License, Version 2.0
* http://www.apache.org/licenses/LICENSE-2.0

* Notes:
* These programs can only be used against the 2010 datasets.
* The format and contents for prior years vary significantly, and need to be processed with
*   their own INPUT, FORMAT and LABEL files.
* Similar programs can be used to export the NHAMCS data for OutPatient and Emergency Department datasets, as well.

libname namcs ‘/Users/rm/health_data/namcs/sas_data’;
FILENAME nam10 ‘/Users/rm/health_data/namcs/NAMCS2010.txt’;
FILENAME nam10inp ‘/Users/rm/health_data/namcs/nam10inp.txt’;
FILENAME nam10for ‘/Users/rm/health_data/namcs/nam10for.txt’;
FILENAME nam10lab ‘/Users/rm/health_data/namcs/nam10lab.txt’;

/* WPS – LABEL and FORMAT-apply works only in subsequent DATA, not as part of original read */

DATA nam2010;
INFILE nam10 MISSOVER LRECL=9999;
%INC nam10inp;
%INC nam10for;
;

DATA namcs.nam2010;
SET nam2010;
%INC nam10lab;
;


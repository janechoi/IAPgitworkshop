***********************************************************************
*** FINAL CLEANING USING FISD DATA
*** CLEANS ABNORMAL PRICE, VOLUME, OBSERVATIONS
*** CLEANS BONDS OFFERED OR MATURING DURING TIME PERIOD OF INTEREST
***********************************************************************
clear
cd "/projects/trace.proj/data"

tempfile fisd



use "/projects/trace.proj/data/rawData/fisd.dta"
rename complete_cusip cusip_id
keep cusip_id prospectus_issuer_name issue_name maturity offering_amt ///
  offering_date principal_amt coupon_type interest_frequency coupon
save "`fisd'", replace
clear


use "/projects/trace.proj/data/04_explained.dta"
drop if droprecord ~= 0
drop droprecord

*** merge on relevant FISD information
merge m:1 cusip_id using "`fisd'"
tab _merge
keep if _merge == 3
drop _merge

gen dr = 0
*** eliminate trades with price/vol issues
replace dr = 1 if dr == 0 & entrd_pr == .
replace dr = 2 if dr == 0 & entrd_pr == 0
replace dr = 3 if dr == 0 & entrd_pr < 0
replace dr = 4 if dr == 0 & entrd_pr > 220

gen relvol = entrd_vol_qt / (offering_amt * 1000)
replace dr = 5 if dr == 0 & relvol >= .5 & offering_amt ~= 0 & ///
  offering_amt ~= 1 & principal_amt ~= 0 & principal_amt ~= 1

replace dr = 6 if dr == 0 & entrd_vol_qt < 1000

*** eliminate trades with timing issues
replace dr = 7 if dr == 0 & date < offering_date
replace dr = 8 if dr == 0 & date > maturity

gen rdate = dofc(trd_rpt_ts)
replace dr = 9 if dr == 0 & date ~= rdate

gen sifma = 0

replace sifma = 1 if date == mdy(3,3,2002)
replace sifma = 1 if date == mdy(3,4,2002)
replace sifma = 1 if date == mdy(8,30,2002)
replace sifma = 1 if date == mdy(9,2,2002)
replace sifma = 1 if date == mdy(10,11,2002)
replace sifma = 1 if date == mdy(10,14,2002)
replace sifma = 1 if date == mdy(11,8,2002)
replace sifma = 1 if date == mdy(11,11,2002)
replace sifma = 1 if date == mdy(11,27,2002)
replace sifma = 1 if date == mdy(11,28,2002)
replace sifma = 1 if date == mdy(11,29,2002)
replace sifma = 1 if date == mdy(12,24,2002)
replace sifma = 1 if date == mdy(12,25,2002)
replace sifma = 1 if date == mdy(12,31,2002)

replace sifma = 1 if date == mdy(1,1,2003)
replace sifma = 1 if date == mdy(1,17,2003)
replace sifma = 1 if date == mdy(1,20,2003)
replace sifma = 1 if date == mdy(2,14,2003)
replace sifma = 1 if date == mdy(2,17,2003)
replace sifma = 1 if date == mdy(4,17,2003)
replace sifma = 1 if date == mdy(4,18,2003)
replace sifma = 1 if date == mdy(5,23,2003)
replace sifma = 1 if date == mdy(5,26,2003)
replace sifma = 1 if date == mdy(7,3,2003)
replace sifma = 1 if date == mdy(7,4,2003)
replace sifma = 1 if date == mdy(8,29,2003)
replace sifma = 1 if date == mdy(9,1,2003)
replace sifma = 1 if date == mdy(10,10,2003)
replace sifma = 1 if date == mdy(10,13,2003)
replace sifma = 1 if date == mdy(11,10,2003)
replace sifma = 1 if date == mdy(11,11,2003)
replace sifma = 1 if date == mdy(11,26,2003)
replace sifma = 1 if date == mdy(11,27,2003)
replace sifma = 1 if date == mdy(11,28,2003)
replace sifma = 1 if date == mdy(12,24,2003)
replace sifma = 1 if date == mdy(12,25,2003)
replace sifma = 1 if date == mdy(12,31,2003)

replace sifma = 1 if date == mdy(1,1,2004)
replace sifma = 1 if date == mdy(1,16,2004)
replace sifma = 1 if date == mdy(1,19,2004)
replace sifma = 1 if date == mdy(2,13,2004)
replace sifma = 1 if date == mdy(2,16,2004)
replace sifma = 1 if date == mdy(4,8,2004)
replace sifma = 1 if date == mdy(4,9,2004)
replace sifma = 1 if date == mdy(5,28,2004)
replace sifma = 1 if date == mdy(5,31,2004)
replace sifma = 1 if date == mdy(7,2,2004)
replace sifma = 1 if date == mdy(7,5,2004)
replace sifma = 1 if date == mdy(9,3,2004)
replace sifma = 1 if date == mdy(9,6,2004)
replace sifma = 1 if date == mdy(10,8,2004)
replace sifma = 1 if date == mdy(10,11,2004)
replace sifma = 1 if date == mdy(11,11,2004)
replace sifma = 1 if date == mdy(11,24,2004)
replace sifma = 1 if date == mdy(11,25,2004)
replace sifma = 1 if date == mdy(11,26,2004)
replace sifma = 1 if date == mdy(12,23,2004)
replace sifma = 1 if date == mdy(12,24,2004)
replace sifma = 1 if date == mdy(12,31,2004)

replace sifma = 1 if date == mdy(1,14,2005)
replace sifma = 1 if date == mdy(1,17,2005)
replace sifma = 1 if date == mdy(2,18,2005)
replace sifma = 1 if date == mdy(2,21,2005)
replace sifma = 1 if date == mdy(3,24,2005)
replace sifma = 1 if date == mdy(3,25,2005)
replace sifma = 1 if date == mdy(5,27,2005)
replace sifma = 1 if date == mdy(5,30,2005)
replace sifma = 1 if date == mdy(7,1,2005)
replace sifma = 1 if date == mdy(7,4,2005)
replace sifma = 1 if date == mdy(9,2,2005)
replace sifma = 1 if date == mdy(9,5,2005)
replace sifma = 1 if date == mdy(10,7,2005)
replace sifma = 1 if date == mdy(10,10,2005)
replace sifma = 1 if date == mdy(11,10,2005)
replace sifma = 1 if date == mdy(11,11,2005)
replace sifma = 1 if date == mdy(11,23,2005)
replace sifma = 1 if date == mdy(11,24,2005)
replace sifma = 1 if date == mdy(11,25,2005)
replace sifma = 1 if date == mdy(12,23,2005)
replace sifma = 1 if date == mdy(12,26,2005)
replace sifma = 1 if date == mdy(12,30,2005)

replace sifma = 1 if date == mdy(1,2,2006)
replace sifma = 1 if date == mdy(1,13,2006)
replace sifma = 1 if date == mdy(1,16,2006)
replace sifma = 1 if date == mdy(2,17,2006)
replace sifma = 1 if date == mdy(2,20,2006)
replace sifma = 1 if date == mdy(4,13,2006)
replace sifma = 1 if date == mdy(4,14,2006)
replace sifma = 1 if date == mdy(5,26,2006)
replace sifma = 1 if date == mdy(5,29,2006)
replace sifma = 1 if date == mdy(7,3,2006)
replace sifma = 1 if date == mdy(7,4,2006)
replace sifma = 1 if date == mdy(9,1,2006)
replace sifma = 1 if date == mdy(9,4,2006)
replace sifma = 1 if date == mdy(10,6,2006)
replace sifma = 1 if date == mdy(10,9,2006)
replace sifma = 1 if date == mdy(11,22,2006)
replace sifma = 1 if date == mdy(11,23,2006)
replace sifma = 1 if date == mdy(11,24,2006)
replace sifma = 1 if date == mdy(12,22,2006)
replace sifma = 1 if date == mdy(12,25,2006)
replace sifma = 1 if date == mdy(12,29,2006)

replace dr = 10 if dr == 0 & sifma == 1

* tabulate results and export
egen drtag = tag(dr cusip_id)

tab dr, matcell(n) matrow(names)
tab dr if drtag, matcell(ncusip)
putexcel A1=("Drop Stage") B1=("CUSIPs") C1=("Trades") ///
  A2=matrix(names) B2=matrix(ncusip) C2=matrix(n) using ///
  "/projects/trace.proj/output/05_final_cleaning_results.xlsx", replace


drop if dr > 0
drop sifma relvol rdate dr drtag
drop gid gsize gagency chid agg_chid chorder dupes_acp_id
save "/projects/trace.proj/data/05_clean_historical.dta", replace
clear
exit

proc import 
  datafile="c:\temp\train_data.xlsx" 
  dbms=xlsx 
  out=work.train
  replace;
run;
proc import 
  datafile="c:\temp\score_data.xlsx" 
  dbms=xlsx 
  out=work.score
  replace;
run;


%let n_bin = 100;
/* when we can use SAS procedures */
proc rank data=train groups=&n_bin. out=train_new;
var f1 f2;
ranks d_f1 d_f2;
run;
proc rank data=score groups=&n_bin. out=score_new;
var f1 f2;
ranks d_f1 d_f2;
run;
/* In the SAPHANA SQL, the SQL codes of EFD are:
select *, floor((row_number() over (order by f1))/(count(*) over ())/&n_bin.) as d_f1 */


/* This is the NB macro without using SAS procedure except SQL (working/...)
the original codes are from "https://www.lexjansen.com/nesug/nesug04/po/po09.pdf"
*/
%macro NB(train=,score=,nclass=,target=,inputs=);
%let error=0;
	%if %length(&train) = 0 %then %do;
	%put ERROR: Value for macro parameter TRAIN is missing ;
	%let error=1;
	%end;

%if %length(&score) = 0 %then %do;
	%put ERROR: Value for macro parameter SCORE is missing ;
	%let error=1;
	%end;

%if %length(&nclass) = 0 %then %do;
	%put ERROR: Value for macro parameter NCLASS is missing ;
	%let error=1;
	%end;

%if %length(&target) = 0 %then %do;
	%put ERROR: Value for macro parameter TARGET is missing ;
	%let error=1;
	%end;

%if %length(&inputs) = 0 %then %do;
	%put ERROR: Value for macro parameter INPUTS is missing ;
	%let error=1;
	%end;

%if &error=1 %then %goto finish;
	%if %sysfunc(exist(&train)) = 0 %then %do;
	%put ERROR: data set &train does not exist ;
	%let error=1;
	%end;

%if %sysfunc(exist(&score)) = 0 %then %do;
	%put ERROR: data set &score does not exist ;
	%let error=1;
	%end;

%if &error=1 %then %goto finish;
	%LET nvar=0;
		%do %while (%length(%scan(&inputs,&nvar+1))>0);
			%LET nvar=%eval(&nvar+1);
		%end;

		proc freq data=&train noprint;
			tables &target / out=_priors_ ;
		run;

		%do k=1 %to &nclass;
		proc sql noprint;
			select percent, count into :Prior&k, :Count&k
			from _priors_
			where &target=&k;
		quit;
		%end k;

%do i=1 %to &nvar;
	%LET var=%scan(&inputs,&i);
		%do j=1 %to &nclass;
		proc freq data=&train noprint;
			tables &var / out=_&var.&j (drop=count) missing;
			where &target=&j;
		run;
		%end j;

	data _&var ;
		merge 
			%do k=1 %to &nclass;
				_&var.&k (rename=(percent=percent&k))
			%end; ;
	by &var;
		%do k=1 %to &nclass; if percent&k=. then percent&k=0; %end;
	run;
		
	proc sql;
	create table &score AS
	select a.*
	%do k=1 %to &nclass;
		, b.percent&k as percent&K._&var
	%end;
	from &score as a left join _&var as b
	on a.&var=b.&var;
	quit;
%end i;

data &score (drop=L product maxprob
	%do i=1 %to &nclass; percent&i._: %end;);
	set &score;
	maxprob=0;
	%do k=1 %to &nclass;
		array vars&k (&Nvar)
			%do i=1 %to &nvar; percent&K._%scan(&inputs,&i) %end; ;
		product=log(&&Prior&k);
		do L=1 to &nvar;
		if vars&k(L)>0 then product=product+log(vars&k(L)); else
			product=product+log(0.5)-log(&&count&k);
		end;
		if product>maxprob then do; maxprob=product; _class_=&k; end;
	%end k;
run;
%finish: ;
%mend NB;
%NB(train=train_new,score=score_new,nclass=8,target=class,inputs=d_f1 d_f2);
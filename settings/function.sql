drop table if exists sys.functions cascade;

create table if not exists sys.functions
(
	language varchar(50) default '',
	function_name varchar(255) default '',
	parameter_list text default '',
	return_type varchar(50) default '',
	declares text default '',
	function_body text default ''
);

--address_number [left(15) of numbers]
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'address_number' as function_name,
	'address varchar, return_length integer default 15' as parameter_list,
	'varchar' as return_type,
	'return case when (regexp_match(trim(address), ''^\d+\w* |^\w+[ ]?-[ ]?\w+ |^\d+''))[1] is not null then left(trim((regexp_match(trim(address), ''^\d+\w* |^\w+[ ]?-[ ]?\w+ |^\d+''))[1]), return_length) else '''' end;' as function_body;

--address_street [left(35) after removing numbers in front, initcap]
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'address_street' as function_name,
	'address varchar, return_length integer default 35' as parameter_list,
	'varchar' as return_type,
	'return case when replace(trim(address), trim((regexp_match(trim(address), ''^\d+\w* |^\w+[ ]?-[ ]?\w+ |^\d+''))[1]), '''') is not null then initcap(left(trim(replace(trim(address), trim((regexp_match(trim(address), ''^\d+\w* |^\w+[ ]?-[ ]?\w+ |^\d+''))[1]), '''')), return_length)) else '''' end;' as function_body;

--country
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'country' as function_name,
	'state_province varchar' as parameter_list,
	'varchar' as return_type,
	'return case when state_province in (''AB'', ''BC'', ''MB'', ''NB'', ''NL'', ''NT'', ''NS'', ''NU'', ''ON'', ''PE'', ''QC'', ''SK'', ''YT'') then ''CA'' when state_province in (''AL'', ''AK'', ''AS'', ''AZ'', ''AR'', ''CA'', ''CO'', ''CT'', ''DE'', ''DC'', ''FM'', ''FL'', ''GA'', ''GU'', ''HI'', ''ID'', ''IL'', ''IN'', ''IA'', ''KS'', ''KY'', ''LA'', ''ME'', ''MH'', ''MD'', ''MA'', ''MI'', ''MN'', ''MS'', ''MO'', ''MT'', ''NE'', ''NV'', ''NH'', ''NJ'', ''NM'', ''NY'',''NC'', ''ND'', ''MP'', ''OH'', ''OK'', ''OR'', ''PW'', ''PA'', ''PR'', ''RI'', ''SC'', ''SD'', ''TN'', ''TX'', ''UT'', ''VT'', ''VI'', ''VA'',''WA'', ''WV'', ''WI'', ''WY'') then ''US'' else '''' end;' as function_body;

--email
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'email' as function_name,
	'email_address varchar, return_length integer default 320' as parameter_list,
	'varchar' as return_type,
	'return case when (regexp_match(trim(email_address), ''@''))[1] is not null then left(lower(trim(email_address)), return_length) else '''' end;' as function_body;

--first day of the month
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'first_day_of_the_month' as function_name,
	'date_var date' as parameter_list,
	'date' as return_type,
	'return cast(date_var as date) - make_interval(days => cast(date_part(''day'', cast(date_var as date)) as integer) - 1);' as function_body;

--fuzzy_search
/*insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'fuzzy_search' as function_name,
	'string varchar' as parameter_list,
	'varchar' as return_type,
	'return case when  is not null then  else '''' end;' as function_body;*/

--id code
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'id_code' as function_name,
	'code_value varchar, return_length integer' as parameter_list,
	'varchar' as return_type,
	'return case when left(regexp_replace(upper(trim(code_value)), ''\W+'', ''_'', ''g''), return_length) != ''_'' then left(regexp_replace(upper(trim(code_value)), ''\W+'', ''_'', ''g''), return_length) else '''' end;' as function_body;

--last day of the month
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'last_day_of_the_month' as function_name,
	'date_var date' as parameter_list,
	'date' as return_type,
	'return cast(date_var as date) - make_interval(days => cast(date_part(''day'', cast(date_var as date)) as integer)) + interval ''1 month'';' as function_body;

--number conversion
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'number_conversion' as function_name,
	'int_value bigint, return_length integer default 5' as parameter_list,
	'varchar' as return_type,
	'return left(cast(upper(to_hex(int_value)) as varchar), return_length);' as function_body;

--phone
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'phone' as function_name,
	'phone_number varchar' as parameter_list,
	'varchar' as return_type,
	'return case when regexp_replace(split_part(regexp_replace(lower(phone_number), ''[^\de]'', '''', ''g''), ''e'', 1), ''1(?=\d{10})'', '''') is not null and trim(phone_number) != '''' then left(lpad(regexp_replace(split_part(regexp_replace(lower(phone_number), ''[^\de]'', '''', ''g''), ''e'', 1), ''1(?=\d{10})'', ''''), 10, '' ''), 10) else '''' end;' as function_body;

--phone_extension
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'phone_extension' as function_name,
	'phone_number varchar, return_length integer default 10' as parameter_list,
	'varchar' as return_type,
	'return case when split_part(regexp_replace(lower(phone_number), ''[^\de]'', '''', ''g''), ''e'', 2) is not null and trim(phone_number) != '''' then left(split_part(regexp_replace(lower(phone_number), ''[^\de]'', '''', ''g''), ''e'', 2), return_length) else '''' end;' as function_body;

--split_first_name
/*insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'split_first_name' as function_name,
	'whole_name varchar, return_length integer default 50' as parameter_list,
	'varchar' as return_type,
	'return case when  is not null then  else '''' end;' as function_body;*/

--split_last_name
/*insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'split_last_name' as function_name,
	'whole_name varchar, return_length integer default 50' as parameter_list,
	'varchar' as return_type,
	'return case when  is not null then  else '''' end;' as function_body;*/

--zip [left(4 or 5) just digits] then [left(6), upper, no space]
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'zip' as function_name,
	'zip_code varchar' as parameter_list,
	'varchar' as return_type,
	'return case when (regexp_match(upper(regexp_replace(zip_code, ''[^a-zA-Z0-9]'', '''', ''g'')), ''^\d{4,5}''))[1] is not null then (regexp_match(upper(regexp_replace(zip_code, ''[^a-zA-Z0-9]'', '''', ''g'')), ''^\d{4,5}''))[1] when (regexp_match(upper(regexp_replace(zip_code, ''[^a-zA-Z0-9]'', '''', ''g'')), ''^(?:[A-Z]|\d){1,6}''))[1] is not null then (regexp_match(upper(regexp_replace(zip_code, ''[^a-zA-Z0-9]'', '''', ''g'')), ''^(?:[A-Z]|\d){1,6}''))[1] else '''' end;' as function_body;

--zip_extended [right(4)]
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'zip_extended' as function_name,
	'zip_code varchar' as parameter_list,
	'varchar' as return_type,
	'return case when (regexp_match(upper(regexp_replace(zip_code, ''[^a-zA-Z0-9]'', '''', ''g'')), ''^\d{4,5}''))[1] is not null and left(regexp_replace(upper(regexp_replace(zip_code, ''[^a-zA-Z0-9]'', '''', ''g'')), ''^\d{4,5}'', ''''), 4) is not null then left(regexp_replace(upper(regexp_replace(zip_code, ''[^a-zA-Z0-9]'', '''', ''g'')), ''^\d{4,5}'', ''''), 4) else''''end;' as function_body;

--valid_date
insert into sys.functions (language, function_name, parameter_list, return_type, function_body)
select 'sql' as language,
	'valid_date' as function_name,
	'test_date varchar, extra_years integer default 0' as parameter_list,
	'date' as return_type,
	'return
	case
		/*null cases*/
		when test_date = ''0'' or test_date = '''' then null
		
		/*invalid year - null*/
		when cast(left(test_date, 4) as int) not between 1950 and extract(year from now()) + extra_years then null

		/*fix invalid month - set month to right 1 value or Jan if 0 or Dec and setting day if invalid too*/
		when cast(substring(test_date, 5, 2) as int) not between 1 and 12 then 
			cast(case
				when cast(substring(test_date, 5, 2) as int) = 0 then left(test_date, 4) || ''01'' || case when cast(right(test_date, 2) as int) = 0 then ''01'' when cast(right(test_date, 2) as int) > 31 then ''31'' else right(test_date, 2) end
				when cast(substring(test_date, 6, 1) as int) between 1 and 9 then left(test_date, 4) || ''0'' || substring(test_date, 6, 1) || case when cast(right(test_date, 2) as int) = 0 then ''01'' when cast(right(test_date, 2) as int) > 31 then ''28'' else right(test_date, 2) end
				else left(test_date, 4) || ''12'' || case when cast(right(test_date, 2) as int) = 0 then ''01'' when cast(right(test_date, 2) as int) > 31 then ''31'' else right(test_date, 2) end
			end as date)

		/*invalid month with 31 days - if 0 then 1 else 31*/
		when cast(substring(test_date, 5, 2) as int) in (1, 3, 5, 7, 8, 10, 12) and cast(right(test_date, 2) as int) not between 1 and 31 then cast(case when cast(right(test_date, 2) as int) = 0 then left(test_date, 6) || ''01'' else left(test_date, 6) || ''31'' end as date)

		/*invalid month with 30 days - if 0 then 1 else 30*/
		when cast(substring(test_date, 5, 2) as int) in (4, 6, 9, 11) and cast(right(test_date, 2) as int) not between 1 and 30 then cast(case when cast(right(test_date, 2) as int) = 0 then left(test_date, 6) || ''01'' else left(test_date, 6) || ''30'' end as date)

		/*february leap year - if 0 then 1 else 29*/
		when cast(substring(test_date, 5, 2) as int) = 2 and cast(right(test_date, 2) as int) not between 1 and 29 and cast(left(test_date, 4) as int) % 4 = 0 and (cast(left(test_date, 4) as int) % 100 != 0 or cast(left(test_date, 4) as int) % 400 = 0) then cast(case when cast(right(test_date, 2) as int) = 0 then left(test_date, 6) || ''01'' else left(test_date, 6) || ''29'' end as date)

		/*february - if 0 then 1 else 28*/
		when cast(substring(test_date, 5, 2) as int) = 2 and cast(right(test_date, 2) as int) not between 1 and 28 then cast(case when cast(right(test_date, 2) as int) = 0 then left(test_date, 6) || ''01'' else left(test_date, 6) || ''28'' end as date)

		else cast(test_date as date)
	end;' as function_body;
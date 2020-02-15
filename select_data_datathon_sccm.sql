-- create Team8 needing tables
-- Part 1. demographic information
-- Part 2. urine output initial information
-- Part 3. creatinine initial information
-- Part 4. develop four decision tree models - using data
-- Part 5. kidgo - predict results (roc)
-- Part 6. EDA (kidgo aki four stages) - outcome (death/dialysis/dialysis_or_death) 



-- Part 1. final_cohort_demographic_info
with study_cohort as (
select *
from `physionet-data.mimiciii_derived.icustay_detail` icud 
where admission_age >= 18
and admission_age < 90
and first_icu_stay = true
and first_hosp_stay = true
and hadm_id in (SELECT distinct hadm_id
FROM `physionet-data.mimiciii_derived.elixhauser_quan`
where renal_failure = 0)
and DATETIME_DIFF(outtime, intime, hour) >= 4
)

, rrt_info_0 as (
select rrt.*, ROW_NUMBER() OVER (PARTITION BY rrt.icustay_id ORDER BY rrt.charttime) as rn
, DATETIME_DIFF(rrt.charttime, icud.intime, hour) as charttime_hour
from `physionet-data.mimiciii_derived.pivoted_rrt` rrt 
inner join `physionet-data.mimiciii_derived.icustay_detail` icud
on rrt.icustay_id = icud.icustay_id
)

, rrt_info as (
	select *
	from rrt_info_0
	where rn = 1
)

, del_cohort as (
	select *
	from rrt_info
	where rn = 1
	and charttime_hour <= 0
)

, mortality_info as (
  select sc.icustay_id
  , case 
  when ad.deathtime <= ad.dischtime then 1
  else 0 end as death_hosp
  from `physionet-data.mimiciii_clinical.admissions` ad 
  inner join study_cohort sc 
  on ad.hadm_id = sc.hadm_id
)

, outcome_info as (
	select sc.icustay_id
	, case when sc.icustay_id = ri.icustay_id then 1 else 0 end as dialysis_flag
	, case when sc.icustay_id = ri.icustay_id then ri.charttime_hour end as dialysis_starttime
	, mi.death_hosp
	from study_cohort sc
	left join rrt_info ri 
	on sc.icustay_id = ri.icustay_id
	left join mortality_info mi 
	on mi.icustay_id = sc.icustay_id 
	where sc.icustay_id not in (select icustay_id from del_cohort)
)

, urine_cohort_0 as (
	select uo.icustay_id, DATETIME_DIFF(uo.charttime, sc.intime, hour) AS los_icu_hour
	, uo.value as urineout
	from `physionet-data.mimiciii_derived.urineoutput` uo 
	inner join study_cohort sc 
	on uo.icustay_id = sc.icustay_id
	where uo.value > 0
)

, urine_cohort_1 as (
	select icustay_id, los_icu_hour, sum(urineout) as urineout
	from urine_cohort_0
	group by icustay_id, los_icu_hour
)

, urine_cohort_2 as (
	select icustay_id, count(*) as num 
	from urine_cohort_1
	where los_icu_hour > -24
	group by icustay_id
)

, urine_cohort as (
	select distinct icustay_id
	from urine_cohort_1
	where los_icu_hour > 0
	and icustay_id in (select icustay_id from urine_cohort_2 where num >= 2)
)

, creatinine_cohort_0 as (
	select sc.icustay_id
	, DATETIME_DIFF(lab.charttime, sc.intime, hour) AS los_icu_hour
	, lab.creatinine
	from `physionet-data.mimiciii_derived.pivoted_lab` lab
	inner join study_cohort sc 
	on lab.hadm_id = sc.hadm_id
	where lab.creatinine > 0
)

, creatinine_cohort_1 as (
	select icustay_id, los_icu_hour, avg(creatinine) as creatinine
	from creatinine_cohort_0
	group by icustay_id, los_icu_hour
)

, creatinine_cohort_2 as (
	select icustay_id, count(*) as num
	from creatinine_cohort_1
	group by icustay_id
)

, creatinine_cohort as (
	select distinct icustay_id
	from creatinine_cohort_1
	where los_icu_hour > 0
	and icustay_id in (select icustay_id from creatinine_cohort_2 where num >= 2)
)

, urine_creatinine_cohort as (
	select icustay_id
	from urine_cohort
	union all
	select icustay_id
	from creatinine_cohort
)


select oi.*
, sc.gender, sc.admission_age as age 
, sc.los_hospital as los_hospital_day
, sc.los_icu as los_icu_day
, sc.admission_type, sc.ethnicity
, sf.sofa 
, aps.apsiii
, ht.height 
, wt.weight
, case when wt.weight > 0 and ht.height > 0 then round((10000 * wt.weight/(ht.height * ht.height)),2) -- weight(kg)/height^2(m)
else null end as bmi
, ie.first_careunit
, sc.intime 
from outcome_info oi 
inner join study_cohort sc 
on oi.icustay_id = sc.icustay_id
left join `physionet-data.mimiciii_derived.sofa` sf 
on oi.icustay_id = sf.icustay_id
left join `physionet-data.mimiciii_derived.apsiii` aps 
on oi.icustay_id = aps.icustay_id
left join `physionet-data.mimiciii_derived.heightfirstday` ht 
on oi.icustay_id = ht.icustay_id
left join `physionet-data.mimiciii_derived.weightfirstday` wt 
on oi.icustay_id = wt.icustay_id
left join `physionet-data.mimiciii_clinical.icustays` ie
on oi.icustay_id = ie.icustay_id
where oi.icustay_id in (select distinct icustay_id from urine_creatinine_cohort)
order by oi.icustay_id;



-- create tables of creatinine and urine information
-- Part 2. final_cohort_urine_info
with study_cohort as (
	select icustay_id, intime
	from `sccm-datathon.team8.final_cohort_demographic_info` 
)

, urine_info_0 as (
	select uo.icustay_id, uo.charttime, sc.intime
	, DATETIME_DIFF(uo.charttime, sc.intime, hour) AS los_icu_hour
	, uo.value as urineout
	from `physionet-data.mimiciii_derived.urineoutput` uo 
	inner join study_cohort sc 
	on uo.icustay_id = sc.icustay_id
	where uo.value > 0
)

, urine_info as (
	select icustay_id, los_icu_hour, sum(urineout) as urineout
	from urine_info_0
	group by icustay_id, los_icu_hour
)

, urine_baseline_0 as (
	select *, ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY los_icu_hour) as rn
	from urine_info
)

, urine_baseline as (
	select icustay_id, urineout as urineout_baseline
	from urine_baseline_0
	where rn = 1
)

select ui.*, uib.urineout_baseline, round(ui.urineout/uib.urineout_baseline,2) as urine_ratio
from urine_info ui 
inner join urine_baseline uib 
on ui.icustay_id = uib.icustay_id
order by icustay_id, los_icu_hour;


-- Part 3. final_cohort_creatinine_info
with study_cohort as (
	select fc.icustay_id, fc.intime, icud.subject_id, icud.hadm_id
	from `sccm-datathon.team8.final_cohort_demographic_info` fc 
	inner join `physionet-data.mimiciii_derived.icustay_detail` icud 
	on fc.icustay_id = icud.icustay_id
)

, creatinine_info_0 as (
	select sc.icustay_id
	, lab.charttime
	, sc.intime
	, DATETIME_DIFF(lab.charttime, sc.intime, hour) AS los_icu_hour
	, lab.creatinine
	from `physionet-data.mimiciii_derived.pivoted_lab` lab
	inner join study_cohort sc 
	on lab.hadm_id = sc.hadm_id
	where lab.creatinine > 0.01
	and lab.creatinine < 20
)

, creatinine_info as (
	select icustay_id, los_icu_hour, avg(creatinine) as creatinine
	from creatinine_info_0
	group by icustay_id, los_icu_hour
)

, creatinine_baseline_0 as (
	select *, ROW_NUMBER() OVER (PARTITION BY icustay_id ORDER BY los_icu_hour) as rn
	from creatinine_info
)

, creatinine_baseline as (
	select icustay_id, creatinine as creatinine_baseline
	from creatinine_baseline_0
	where rn = 1
)


select ci.*, cib.creatinine_baseline, round(ci.creatinine/cib.creatinine_baseline,2) as creatinine_ratio
from creatinine_info ci 
inner join creatinine_baseline cib 
on ci.icustay_id = cib.icustay_id
order by icustay_id, los_icu_hour;



-- Part 4. final_creatinine_uo_treemodel_results
-- we consider 2d and 7d windows
with first_2d_creatinine_0 as (
	SELECT icustay_id
	, creatinine
	, (creatinine - creatinine_baseline) as creatinine_change
	, creatinine_ratio
	FROM `sccm-datathon.team8.final_cohort_creatinine_info` 
	where los_icu_hour <= 24*2
)

, first_2d_creatinine as (
	SELECT icustay_id
	, max(creatinine) as creatinine_max_2d
	, max(creatinine_change) as creatinine_change_2d
	, max(creatinine_ratio) as creatinine_ratio_2d
	from first_2d_creatinine_0
	group by icustay_id
)

, first_7d_creatinine_0 as (
	SELECT icustay_id
	, creatinine
	, (creatinine - creatinine_baseline) as creatinine_change
	, creatinine_ratio
	FROM `sccm-datathon.team8.final_cohort_creatinine_info` 
	where los_icu_hour <= 24*7
)

, first_7d_creatinine as (
	SELECT icustay_id
	, max(creatinine) as creatinine_max_7d
	, max(creatinine_change) as creatinine_change_7d
	, max(creatinine_ratio) as creatinine_ratio_7d
	from first_7d_creatinine_0
	group by icustay_id
)

-- calculate 2d and 7d urine output
, weight_info as (
	select icustay_id
	, case when weight is null then 79 
	when weight > 310 or weight < 20 then 79 
	else weight end as weight
	from `sccm-datathon.team8.final_cohort_demographic_info`
)

, first_1d_uo as (
	SELECT fc.icustay_id
	, sum(fc.urineout) as uo_1d
	FROM `sccm-datathon.team8.final_cohort_urine_info` fc
	inner join weight_info wi
	on fc.icustay_id = wi.icustay_id 
	where fc.los_icu_hour <= 24
	group by fc.icustay_id
)

, first_2d_uo as (
	SELECT fc.icustay_id
	, sum(fc.urineout) as uo_2d
	FROM `sccm-datathon.team8.final_cohort_urine_info` fc
	inner join weight_info wi
	on fc.icustay_id = wi.icustay_id 
	where fc.los_icu_hour > 24
	and fc.los_icu_hour <= 24*2
	group by fc.icustay_id
)

, first_3d_uo as (
	SELECT fc.icustay_id
	, sum(fc.urineout) as uo_3d
	FROM `sccm-datathon.team8.final_cohort_urine_info` fc
	inner join weight_info wi
	on fc.icustay_id = wi.icustay_id 
	where fc.los_icu_hour > 24*2
	and fc.los_icu_hour <= 24*3
	group by fc.icustay_id
)

, first_4d_uo as (
	SELECT fc.icustay_id
	, sum(fc.urineout) as uo_4d
	FROM `sccm-datathon.team8.final_cohort_urine_info` fc
	inner join weight_info wi
	on fc.icustay_id = wi.icustay_id 
	where fc.los_icu_hour > 24*3
	and fc.los_icu_hour <= 24*4
	group by fc.icustay_id
)

, first_5d_uo as (
	SELECT fc.icustay_id
	, sum(fc.urineout) as uo_5d
	FROM `sccm-datathon.team8.final_cohort_urine_info` fc
	inner join weight_info wi
	on fc.icustay_id = wi.icustay_id 
	where fc.los_icu_hour > 24*4
	and fc.los_icu_hour <= 24*5
	group by fc.icustay_id
)

, first_6d_uo as (
	SELECT fc.icustay_id
	, sum(fc.urineout) as uo_6d
	FROM `sccm-datathon.team8.final_cohort_urine_info` fc
	inner join weight_info wi
	on fc.icustay_id = wi.icustay_id 
	where fc.los_icu_hour > 24*5
	and fc.los_icu_hour <= 24*6
	group by fc.icustay_id
)

, first_7d_uo as (
	SELECT fc.icustay_id
	, sum(fc.urineout) as uo_7d
	FROM `sccm-datathon.team8.final_cohort_urine_info` fc
	inner join weight_info wi
	on fc.icustay_id = wi.icustay_id 
	where fc.los_icu_hour > 24*6
	and fc.los_icu_hour <= 24*7
	group by fc.icustay_id
)

, first_2d_urine_statistic_0 as (
	select f1.icustay_id, f1.uo_1d/(24*wi.weight) as uo 
	from first_1d_uo f1 
	inner join weight_info wi 
	on f1.icustay_id = wi.icustay_id
	union all 
	select f2.icustay_id, f2.uo_2d/(24*wi.weight) as uo 
	from first_2d_uo f2 
	inner join weight_info wi 
	on f2.icustay_id = wi.icustay_id	
)

, first_2d_urine_statistic as (
	select icustay_id, min(uo) as urine_2d_min
	from first_2d_urine_statistic_0
	group by icustay_id
)

, first_7d_urine_statistic_0 as (
	select f1.icustay_id, f1.uo_1d/(24*wi.weight) as uo 
	from first_1d_uo f1
	inner join weight_info wi 
	on f1.icustay_id = wi.icustay_id

	union all 
	select f2.icustay_id, f2.uo_2d/(24*wi.weight) as uo 
	from first_2d_uo f2 
	inner join weight_info wi 
	on f2.icustay_id = wi.icustay_id

	union all
	select f3.icustay_id, f3.uo_3d/(24*wi.weight) as uo 
	from first_3d_uo f3
	inner join weight_info wi 
	on f3.icustay_id = wi.icustay_id

	union all 
	select f4.icustay_id, f4.uo_4d/(24*wi.weight) as uo 
	from first_4d_uo f4
	inner join weight_info wi 
	on f4.icustay_id = wi.icustay_id

	union all
	select f5.icustay_id, f5.uo_5d/(24*wi.weight) as uo 
	from first_5d_uo f5
	inner join weight_info wi 
	on f5.icustay_id = wi.icustay_id

	union all 
	select f6.icustay_id, f6.uo_6d/(24*wi.weight) as uo 
	from first_6d_uo f6 
	inner join weight_info wi 
	on f6.icustay_id = wi.icustay_id

	union all
	select f7.icustay_id, f7.uo_7d/(24*wi.weight) as uo 
	from first_7d_uo f7
	inner join weight_info wi 
	on f7.icustay_id = wi.icustay_id
)

, first_7d_urine_statistic as (
	select icustay_id, min(uo) as urine_7d_min
	from first_7d_urine_statistic_0
	group by icustay_id
)


SELECT fc.icustay_id, fc.dialysis_flag, fc.death_hosp
, case when fc.dialysis_flag = 0 and fc.death_hosp = 0 then 0 
else 1 end as dialysis_or_death
, fc.gender, fc.age, fc.los_hospital_day, fc.los_icu_day
, f2.creatinine_max_2d, f2.creatinine_change_2d, f2.creatinine_ratio_2d
, f7.creatinine_max_7d, f7.creatinine_change_7d, f7.creatinine_ratio_7d
, u2.urine_2d_min, u7.urine_7d_min
FROM `sccm-datathon.team8.final_cohort_demographic_info` fc 
left join first_2d_creatinine f2 
on fc.icustay_id = f2.icustay_id
left join first_7d_creatinine f7 
on fc.icustay_id = f7.icustay_id
left join first_2d_urine_statistic u2 
on fc.icustay_id = u2.icustay_id
left join first_7d_urine_statistic u7 
on fc.icustay_id = u7.icustay_id
order by fc.icustay_id;



-- Part 5. kidgo - predict results
-- kidgo_roc_results
with kidgo_2d_result as (
	SELECT icustay_id
	, case when aki_stage_48hr is null then 0
	else aki_stage_48hr end as kidgo_2d
	FROM `physionet-data.mimiciii_derived.kdigo_stages_48hr`
)

, kidgo_7d_result as (
	SELECT icustay_id
	, case when aki_stage_7day is null then 0
	else aki_stage_7day end as kidgo_7d
	FROM `physionet-data.mimiciii_derived.kdigo_stages_7day`
)

select fc.icustay_id, fc.dialysis_flag, fc.death_hosp
, case when fc.dialysis_flag = 0 and fc.death_hosp = 0 then 0 else 1 end as dialysis_or_death
, k2.kidgo_2d
, k7.kidgo_7d
from `sccm-datathon.team8.final_cohort_demographic_info` fc
left join kidgo_2d_result k2 
on fc.icustay_id = k2.icustay_id
left join kidgo_7d_result k7 
on fc.icustay_id = k7.icustay_id
order by fc.icustay_id;


-- Part 6. EDA (kidgo aki four stages) - outcome (death/dialysis/dialysis_or_death) 
-- just will change to the decision tree model's train dataset
-- kidgo_histogram 
-- first 7 days
with aa as (
select 0 as aki, count(*)/3544 as death_7d_num
from `sccm-datathon.team8.kidgo_roc_results`
where dialysis_or_death = 1 and kidgo_7d = 0
and kidgo_7d is not null
union all 
select 1 as aki, count(*)/3544 as death_7d_num
from `sccm-datathon.team8.kidgo_roc_results`
where dialysis_or_death = 1 and kidgo_7d = 1
and kidgo_7d is not null
union all 
select 2 as aki, count(*)/3544 as death_7d_num
from `sccm-datathon.team8.kidgo_roc_results`
where dialysis_or_death = 1 and kidgo_7d = 2
and kidgo_7d is not null
union all 
select 3 as aki, count(*)/3544 as death_7d_num
from `sccm-datathon.team8.kidgo_roc_results`
where dialysis_or_death = 1 and kidgo_7d = 3
and kidgo_7d is not null
)

select *
from aa
order by aki;

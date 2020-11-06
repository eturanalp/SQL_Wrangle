Received postgres dump file from HsLynk. Rstored it into POstgres local using PgAdmin. There are 4 schemas 2014,2015,2016,2017 for HMIS data.
There is Survey schema for survey data. Extracted HUD CSV compliant(version 5.1 from https://hudhdx.info/VendorResources.aspx ) csv files using SQL below.

TODO:
1. Deal with first row column names case
2. Deal with null values in integer type columns (e.g. ProjectType) being NULL in the csv file (HuD sample has ""). Done with integer type.
3. SSN decryption and optionally SHA-1. Done:Cleartext.
4. v2014.enrollment.Householdid is null for all records
5. CREATED TABLE clientidentify for importing csv file from hslynk containing clear dobssn. Merge them with client table.
6. Housing_inventory.household_membership has 498 rows. v2014\v2017.hmis_household_member tables are empty.
7. v2014.domesticviolence.datacollectionstage and v2014.domesticviolence.informationdate can not be null! May need update incomeandsources query:Done. Assumed datacollectionstage=1 for NULLs.
8. There are 10K enrollments with either information_date or datacollectionstage is equal to NULL
9. v2016\v2017.client_veteran_info is empty
10. Forgot to inclide v2016.entryssvf fields to Enrollment.csv  : Done
11. Forgot to include v2016.housingassessmentdisposition and v2016.exithousingassessment in Exit.csv (see v2017 sql)
12. Used HUD CSV spec 5.1 for all 4 schemas.


--SELECT id, source_system_id, deleted FROM v2014.enrollment
--WHERE id='0a2c29f5-3108-4dfe-bc65-a8d173856ab8'
--ORDER BY id DESC LIMIT 100


--select * from information_schema.columns where column_name like '%prev%street%'

--Use this for UUID type fields:
--replace(CAST(id AS Text),'-','')

/* Type ID Definition
Date D A date in the format yyyy-mm-dd
Datetime T A date and time in the format yyyy-mm-dd hh:mm:ss1
Integer I A non-negative whole number
Money M Number with two decimal places (no commas and no currency symbol); numbers
may be negative
Money M+ Non-negative number with two decimal places (no commas and no currency symbol)
String S# A combination of letters, numbers, and standard punctuation (see list of characters
permitted in string fields below); the number following the ‘S’ identifies the
maximum number of characters permitted for a given field. For example, fields with
a data type of S50 are limited to 50 characters. String fields must be padded with
double-quotes. */ 

SELECT 
replace(CAST(id AS Text),'-','') OrganizationID, 
left(organizationname,50) OrganizationName, 
left(organizationcommonname,50) OrganizationCommonName,
date_created DateCreated , 
date_updated DateUpdated ,
replace(CAST(user_id AS Text),'-','') UserID,
NULL DateDeleted,
'20200309' ExportID 
FROM v2017.organization


SELECT 
replace(CAST(id AS Text),'-','') ProjectID, 
replace(CAST(organizationid AS Text),'-','') OrganizationID,  --all records are NULL in v2016
left(projectname,50) ProjectName, 
left(projectcommonname,50) ProjectCommonName,
CAST(CAST(continuumproject AS Text) AS integer) ContinuumProject,
CAST(CAST(projecttype AS Text) AS integer) ProjectType,
'' ResidentialAffiliation,
CAST(CAST(trackingmethod AS Text) AS integer) TrackingMethod,
CAST(CAST(targetpopulation AS Text) AS integer) TargetPopulation,
'' PITCount,
date_created DateCreated , 
date_updated DateUpdated ,
replace(CAST(user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.project


SELECT DISTINCT ON (client.id)
replace(CAST(client.id AS Text),'-','') PersonalID , 
left(first_name,50) FirstName, 
left(middle_name,50) MiddleName,
left(last_name,50) LastName,
left(name_suffix,50) NameSuffix,
CAST(CAST(name_data_quality AS Text) AS integer) NameDataQuality,
clientidentify.socsecno SSN, 
CAST(CAST(ssn_data_quality AS Text) AS integer) SSNDataQuality,
to_char(clientidentify.dob,'yyyy-mm-dd') dob, 
CAST(CAST(dob_data_quality AS Text) AS integer)  DOBDataQuality,
CASE WHEN race::Text::integer=1 THEN 1 
     WHEN race::Text::integer>7 THEN 99
	 ELSE 0 END AmIndAKNative,
CASE WHEN race::Text::integer=2 THEN 1 
     WHEN race::Text::integer>7 THEN 99
	 ELSE 0 END Asian,
CASE WHEN race::Text::integer=3 THEN 1 
     WHEN race::Text::integer>7 THEN 99
	 ELSE 0 END BlackAfAmerican,
CASE WHEN race::Text::integer=4 THEN 1 
     WHEN race::Text::integer>7 THEN 99
	 ELSE 0 END NativeHIOtherPacific,
CASE WHEN race::Text::integer=5 THEN 1 
     WHEN race::Text::integer>7 THEN 99
	 ELSE 0 END White,
CASE WHEN race::Text::integer>7 THEN race::Text::integer
     ELSE NULL
	 END RaceNone,
ethnicity::Text::integer Ethnicity,
gender::Text::integer Gender,
veteran_status::Text::integer VeteranStatus,
year_entrd_service YearEnteredService,
year_seperated YearSeparated,
world_war_2::Text::integer WorldWarII,
korean_war::Text::integer KoreanWar,
vietnam_war::Text::integer VietnamWar,
desert_storm::Text::integer DesertStorm,
afghanistan_oef::Text::integer AfghanistanOEF,
iraq_oif::Text::integer IraqOIF,
iraq_ond::Text::integer IraqOND,
other_theater::Text::integer OtherTheater,
military_branch::Text::integer MilitaryBranch,
discharge_status::Text::integer DischargeStatus,
to_char(client.date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(client.date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(client.user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.client
LEFT JOIN v2017.clientidentify USING(id)
LEFT JOIN v2017.client_veteran_info ON client_veteran_info.client_id=client.id --empty in v2016
WHERE client_veteran_info.id IS NULL OR client_veteran_info.id::Text NOT IN ( --eliminate dupicate veteran records
  SELECT  max(client_veteran_info.id::Text) veteraninfo_id
  FROM v2017.client
  LEFT JOIN v2017.client_veteran_info ON client_id=client.id
  GROUP BY client.id
  HAVING count(client_veteran_info.id)>1 
  )
  ORDER BY client.id,client.date_created DESC


SELECT 
replace(CAST(id AS Text),'-','') ProjectEntryID ,
replace(CAST(client_id AS Text),'-','') PersonalID ,
replace(CAST(projectid AS Text),'-','') ProjectID ,
to_char(entrydate,'YYYY-MM-DD') EntryDate,
replace(CAST(householdid AS Text),'-','') HouseholdID,
relationshiptohoh::Text::integer RelationshipToHoH,
livingsituation::Text::integer ResidencePrior,
lengthofstay::Text::integer ResidencePriorLengthOfStay,
'' LOSUnderThreshold,  --v2016, v2017
'' PreviousStreetESSH,
'' DateToStreetESSH,
timeshomelesspastthreeyears::Text::integer TimesHomelessPastThreeYears,
monthshomelesspastthreeyears::Text::integer MonthsHomelessPastThreeYears,
disablingcondition::Text::integer DisablingCondition,
'' HousingStatus,
maxdateofengagement::Date DateOfEngagement,  -- Lookup from DateOfEngagement Table
maxresidentialmoveindate::Date ResidentialMoveInDate,  -- Lookup from ResidentialMoveInDate Table
date_of_status::Date DateOfPATHStatus,
client_enrolled_in_path::Text::integer ClientEnrolledInPATH,
reason_not_enrolled ReasonNotEnrolled,
'' WorstHousingSituation,  --v2016.entryrhsp, v2017.entryrhsp
percentami::Text::integer PercentAMI,  -- lookup from v2015.entryssvf
left(last_permanent_street,100) LastPermanentStreet,
left(last_permanent_city,50) LastPermanentCity,
left(last_permanent_state,2) LastPermanentState,
left(last_permanent_zip,5) LastPermanentZIP,
address_data_quality::Text::integer AddressDataQuality,
'' DateOfBCPStatus,   --v2015.rhybcp_status empty
'' FYSBYouth,
'' ReasonNoServices,
sexual_orientation::Text::integer SexualOrientation, -- lookup from v2015.sexualorientation table, v2017.entryrhy
formerly_ward_child_welfr_forest_care::Text::integer FormerWardChildWelfare, --lookup from v2014.formarwardChildWelfare, v2017.entryrhy
years_child_welfr_forest_care::Text::integer ChildWelfareYears,  -- same as above
months_child_welfr_forest_care::Text::integer  ChildWelfareMonths, -- same as above
formerly_ward_of_juvenile_justice::Text::integer FormerWardJuvenileJustice, --lookup from v2014.formarwardJuvenileJustice, v2017.entryrhy
years_juvenile_justice::Text::integer JuvenileJusticeYears,  -- same as above
'' JuvenileJusticeMonths,  -- v2016.entryrhy.months_juvinilejustice
'' HouseholdDynamics, -- lookup from v2014.YouthCriticalIssues table, v2017.entryrhy
'' SexualOrientationGenderIDYouth,  --same as above
'' SexualOrientationGenderIDFam,
'' HousingIssuesYouth,
'' HousingIssuesFam,
'' SchoolEducationalIssuesYouth,
'' SchoolEducationalIssuesFam,
'' UnemploymentYouth,
unemployement_family_mbr::Text::integer UnemploymentFam,
'' MentalHealthIssuesYouth,
'' MentalHealthIssuesFam,
'' HealthIssuesYouth,
'' HealthIssuesFam,
'' PhysicalDisabilityYouth,
physical_disability_family_mbr::Text::integer PhysicalDisabilityFam,
'' MentalDisabilityYouth,
'' MentalDisabilityFam,
'' AbuseAndNeglectYouth,
'' AbuseAndNeglectFam,
'' AlcoholDrugAbuseYouth,
alcohol_drug_abuse_family_mbr::Text::integer AlcoholDrugAbuseFam,
insufficient_income_to_support_youth::Text::integer InsufficientIncome,
'' ActiveMilitaryParent,
incarcerated_parent::Text::integer IncarceratedParent,
'' IncarceratedParentStatus,  -- same as above
referral_source::Text::integer ReferralSource,
count_out_reach_referral_approaches::Text::integer CountOutreachReferralApproaches,
'' ExchangeForSex,  --looked up from commercialsexualexploitation table, v2017.entryrhy
'' ExchangeForSexPastThreeMonths,
'' CountOfExchangeForSex,
'' WorkPlaceViolenceThreats, -- v2016.entryrhy, v2017.exitrhy, v2015.entryrhy
'' WorkplacePromiseDifference, 
'' CoercedToContinueWork,
'' LaborExploitPastThreeMonths,
urgent_referral::Text::integer UrgentReferral,
timetohousingloss::Text::integer TimeToHousingLoss,  --v2016, v2017
zeroincome::Text::integer ZeroIncome,
annualpercentami::Text::integer AnnualPercentAMI,
financialchange::Text::integer FinancialChange,  -- v2016.entryrhy, v2017.entryrhy
householdchange::Text::integer HouseholdChange, 
evictionhistory::Text::integer EvictionHistory,
subsidyatrisk::Text::integer SubsidyAtRisk,
literalhomelesshistory::Text::integer LiteralHomelessHistory,
disablehoh::Text::integer DisabledHoH,
criminalrecord::Text::integer CriminalRecord,
sexoffender::Text::integer SexOffender,
dependendunder6::Text::integer DependentUnder6,
singleparent::Text::integer SingleParent,
hh5plus::Text::integer HH5Plus,
iraqafghanistan::Text::integer IraqAfghanistan,
femvet::Text::integer FemVet,
hp_screen_score HPScreeningScore,
thresholdscore ThresholdScore,
left(vamc_staction,8) VAMCStation,
'' ERVisits,  -- v2016.entryrhy
'' JailNights,
'' HospitalNights,
to_char(date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.enrollment
LEFT JOIN (SELECT enrollmentid, MAX(dateofengagement) maxdateofengagement FROM v2017.dateofengagement GROUP BY enrollmentid) doe ON doe.enrollmentid=enrollment.id
LEFT JOIN (SELECT enrollmentid, MAX(moveindate) maxresidentialmoveindate FROM v2017.moveindate GROUP BY enrollmentid) rmid ON rmid.enrollmentid=enrollment.id
LEFT JOIN (select distinct on (enrollmentid) enrollmentid,date_of_status,client_enrolled_in_path,reason_not_enrolled
           from v2017.path_status
           order by enrollmentid, date_created desc) path ON path.enrollmentid=enrollment.id
LEFT JOIN (select distinct on (enrollmentid) enrollmentid,percentami,last_permanent_street,last_permanent_city,last_permanent_state,last_permanent_zip,address_data_quality,urgent_referral,timetohousingloss,zeroincome,annualpercentami,financialchange,householdchange,evictionhistory,subsidyatrisk,literalhomelesshistory,disablehoh,criminalrecord,sexoffender,dependendunder6,singleparent,hh5plus,iraqafghanistan,femvet,thresholdscore,hp_screen_score,vamc_staction
            from v2017.entryssvf
            order by enrollmentid, date_created desc) lpa ON lpa.enrollmentid=enrollment.id
LEFT JOIN (select distinct on (enrollmentid) enrollmentid,sexual_orientation,formerly_ward_child_welfr_forest_care,years_child_welfr_forest_care,months_child_welfr_forest_care,formerly_ward_of_juvenile_justice,years_juvenile_justice,unemployement_family_mbr,mental_health_issues_family_mbrily_mbr,physical_disability_family_mbr,mental_health_issues_family_mbrily_mbr,alcohol_drug_abuse_family_mbr,insufficient_income_to_support_youth,incarcerated_parent,referral_source,count_out_reach_referral_approaches
		   from v2017.entryrhy
           order by enrollmentid, date_created desc) fwcw ON fwcw.enrollmentid=enrollment.id




SELECT 
replace(CAST(id AS Text),'-','') FunderID, 
replace(CAST(projectid AS Text),'-','') ProjectID,
funder::Text::integer Funder,
replace(CAST(grantid AS Text),'-','') GrantID,
to_char(startdate,'yyyy-mm-dd hh:mm:ss') StartDate , 
to_char(enddate,'yyyy-mm-dd hh:mm:ss') EndDate ,
to_char(date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.funder


SELECT    ---empty in v2017
replace(CAST(id AS Text),'-','') ProjectCoCID, 
replace(CAST(projectid AS Text),'-','') ProjectID ,
left(coccode,6) CoCCode, 
to_char(date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.coc

SELECT 
replace(CAST(inventory.id AS Text),'-','') InventoryID, 
replace(CAST(coc_id AS Text),'-','') ProjectID ,
left(coc.coccode,6) CoCCode, 
to_char(informationdate,'yyyy-mm-dd hh:mm:ss') InformationDate,
householdtype::Text::Integer HouseholdType ,
bedtype::Text::Integer BedType,
availabilty::Text::Integer Availability,
unitinventory UnitInventory ,
bed_inventory BedInventory,
ch_bed_inventory CHBedInventory,
vet_bed_inventory VetBedInventory,
youth_bed_inventory YouthBedInventory ,
'' YouthAgeGroup ,
to_char(inventorystartdate,'yyyy-mm-dd') InventoryStartDate,
to_char(inventoryenddate,'yyyy-mm-dd') InventoryEndDate,
hmisparticipatingbeds HMISParticipatingBeds,
to_char(inventory.date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(inventory.date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(inventory.user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.inventory
LEFT JOIN v2017.coc ON coc.projectid=inventory.coc_id

SELECT 
replace(CAST(geography.id AS Text),'-','') SiteID, 
replace(CAST(coc_id AS Text),'-','') ProjectID ,
left(coc.coccode,6) CoCCode, 
'' PrincipalSite,
left(geo_code::Text,6) Geocode,
left(address1,100) Address,
left(city,50) City,
left(state::Text,2) State,
left(zip,5) ZIP,
to_char(geography.date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(geography.date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(geography.user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.geography
LEFT JOIN v2017.coc ON coc.projectid=geography.coc_id

SELECT 
replace(CAST(id AS Text),'-','') AffliationID, 
replace(CAST(projectid AS Text),'-','') ProjectID,
replace(CAST(resprojectid AS Text),'-','') ResProjectID,
to_char(date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.affiliation;   -- Empty in v2015


SELECT DISTINCT ON (enrollment_coc.id)
replace(CAST(enrollment_coc.id AS Text),'-','') EnrollmentCoCID, 
replace(CAST(enrollment_coc.enrollmentid AS Text),'-','') ProjectEntryID,
coalesce(replace(CAST(enrollment.householdid AS Text),'-',''),'') HouseholdID, --NULL because no data
replace(CAST(coc.projectid AS Text),'-','') ProjectID,
replace(CAST(enrollment.client_id AS Text),'-','') PersonalID ,
to_char(information_date,'yyyy-mm-dd') InformationDate ,
left(coc.coccode,6) CoCCode,
coalesce(datacollectionstage::Text::integer,1) DataCollectionStage,
to_char(enrollment_coc.date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(enrollment_coc.date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(enrollment_coc.user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.enrollment_coc
LEFT JOIN v2017.enrollment ON enrollment_coc.enrollmentid=enrollment.id
LEFT JOIN v2017.coc ON enrollment.projectid=coc.projectid
WHERE coc.projectid IS NOT NULL


SELECT DISTINCT ON (ProjectEntryID) *
FROM(
SELECT 
replace(CAST(exit.id AS Text),'-','') ExitID, 
replace(CAST(exit.enrollmentid AS Text),'-','') ProjectEntryID,
coalesce(replace(CAST(enrollment.client_id AS Text),'-',''),'') PersonalID ,
to_char(exitdate,'yyyy-mm-dd') ExitDate ,
destination::Text::integer Destination,
coalesce(left(otherdestination,50),'') OtherDestination ,
assessmentdisposition::Text::integer AssessmentDisposition,  --v2014.housingassessmentdisposition empty
left(otherdisposition,50) OtherDisposition ,
housingassessment::Text::integer HousingAssessment , --v2014.exithousingassessment empty
subsidyinformation::Text::integer SubsidyInformation , --v2014.exithousingassessment empty
'' WrittenAftercarePlan,
'' AssistanceMainstreamBenefits,
'' PermanentHousingPlacement,
'' TemporaryShelterPlacement,
'' ExitCounseling,
'' FurtherFollowUpServices ,
'' ScheduledFollowUpContacts,
'' ResourcePackage ,
'' OtherAftercarePlanOrAction,
project_completion_status::Text::integer ProjectCompletionStatus ,  -- from v2017.exitrhy
early_exit_reason::Text::integer EarlyExitReason ,
'' FamilyReunificationAchieved , 
to_char(exit.date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(exit.date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(exit.user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.exit
LEFT JOIN v2017.enrollment ON exit.enrollmentid=enrollment.id
LEFT JOIN v2017.exitrhy ON exitrhy.exitid=exit.id
LEFT JOIN v2017.exithousingassessment ON exit.id=exithousingassessment.exitid
LEFT JOIN v2017.housingassessmentdisposition ON exit.id=housingassessmentdisposition.exitid	
) ExitAll
ORDER BY ProjectEntryID, DateCreated desc



SELECT DISTINCT ON (incomeandsources.enrollmentid,noncashbenefits.enrollmentid,healthinsurance.enrollmentid,medicalassistance.enrollmentid, incomeandsources.datacollectionstage,noncashbenefits.datacollectionstage,healthinsurance.datacollectionstage,medicalassistance.datacollectionstage)
replace(CAST(coalesce(incomeandsources.id,noncashbenefits.id,healthinsurance.id,medicalassistance.id) AS Text),'-','') IncomeBenefitsID, 
replace(CAST(coalesce(incomeandsources.enrollmentid,noncashbenefits.enrollmentid,healthinsurance.enrollmentid,medicalassistance.enrollmentid) AS Text),'-','') ProjectEntryID,
replace(CAST(enrollment.client_id AS Text),'-','') PersonalID ,
to_char(coalesce(incomeandsources.information_date,noncashbenefits.information_date,healthinsurance.information_date,medicalassistance.information_date),'yyyy-mm-dd') InformationDate ,
--enrollment.entrydate,
incomefromanysource::Text::integer IncomeFromAnySource,
to_char(totalmonthlyincome,'9999990.99') TotalMonthlyIncome ,
earned::Text::integer Earned ,
to_char(earnedamount,'9999990.99') EarnedAmount ,
unemployment::Text::integer Unemployment,
to_char(unemploymentamount,'9999990.99') UnemploymentAmount,
ssi::Text::integer SSI,
to_char(ssiamount,'9999990.99') SSIAmount,
ssdi::Text::integer SSDI,
to_char(ssdiamount,'9999990.99') SSDIAmount ,
vadisabilityservice::Text::integer VADisabilityService ,
to_char(vadisabilityserviceamount,'9999990.99') VADisabilityServiceAmount,
vadisabilitynonservice::Text::integer VADisabilityNonService,
to_char(vadisabilitynonserviceamount,'9999990.99') VADisabilityNonServiceAmount,
privatedisability::Text::integer PrivateDisability,
to_char(privatedisabilityamount,'9999990.99') PrivateDisabilityAmount,
workerscomp::Text::integer WorkersComp,
to_char(workerscompamount,'9999990.99') WorkersCompAmount,
tanf::Text::integer TANF,
to_char(tanfamount,'9999990.99') TANFAmount,
ga::Text::integer GA,
to_char(gaamount,'9999990.99') GAAmount,
socsecretirement::Text::integer SocSecRetirement ,
to_char(socsecretirementamount,'9999990.99') SocSecRetirementAmount,
pension::Text::integer Pension,
to_char(pensionamount,'9999990.99') PensionAmount,
childsupport::Text::integer ChildSupport,
to_char(childsupportamount,'9999990.99') ChildSupportAmount,
alimony::Text::integer Alimony ,
to_char(alimonyamount,'9999990.99') AlimonyAmount,
incomeandsources.othersource::Text::integer OtherIncomeSource,
to_char(othersourceamount,'9999990.99') OtherIncomeAmount,
left(incomeandsources.othersourceidentify,50) OtherIncomeSourceIdentify ,
benefitsfromanysource::Text::integer BenefitsFromAnySource ,
snap::Text::integer SNAP,
wic::Text::integer WIC ,
tanfchildcare::Text::integer TANFChildCare,
tanftransportation::Text::integer TANFTransportation,
othertanf::Text::integer OtherTANF,
rentalassistanceongoing::Text::integer RentalAssistanceOngoing,
rentalassistancetemp::Text::integer RentalAssistanceTemp,
noncashbenefits.othersource::Text::integer OtherBenefitsSource,
left(noncashbenefits.othersourceidentify,50) OtherBenefitsSourceIdentify ,
insurancefromanysource::Text::integer InsuranceFromAnySource,
medicaid::Text::integer Medicaid ,
nomedicaidreason::Text::integer NoMedicaidReason ,
medicare::Text::integer Medicare,
nomedicarereason::Text::integer NoMedicareReason,
schip::Text::integer SCHIP,
noschipreason::Text::integer NoSCHIPReason,
vamedicalservices::Text::integer VAMedicalServices,
novamedreason::Text::integer NoVAMedReason,
employerprovided::Text::integer EmployerProvided,
noemployerprovidedreason::Text::integer NoEmployerProvidedReason,
cobra::Text::integer COBRA,
nocobrareason::Text::integer NoCOBRAReason,
privatepay::Text::integer PrivatePay,
noprivatepayreason::Text::integer NoPrivatePayReason,
statehealthinadults::Text::integer StateHealthIns,
nostatehealthinsreason::Text::integer NoStateHealthInsReason,
indianhealthservices::Text::integer IndianHealthServices, 
noindianhealthservicesreason::Text::integer NoIndianHealthServicesReason,
other_insurance::Text::integer OtherInsurance,  --v2016.healthinsurance, v2017.healthinsurance
left(other_insurance_identify,50) OtherInsuranceIdentify,
hivaidsassistance HIVAIDSAssistance , --V2014-v2017.medialassistance
nohivaidsassistancereason NoHIVAIDSAssistanceReason,
adap ADAP,
noadapreason NoADAPReason,
connection_with_soar::Text::integer ConnectionWithSOAR,  --v2017.connectionwithsoar
coalesce(incomeandsources.datacollectionstage::Text::integer,noncashbenefits.datacollectionstage::Text::integer,healthinsurance.datacollectionstage::Text::integer,medicalassistance.datacollectionstage::Text::integer) DataCollectionStage,
coalesce(to_char(incomeandsources.date_created,'yyyy-mm-dd hh:mm:ss'),to_char(noncashbenefits.date_created,'yyyy-mm-dd hh:mm:ss'),to_char(healthinsurance.date_created,'yyyy-mm-dd hh:mm:ss'),to_char(medicalassistance.date_created,'yyyy-mm-dd hh:mm:ss')) DateCreated , 
coalesce(to_char(incomeandsources.date_updated,'yyyy-mm-dd hh:mm:ss'),to_char(noncashbenefits.date_updated,'yyyy-mm-dd hh:mm:ss'),to_char(healthinsurance.date_updated,'yyyy-mm-dd hh:mm:ss'),to_char(medicalassistance.date_updated,'yyyy-mm-dd hh:mm:ss')) DateUpdated ,
coalesce(replace(CAST(incomeandsources.user_id AS Text),'-',''),replace(CAST(noncashbenefits.user_id AS Text),'-',''),replace(CAST(healthinsurance.user_id AS Text),'-',''),replace(CAST(medicalassistance.user_id AS Text),'-','')) UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.incomeandsources
FULL OUTER JOIN v2017.noncashbenefits 
     ON incomeandsources.enrollmentid=noncashbenefits.enrollmentid AND 
        incomeandsources.datacollectionstage=noncashbenefits.datacollectionstage  AND
	    incomeandsources.information_date::date=noncashbenefits.information_date::date 
--		incomeandsources.date_created::date=noncashbenefits.date_created::date
FULL OUTER JOIN v2017.healthinsurance
     ON incomeandsources.enrollmentid=healthinsurance.enrollmentid AND 
        incomeandsources.datacollectionstage=healthinsurance.datacollectionstage  AND
	    incomeandsources.information_date::date=healthinsurance.information_date::date 
FULL OUTER JOIN v2017.medicalassistance
     ON incomeandsources.enrollmentid=medicalassistance.enrollmentid AND 
        incomeandsources.datacollectionstage=medicalassistance.datacollectionstage  AND
	    incomeandsources.information_date::date=medicalassistance.information_date::date 
JOIN v2017.enrollment ON incomeandsources.enrollmentid=enrollment.id OR noncashbenefits.enrollmentid=enrollment.id  OR healthinsurance.enrollmentid=enrollment.id OR enrollment.id=medicalassistance.enrollmentid
LEFT JOIN v2017.connectionwithsoar ON connectionwithsoar.enrollmentid=enrollment.id
WHERE (incomeandsources.enrollmentid IS NOT NULL OR healthinsurance.enrollmentid IS NOT NULL OR noncashbenefits.enrollmentid IS NOT NULL OR medicalassistance.enrollmentid IS NOT NULL) AND
      (incomeandsources.datacollectionstage IS NOT NULL OR healthinsurance.datacollectionstage IS NOT NULL OR noncashbenefits.datacollectionstage IS NOT NULL OR medicalassistance.datacollectionstage IS NOT NULL) AND --datacollectionstage can not be null as per HMIS_CSV_Specifications_5_1.pdf page 29
      (incomeandsources.information_date IS NOT NULL OR healthinsurance.information_date IS NOT NULL OR noncashbenefits.information_date IS NOT NULL OR medicalassistance.information_date IS NOT NULL) --information_date can not be null as per HMIS_CSV_Specifications_5_1.pdf page 29
ORDER BY incomeandsources.enrollmentid,noncashbenefits.enrollmentid,healthinsurance.enrollmentid,medicalassistance.enrollmentid,incomeandsources.datacollectionstage,noncashbenefits.datacollectionstage,healthinsurance.datacollectionstage,medicalassistance.datacollectionstage,incomeandsources.information_date DESC, noncashbenefits.information_date DESC,healthinsurance.information_date DESC,medicalassistance.information_date DESC




--CREATE extension tablefunc;

-- SELECT *   --health_status_crosstab for categories of mental, dental and general health
-- FROM v2014.health_status 
-- LEFT JOIN (
--   SELECT * 
--   FROM crosstab('
--   SELECT enrollmentid, datacollectionstage::Text::integer,information_date, health_category::Text::integer , health_status::Text::integer
--   FROM v2014.health_status
--   ORDER BY 1','VALUES (27), (28), (29), (30)')
--   AS ct(enrollmentid uuid, datacollectionstage integer,information_date timestamp, GeneralHealthStatus integer,DentalHealthStatus integer, MentalHealthStatus integer, PregnancyStatus integer)
--   ) health_status_ct
-- ON health_status.enrollmentid=health_status_ct.enrollmentid AND health_status.datacollectionstage::Text::integer=health_status_ct.datacollectionstage AND health_status.information_date=health_status_ct.information_date

--SELECT * FROM  v2014.health_status
--WHERE enrollmentid='00be27bc-1055-4c9f-bcde-d0ce756d2ae5'

--CREATE extension tablefunc;
-- SELECT *   --health_status_crosstab for categories of mental, dental and general health
-- FROM v2014.health_status 
-- LEFT JOIN (
--   SELECT * 
--   FROM crosstab('
--   SELECT enrollmentid, datacollectionstage::Text::integer,information_date, health_category::Text::integer , health_status::Text::integer
--   FROM v2014.health_status
--   ORDER BY 1','VALUES (27), (28), (29), (30)')
--   AS ct(enrollmentid uuid, datacollectionstage integer,information_date timestamp, GeneralHealthStatus integer,DentalHealthStatus integer, MentalHealthStatus integer, PregnancyStatus integer)
--   ) health_status_ct
-- ON health_status.enrollmentid=health_status_ct.enrollmentid AND health_status.datacollectionstage::Text::integer=health_status_ct.datacollectionstage AND health_status.information_date=health_status_ct.information_date

--SELECT * FROM  v2014.health_status
--WHERE enrollmentid='00be27bc-1055-4c9f-bcde-d0ce756d2ae5'

SELECT DISTINCT ON (domesticviolence.enrollmentid,healthstatus.enrollmentid, domesticviolence.datacollectionstage,healthstatus.datacollectionstage)
replace(CAST(coalesce(domesticviolence.id,healthstatus.id) AS Text),'-','') HealthAndDVID, 
replace(CAST(coalesce(domesticviolence.enrollmentid,healthstatus.enrollmentid) AS Text),'-','') ProjectEntryID,
replace(CAST(enrollment.client_id AS Text),'-','') PersonalID ,
to_char(coalesce(domesticviolence.information_date,healthstatus.information_date),'yyyy-mm-dd') InformationDate ,
domesticviolencevictim::Text::integer DomesticViolenceVictim,
whenoccurred::Text::integer WhenOccurred ,
currently_fleeing::Text::integer CurrentlyFleeing, --v2015-v2017.domesticviolence.currently_fleeing
healthstatus.GeneralHealthStatus::Text::integer GeneralHealthStatus,
healthstatus.DentalHealthStatus::Text::integer DentalHealthStatus,
healthstatus.MentalHealthStatus::Text::integer MentalHealthStatus,
healthstatus.PregnancyStatus::Text::integer PregnancyStatus ,
to_char(healthstatus.due_date,'yyyy-mm-dd') DueDate,
coalesce(domesticviolence.datacollectionstage::Text::integer,healthstatus.datacollectionstage::Text::integer) DataCollectionStage,
coalesce(to_char(domesticviolence.date_created,'yyyy-mm-dd hh:mm:ss'),to_char(healthstatus.date_created,'yyyy-mm-dd hh:mm:ss')) DateCreated , 
coalesce(to_char(domesticviolence.date_updated,'yyyy-mm-dd hh:mm:ss'),to_char(healthstatus.date_updated,'yyyy-mm-dd hh:mm:ss')) DateUpdated ,
coalesce(replace(CAST(domesticviolence.user_id AS Text),'-',''),replace(CAST(healthstatus.user_id AS Text),'-','')) UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.domesticviolence
FULL OUTER JOIN 
(SELECT *   --health_status_crosstab for categories of mental, dental and general health
FROM v2017.health_status 
LEFT JOIN (
  SELECT * 
  FROM crosstab('
  SELECT enrollmentid, datacollectionstage::Text::integer,information_date, health_category::Text::integer , health_status::Text::integer
  FROM v2015.health_status
  ORDER BY 1','VALUES (27), (28), (29), (30)')
  AS ct(eid uuid, datacollectionstage1 integer,information_date1 timestamp, GeneralHealthStatus integer,DentalHealthStatus integer, MentalHealthStatus integer, PregnancyStatus integer)
  ) health_status_ct
ON health_status.enrollmentid=health_status_ct.eid AND health_status.datacollectionstage::Text::integer=health_status_ct.datacollectionstage1 AND health_status.information_date=health_status_ct.information_date1
) healthstatus
 
     ON domesticviolence.enrollmentid=healthstatus.enrollmentid AND 
        domesticviolence.datacollectionstage=healthstatus.datacollectionstage  AND
	    domesticviolence.information_date::date=healthstatus.information_date::date 
JOIN v2017.enrollment ON domesticviolence.enrollmentid=enrollment.id OR healthstatus.enrollmentid=enrollment.id 
WHERE (domesticviolence.enrollmentid IS NOT NULL OR healthstatus.enrollmentid IS NOT NULL) AND
      (domesticviolence.datacollectionstage IS NOT NULL OR healthstatus.datacollectionstage IS NOT NULL) AND --datacollectionstage can not be null as per HMIS_CSV_Specifications_5_1.pdf page 29
      (domesticviolence.information_date IS NOT NULL OR healthstatus.information_date IS NOT NULL) --information_date can not be null as per HMIS_CSV_Specifications_5_1.pdf page 29
ORDER BY domesticviolence.enrollmentid,healthstatus.enrollmentid,domesticviolence.datacollectionstage,healthstatus.datacollectionstage,domesticviolence.information_date DESC, healthstatus.information_date DESC


SELECT DISTINCT ON (employment.enrollmentid,education.enrollmentid, employment.datacollectionstage,education.datacollectionstage)
replace(CAST(coalesce(employment.id,education.id) AS Text),'-','') IncomeBenefitsID, 
replace(CAST(coalesce(employment.enrollmentid,education.enrollmentid) AS Text),'-','') ProjectEntryID,
replace(CAST(enrollment.client_id AS Text),'-','') PersonalID ,
to_char(coalesce(employment.information_date,education.information_date,employment.date_created,education.date_created),'yyyy-mm-dd') InformationDate ,
lastgradecompleted::Text::integer LastGradeCompleted,
school_status::Text::integer SchoolStatus,
employed::Text::integer Employed ,
employment_type::Text::integer EmploymentType ,
not_employed_reason::Text::integer NotEmployedReason ,
coalesce(employment.datacollectionstage::Text::integer,education.datacollectionstage::Text::integer,1) DataCollectionStage,
coalesce(to_char(employment.date_created,'yyyy-mm-dd hh:mm:ss'),to_char(education.date_created,'yyyy-mm-dd hh:mm:ss')) DateCreated , 
coalesce(to_char(employment.date_updated,'yyyy-mm-dd hh:mm:ss'),to_char(education.date_updated,'yyyy-mm-dd hh:mm:ss')) DateUpdated ,
coalesce(replace(CAST(employment.user_id AS Text),'-',''),replace(CAST(education.user_id AS Text),'-','')) UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.employment  
FULL OUTER JOIN v2017.education   --v2015.schoolstatus is empty
     ON employment.enrollmentid=education.enrollmentid AND 
        employment.datacollectionstage=education.datacollectionstage  AND
	    employment.information_date::date=education.information_date::date 
JOIN v2017.enrollment ON employment.enrollmentid=enrollment.id OR education.enrollmentid=enrollment.id 
WHERE (employment.enrollmentid IS NOT NULL OR education.enrollmentid IS NOT NULL)
--       (employment.datacollectionstage IS NOT NULL OR education.datacollectionstage IS NOT NULL) AND --datacollectionstage can not be null as per HMIS_CSV_Specifications_5_1.pdf page 29
--       (employment.information_date IS NOT NULL OR education.information_date IS NOT NULL ) --information_date can not be null as per HMIS_CSV_Specifications_5_1.pdf page 29
ORDER BY employment.enrollmentid,education.enrollmentid,employment.datacollectionstage,education.datacollectionstage,employment.information_date DESC,education.information_date DESC
 
   
SELECT DISTINCT ON (enrollmentid,disabilitytype,datacollectionstage)
replace(CAST(disabilities.id AS Text),'-','') DisabilitiesID, 
replace(CAST(enrollmentid AS Text),'-','') ProjectEntryID,
replace(CAST(client_id AS Text),'-','') PersonalID,
to_char(coalesce(information_date,disabilities.date_created),'yyyy-mm-dd') InformationDate ,
disabilitytype::Text::integer   DisabilityType ,
disabilityresponse::Text::integer DisabilityResponse,
indefiniteandimpairs::Text::integer::Text::integer::Text::integer::Text::integer IndefiniteAndImpairs,
'' DocumentationOnFile,
'' ReceivingServices,
'' PATHHowConfirmed ,
'' PATHSMIInformation,
tcellcountavailable::Text::integer TCellCountAvailable,  --v2015-v2017.disabilities
tcellcount::Text::integer TCellCount,
tcellcountsource::Text::integer TCellSource,
viral_load_available::Text::integer ViralLoadAvailable,
viral_load::Text::integer ViralLoad ,
viral_load_source::Text::integer ViralLoadSource ,
coalesce(datacollectionstage::Text::integer,1) DataCollectionStage,
to_char(disabilities.date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(disabilities.date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(disabilities.user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.disabilities
JOIN v2017.enrollment ON disabilities.enrollmentid=enrollment.id
ORDER BY enrollmentid,disabilitytype,datacollectionstage, information_date DESC;

SELECT 
replace(CAST(service_fa_referral.id AS Text),'-','') ServicesID, 
replace(CAST(enrollmentid AS Text),'-','') ProjectEntryID,
replace(CAST(client_id AS Text),'-','') PersonalID,
to_char(dateprovided,'yyyy-mm-dd') DateProvided ,
record_type::Text::integer   RecordType ,
type_provided::Text::integer TypeProvided ,
left(other_type_provided,50) OtherTypeProvided ,
sub_type_provided::Text::integer SubTypeProvided ,
to_char(fa_amount,'9999990.99') FAAmount ,
referral_outcome::Text::integer ReferralOutcome ,
to_char(service_fa_referral.date_created,'yyyy-mm-dd hh:mm:ss') DateCreated , 
to_char(service_fa_referral.date_updated,'yyyy-mm-dd hh:mm:ss') DateUpdated ,
replace(CAST(service_fa_referral.user_id AS Text),'-','') UserID,
'' DateDeleted,
'20200309' ExportID 
FROM v2017.service_fa_referral
JOIN v2017.enrollment ON service_fa_referral.enrollmentid=enrollment.id




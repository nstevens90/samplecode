CREATE PROCEDURE [dbo].[DS_ACA_NewMember_Inputs_monthly]

@year int,
@month int

AS
BEGIN

DECLARE @periodKey int = (SELECT MAX((periodkey)) 
							FROM DM_AnalysisPeriods 
							WHERE Year(PeriodEnd) = @year 
								AND PeriodDesc NOT LIKE 'Trailing%')

EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Begin Process';
EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Create Tables';


--0.0: pre-processing
	--0.1: client member keys and table creation
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DS_Client_CloneACAKey]') AND TYPE IN (N'U'))
			BEGIN
			CREATE TABLE [dbo].DS_Client_CloneACAKey(
				[clientDSMemberKey] [int] IDENTITY(1,1) NOT NULL,
				[insuredMemberIdentifier] varchar(50) NOT NULL,
			) ON [PRIMARY]
			END;
			
		INSERT INTO DS_Client_CloneACAKey (insuredMemberIdentifier)
		SELECT DISTINCT a.insuredMemberIdentifier 
		FROM B_100_DerivedCoverage a
		LEFT JOIN DS_Client_CloneACAKey b
			ON a.insuredMemberIdentifier = b.insuredMemberIdentifier
		WHERE b.insuredMemberIdentifier IS NULL;
		 
	--0.2: create member attribute table 
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DS_NewMember_001_ACA_Info]') AND TYPE IN (N'U'))
			BEGIN
			CREATE TABLE [dbo].[DS_NewMember_001_ACA_Info](
				[clientDSMemberKey] [int] NOT NULL,
				[clientDB] [nvarchar](128) NULL,
				[metalLevel] [char](1) NOT NULL,
				[age] [tinyint] NOT NULL,
				[sex] [char](1) NOT NULL,
				[CSR_Indicator] [int] NULL,
				[ARF] [float] NULL,
				[AV] [float] NULL,
				[GCF] [float] NULL,
				[IDF] [float] NULL,
				[relationship] [varchar](10) NOT NULL,
				[Drug_MOOP] [float] NULL,
				[Drug_Deductible] [float] NULL,
				[ER_Coinsurance] [float] NULL,
				[ER_Copay] [float] NULL,
				[IP_Coinsurance] [float] NULL,
				[IP_Copay] [float] NULL,
				[Med_Deductible] [float] NULL,
				[Med_MOOP] [float] NULL,
				[OP_Coinsurance] [float] NULL,
				[OP_Copay] [float] NULL,
				[PCP_Coinsurance] [float] NULL,
				[PCP_Copay] [float] NULL,
				[Spec_Coinsurance] [float] NULL,
				[Spec_Copay] [float] NULL,
				[UrgentCare_Coinsurance] [float] NULL,
				[UrgentCare_Copay] [float] NULL,
				[pro_claim_count] [int] NOT NULL,
				[op_claim_count] [int] NOT NULL,
				[ip_claim_count] [int] NOT NULL,
				[active_year] [int] NOT NULL,
				[medClaimTotal] [money] NULL,
				[rxClaimTotal] [money] NULL,
				PRIMARY KEY CLUSTERED(
					[clientDSMemberKey] ASC,
					active_year ASC
				)
			) ON [PRIMARY]
			END;

		TRUNCATE TABLE DS_NewMember_001_ACA_Info
	--0.3: create captured conditions table
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DS_NewMember_002_capturedconditions]') AND TYPE IN (N'U'))
			BEGIN
			CREATE TABLE [dbo].[DS_NewMember_002_capturedconditions](
				[clientDSMemberKey] [int] NOT NULL,
				[CC] [varchar](50) NOT NULL,
				[instances] [int] NULL,
				[active_year] [int] NOT NULL,
				PRIMARY KEY CLUSTERED(
					[clientDSMemberKey] ASC,
					CC ASC,
					active_year ASC
				)
			) ON [PRIMARY]
			END

		TRUNCATE TABLE [DS_NewMember_002_capturedconditions]

	--0.4: create nonscored conditions table
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DS_NewMember_003_nonscoredconditions]') AND TYPE IN (N'U'))
			BEGIN
			CREATE TABLE [dbo].[DS_NewMember_003_nonscoredconditions](
				[clientDSMemberKey] [int] NOT NULL,
				[CC] [varchar](50) NOT NULL,
				[instances] [int] NULL,
				[active_year] [int] NOT NULL,
				PRIMARY KEY CLUSTERED(
					[clientDSMemberKey] ASC,
					CC ASC,
					active_year ASC
				)
			) ON [PRIMARY]
			END

		TRUNCATE TABLE [DS_NewMember_003_nonscoredconditions]

	--0.5: create drugs table
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DS_NewMember_005_drugs]') AND TYPE IN (N'U'))
			BEGIN
			CREATE TABLE [dbo].[DS_NewMember_005_drugs](
				[clientDSMemberKey] [int] NOT NULL,
				[drug_subclass] [nvarchar](255) NOT NULL,
				[instances] [int] NULL,
				[active_year] [int] NOT NULL,
				PRIMARY KEY CLUSTERED(
					[clientDSMemberKey] ASC,
					[drug_subclass] ASC,
					active_year ASC
				)
			) ON [PRIMARY]
			END

		TRUNCATE TABLE [DS_NewMember_005_drugs]

	--0.6: create procedures table
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DS_NewMember_004_procedures]') AND TYPE IN (N'U'))
			BEGIN
			CREATE TABLE [dbo].[DS_NewMember_004_procedures](
				[clientDSMemberKey] [int] NOT NULL,
				[class] [varchar](200) NOT NULL,
				[category] [varchar](50) NOT NULL,
				[instances] [int] NULL,
				[active_year] [int] NOT NULL,
				PRIMARY KEY CLUSTERED(
					[clientDSMemberKey] ASC,
					[class] ASC,
					[category] ASC,
					active_year ASC
			) ON [PRIMARY])
			END

		TRUNCATE TABLE [DS_NewMember_004_procedures]
		
	--0.7: create tpd county level
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DS_NewMember_006_county_tpd]') AND TYPE IN (N'U'))
			BEGIN
			CREATE TABLE [dbo].[DS_NewMember_006_county_tpd](
				[clientDSMemberKey] [int] NOT NULL,
				[metric] [varchar](200) NOT NULL,
				[value] [float] NOT NULL,
				PRIMARY KEY CLUSTERED(
					[clientDSMemberKey] ASC,
					[metric] ASC
				)
			) ON [PRIMARY]
			END

		TRUNCATE TABLE [DS_NewMember_006_county_tpd]
		

	--0.9: create clone output
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DS_Clone_007_output]') AND TYPE IN (N'U'))
			BEGIN
			CREATE TABLE [dbo].[DS_Clone_007_output](
				[clientDSMemberKey] [int] NOT NULL,
				[DropProbability] [float] NOT NULL,
				[Low_Risk] [float] NOT NULL,
				[Med_Risk] [float] NOT NULL,
				[High_Risk] [float] NOT NULL,
				[Feature_1_Risk] [varchar](300) NULL,
				[Feature_2_Risk] [varchar](300) NULL,
				[Feature_3_Risk] [varchar](300) NULL,
				[Feature_4_Risk] [varchar](300) NULL,
				[Feature_5_Risk] [varchar](300) NULL,
				[Low_Cost] [float] NOT NULL,
				[Med_Cost] [float] NOT NULL,
				[High_Cost] [float] NOT NULL,
				[Feature_1_Cost] [varchar](300) NULL,
				[Feature_2_Cost] [varchar](300) NULL,
				[Feature_3_Cost] [varchar](300) NULL,
				[Feature_4_Cost] [varchar](300) NULL,
				[Feature_5_Cost] [varchar](300) NULL,
				[Month] [varchar](2) NOT NULL,
				runDate DATETIME NOT NULL
				PRIMARY KEY (clientDSMemberKey, Month, runDate
				)
			) ON [PRIMARY]
			END

		
--1.0: pull all relevant member info
		--1.0.1: temp table of new members to inner join
		IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[#tmp_newmember]') AND TYPE IN (N'U'))
			DROP TABLE #tmp_newmember;

		--create table of all members within each periodkey to find new members for every year
		SELECT DISTINCT a.InsuredMemberIdentifier, YEAR(b.BenefitYearEnd) AS currentYear, 1 AS NewMember_CurrentYear,0 AS PreviousMember
		INTO #tmp_newmember
		FROM B_100_DerivedCoverage a
		INNER JOIN DM_AnalysisPeriods b
			ON a.PeriodKey = b.PeriodKey
		LEFT JOIN 
			(
			SELECT DISTINCT insuredMemberIdentifier,a.PeriodKey AS lastyear_PeriodKey, YEAR(b.BenefitYearEnd) AS LastYear
			FROM B_100_DerivedCoverage a
			INNER JOIN DM_AnalysisPeriods b 
				ON a.PeriodKey = b.PeriodKey
			WHERE b.MostRecentProcDateHistIndic = 1
			AND b.PeriodDesc NOT LIKE 'Trailing%'
				AND RiskAdjustedIndicator = 1
			) c
		ON c.InsuredMemberIdentifier = a.InsuredMemberIdentifier
			AND YEAR(b.BenefitYearEnd) = c.LastYear + 1
		WHERE a.RiskAdjustedIndicator = 1
			AND c.InsuredMemberIdentifier IS NULL
		;

		UPDATE a
		SET a.PreviousMember= 1
		FROM #tmp_newmember a
		INNER JOIN (
			SELECT DISTINCT insuredMemberIdentifier, a.PeriodKey AS lastyear_PeriodKey, year(b.BenefitYearEnd) AS LastYear
			FROM B_100_DerivedCoverage a
			INNER JOIN DM_AnalysisPeriods b 
				ON a.PeriodKey = b.PeriodKey
			WHERE b.MostRecentProcDateHistIndic = 1
			AND b.PeriodDesc NOT LIKE 'Trailing%'
				AND RiskAdjustedIndicator = 1
			) b
		ON a.InsuredMemberIdentifier = b.InsuredMemberIdentifier
		WHERE a.currentYear - b.LastYear >= 2

	--1.1: base member info
EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Populate ACA Info Table';
		IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[#tmp_claimTotals]') AND TYPE IN (N'U'))
			DROP TABLE #tmp_claimTotals;

			SELECT insuredMemberIdentifier, SUM(b.policyPaidAmount) AS medClaimTotal, YEAR(b.serviceToDate) AS active_year,
				MONTH(b.serviceToDate) AS month
			INTO #tmp_claimTotals
			FROM (
				SELECT DISTINCT g.serviceToDate, g.policyPaidAmount, h.insuredMemberIdentifier, h.claimIdentifier 
				FROM B_002_MedicalClaimDetail h
					INNER JOIN B_002a_MedicalClaimServiceLine g
						ON g.claimIdentifier = h.claimIdentifier
					WHERE YEAR(h.statementCoverFromDate) = @year
						AND MONTH(h.statementCoverFromDate) <= @month
				) b
			GROUP BY insuredMemberIdentifier, YEAR(b.serviceToDate)	, MONTH(b.serviceToDate)
			
			
	INSERT INTO DS_NewMember_001_ACA_Info (clientDSMemberKey, clientDB, metalLevel, age, sex,
			CSR_Indicator, ARF, AV, GCF, IDF, relationship, Drug_MOOP, Drug_Deductible, ER_Coinsurance,
			ER_Copay, IP_Coinsurance, IP_Copay, Med_Deductible, Med_MOOP, OP_Coinsurance, OP_Copay, PCP_Coinsurance, PCP_Copay, Spec_Coinsurance, Spec_Copay, UrgentCare_Coinsurance, UrgentCare_Copay, 
			pro_claim_count, op_claim_count, ip_claim_count, active_year, medClaimTotal, rxClaimTotal)
	SELECT DISTINCT clientDSMemberKey, db_name() AS clientDB, MAX(CAST(a.metalLevel AS CHAR)) metalLevel, MAX(CAST(A.AgeLast AS tinyint)) AS age,MAX(CAST(a.insuredMemberGenderCode AS char)) sex,
			MAX(CSR_Indicator) CSR_Indicator, MAX(CAST(ARF AS FLOAT)) ARF, MAX(CAST(AV AS FLOAT)) AV, MAX(CAST(a.GCF AS FLOAT)) GCF, MAX(CAST(IDF AS FLOAT)) IDF, MAX(CAST(c.relationship AS varchar)) relationship, MAX(CAST(Drug_MOOP AS FLOAT)) Drug_MOOP, MAX(CAST(Drug_Deductible AS FLOAT)) Drug_Deductible, MAX(CAST(ER_Coinsurance AS FLOAT)) ER_Coinsurance,
			MAX(CAST(ER_Copay AS FLOAT)) ER_Copay, MAX(CAST(IP_Coinsurance AS FLOAT)) IP_Coinsurance, MAX(CAST(IP_Copay AS FLOAT)) IP_Copay, MAX(CAST(Med_Deductible AS FLOAT)) Med_Deductible, MAX(CAST(Med_MOOP AS FLOAT)) Med_MOOP, MAX(CAST(OP_Coinsurance AS FLOAT)) OP_Coinsurance, MAX(OP_Copay) OP_Copay, MAX(CAST(PCP_Coinsurance AS FLOAT)) PCP_Coinsurance, MAX(CAST(PCP_Copay AS FLOAT)) PCP_Copay, MAX(CAST(Spec_Coinsurance AS FLOAT)) Spec_Coinsurance, MAX(CAST(Spec_Copay AS FLOAT)) Spec_Copay, MAX(CAST(UrgentCare_Coinsurance AS FLOAT)) UrgentCare_Coinsurance, MAX(CAST(UrgentCare_Copay AS FLOAT)) UrgentCare_Copay,
			MAX(ISNULL(pro_claim_count, 0)) AS pro_claim_count, MAX(ISNULL(op_claim_count, 0)) AS op_claim_count, MAX(ISNULL(ip_claim_count, 0)) AS ip_claim_count, 
			MAX(CAST(a.derivedCoverageYear AS smallint)) as active_year, 
			ISNULL(SUM(l.medClaimTotal),0) AS medClaimTotal, 
			ISNULL(k.rxClaimTotal,0) AS rxClaimTotal
		FROM dbo.B_100_DerivedCoverage a
		INNER JOIN DS_Client_CloneACAKey h
			ON h.insuredMemberIdentifier = a.InsuredMemberIdentifier
		INNER JOIN L_005_Plan b
			ON a.PlanKey = b.PlanKey
		INNER JOIN B_001_InsuredMemberProfile c
			ON a.origPolicyRecKey = c.recordIdentifier
		INNER JOIN DM_AnalysisPeriods d
			ON a.PeriodKey = d.PeriodKey
		INNER JOIN #tmp_newmember z
			ON z.InsuredMemberIdentifier = a.InsuredMemberIdentifier
			AND z.currentYear = @Year
		INNER JOIN A_001_Import_MemberInfo g
			ON a.InsuredMemberIdentifier = g.insuredMemberIdentifier
		LEFT JOIN ParetoReferenceDB..REF_County e
			ON g.mbrZipCode = e.ZIPCODE
		LEFT JOIN ParetoReferenceDB..CMS_2014_Rating_Area_County_Crosswalk x
			ON e.COUNTY_NM = x.County
			AND e.STATE_CD = x.State
		LEFT JOIN ParetoReferenceDB.dbo.RWJ_INDSG_PlanBenefits f
			ON ISNULL(NULLIF(g.RateAreaIdentifier,''), x.Rating_Area_Number) = RIGHT(f.AREA,1)
			AND b.InsurancePlan = F.PlanID_Formatted
			AND f.year = @Year
		--count of claims for feature
		LEFT JOIN (
			SELECT DISTINCT b.insuredMemberIdentifier, b.active_year, MAX(b.pro_claim_count) AS pro_claim_count,
				MAX(b.op_claim_count) AS op_claim_count, MAX(b.ip_claim_count) AS ip_claim_count
			FROM(
				SELECT DISTINCT a.insuredMemberIdentifier, YEAR(statementCoverFromDate) AS active_year,
					CASE WHEN claimType = 'P' THEN COUNT(DISTINCT claimIdentifier) ELSE 0 END AS pro_claim_count,
					CASE WHEN claimType = 'OP' THEN COUNT(DISTINCT claimIdentifier) ELSE 0 END AS op_claim_count,
					CASE WHEN claimType = 'IP' THEN COUNT(DISTINCT claimIdentifier) ELSE 0 END AS ip_claim_count
				FROM B_002_MedicalClaimDetail  a
				WHERE YEAR(a.statementCoverFromDate) = @year
					AND MONTH(a.statementCoverFromDate) <= @month
					AND claimType IS NOT NULL
				GROUP BY a.insuredMemberIdentifier, YEAR(statementCoverFromDate), claimType 
			) b
			GROUP BY b.insuredMemberIdentifier, b.active_year
		) j
			ON a.insuredMemberIdentifier = j.insuredMemberIdentifier
			AND a.derivedCoverageYear = j.active_year
		--rxc ytd amounts
		LEFT JOIN (	
					SELECT DISTINCT b.insuredMemberIdentifier, SUM(b.policyPaidAmount) AS rxClaimTotal, 
						YEAR(prescriptionFillDate) AS active_year
					FROM B_003_PharmacyClaimDetail b
					WHERE YEAR(b.prescriptionFillDate) = @year
						AND MONTH(b.prescriptionFillDate) <= @month
					GROUP BY insuredMemberIdentifier, YEAR(prescriptionFillDate)
					) k
			ON k.insuredMemberIdentifier = a.InsuredMemberIdentifier
			AND k.active_year = @Year
		--med claims ytd amounts
		LEFT JOIN #tmp_claimTotals l
			ON l.insuredMemberIdentifier = a.InsuredMemberIdentifier
			AND l.active_year = @Year
			AND l.month <= @month
		WHERE YEAR(a.derivedCoverageEnd) = @year
			AND MONTH(a.derivedCoverageEnd) >= @month
			AND MONTH(a.derivedCoverageStart) <= @month
			AND a.RiskAdjustedIndicator=1
			AND a.PolicyRank = 1
			AND z.PreviousMember = 0
			AND a.MostRecentPolicyIndicator = 1
			AND a.PeriodKey = @periodKey
		GROUP BY  clientDSMemberKey, a.derivedCoverageYear, k.rxClaimTotal
			
		
EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Populate Captured Conditions Table';	
--2.0: pull captured conditions
	--2.1: captured cc insert
		INSERT INTO DS_NewMember_002_capturedconditions (clientDSMemberKey, cc, instances, active_year)
		SELECT DISTINCT a.clientDSMemberKey, c.conditionCode AS cc, COUNT(DISTINCT d.servicefromdate) AS instances, @Year AS active_year
		FROM DS_Client_CloneACAKey a
		INNER JOIN B_002c_InsuredMemberClaimDiag d
			on d.insuredMemberIdentifier=a.insuredMemberIdentifier
		INNER JOIN [ParetoReferenceDB].[dbo].[REF_Scala_ACA_ICDCrosswalk] c
			ON d.diagnosisCode = c.diagnosisCode
			AND @year = c.ModelYear
		INNER JOIN DS_NewMember_001_ACA_Info e
			ON e.clientDSMemberKey = a.clientDSMemberKey
		WHERE YEAR(d.servicefromdate) = @year
			AND MONTH(d.servicefromdate) <= @month
			AND d.scored_claim = 1
		GROUP BY a.clientDSMemberKey, c.conditionCode
		;

--3.0: pull nonscored conditions
EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Populate NonScored Conditions Table';	
		INSERT INTO DS_NewMember_003_nonscoredconditions (clientDSMemberKey, cc, instances, active_year)
		SELECT DISTINCT a.clientDSMemberKey, c.conditionCode AS cc, COUNT(DISTINCT d.serviceFromDate) AS instances, @Year AS active_year
		FROM DS_Client_CloneACAKey a
		INNER JOIN B_002c_InsuredMemberClaimDiag d
			on d.insuredMemberIdentifier=a.insuredMemberIdentifier
		INNER JOIN [ParetoReferenceDB].[dbo].[REF_Scala_ACA_ICDCrosswalk] c
			ON d.diagnosisCode = c.diagnosisCode
			AND @year = c.ModelYear
		INNER JOIN DS_NewMember_001_ACA_Info e
			ON e.clientDSMemberKey = a.clientDSMemberKey
		WHERE YEAR(d.servicefromdate) = @year
			AND MONTH(d.servicefromdate) <= @month
			AND d.scored_claim =  0
		GROUP BY a.clientDSMemberKey, c.conditionCode

--4.0: proc codes
EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Populate Procedures Table';	
		INSERT INTO DS_NewMember_004_procedures (clientDSMemberKey, class, category, instances, active_year)
		
		SELECT DISTINCT a.clientDSMemberKey, d.class, d.category, COUNT(DISTINCT c.serviceToDate) AS instances, a.active_year
		FROM (
			SELECT DISTINCT c.clientDSMemberKey, b.insuredMemberIdentifier, b.claimIdentifier, @year as active_year 
			FROM B_002_MedicalClaimDetail b
			INNER JOIN DS_Client_CloneACAKey c
				on b.insuredMemberIdentifier=c.insuredMemberIdentifier
			WHERE YEAR(b.statementCoverFromDate) = @year
				AND MONTH(b.statementCoverFromDate) <= @month
		) a
		INNER JOIN B_002a_MedicalClaimServiceLine c
			ON a.claimIdentifier = c.claimIdentifier
			AND YEAR(c.serviceToDate) = @year
			AND MONTH(c.serviceToDate) <= @month
		INNER JOIN Paretoreferencedb..REF_CPT_Category d
			ON c.serviceCode = d.code
		INNER JOIN DS_NewMember_001_ACA_Info e
			ON e.clientDSMemberKey = a.clientDSMemberKey
		WHERE serviceCode <> ''
		GROUP BY a.clientDSMemberKey, a.active_year, d.class, d.category
		
--5.0: drugs
	--5.1: drugs insert
EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Populate Drug Table';	
		INSERT INTO  DS_NewMember_005_drugs (clientDSMemberKey, drug_subclass, instances, active_year)
		SELECT DISTINCT a.clientDSMemberKey, c.drug_subclass, COUNT(DISTINCT b.prescriptionFillDate) AS instances, @Year AS active_year
		FROM DS_Client_CloneACAKey a
		INNER JOIN B_003_PharmacyClaimDetail b
			ON a.insuredMemberIdentifier = b.insuredMemberIdentifier
		INNER JOIN ParetoReferenceDB..REF_Medispan_NDC_DrugClass c
			ON b.nationalDrugCode = c.ndc_upc_hri
		INNER JOIN DS_NewMember_001_ACA_Info e
			ON e.clientDSMemberKey = a.clientDSMemberKey
		WHERE YEAR(b.prescriptionFillDate) = @year
			AND MONTH(b.prescriptionFillDate) <= @month
		GROUP BY a.clientDSMemberKey, c.drug_subclass

--6.0: Collect Third Party Data		
EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Populate Third Party Data Table';	
		INSERT INTO  DS_NewMember_006_county_tpd  (clientDSMemberKey, metric, value)
		SELECT DISTINCT a.clientDSMemberKey, 
			CASE WHEN c.metric IS NULL THEN d.metric ELSE c.metric END,
			CASE WHEN c.value IS NULL THEN d.value ELSE c.value END
		FROM DS_Client_CloneACAKey a
		INNER JOIN (
			SELECT DISTINCT g.insuredMemberIdentifier, mbrZipCode, RateAreaIdentifier, issuerState
			FROM dbo.B_100_DerivedCoverage a
			INNER JOIN DS_Client_CloneACAKey h
				ON h.insuredMemberIdentifier = a.InsuredMemberIdentifier
			INNER JOIN DM_AnalysisPeriods d
				ON a.PeriodKey = d.PeriodKey
			INNER JOIN	A_001_Import_MemberInfo g
				on a.insuredMemberIdentifier=g.insuredMemberIdentifier
				AND a.derivedCoverageStart = g.coverageStartDate
			WHERE d.periodkey=@periodKey
				AND g.coverageEndDate > CONCAT(@year,'-01-01')
				AND policyrank = 1
				AND a.derivedCoverageEnd = MAX(a.derivedCoverageEnd)
			GROUP BY g.insuredMemberIdentifier
		) b
			ON a.InsuredMemberIdentifier = b.insuredMemberIdentifier
		LEFT JOIN (
			SELECT s.ZIPCODE, t.metric, t.value
			FROM ParetoReferenceDB..REF_County s
			INNER JOIN ParetoReferenceDB..REF_Spotlight_County_TPD t
				ON s.CNTY_CD = t.FIPS_state_county_code
				) c
			ON b.mbrZipCode = c.ZIPCODE
		LEFT JOIN (
			SELECT x.Rating_Area_Number, x.State, z.metric, AVG(z.value) as value
			FROM [ParetoReferenceDB].[dbo].CMS_2018_Rating_Area_County_Crosswalk x
			INNER JOIN ParetoReferenceDB..REF_County y
				ON x.County = y.COUNTY_NM 
				AND x.State = y.STATE_CD
			INNER JOIN ParetoReferenceDB..REF_Spotlight_County_TPD z
				ON y.CNTY_CD = z.FIPS_state_county_code
			GROUP BY x.Rating_Area_Number, x.State, z.metric
			) d
			ON b.RateAreaIdentifier = d.Rating_Area_Number
			AND b.issuerState = d.State
		INNER JOIN DS_NewMember_001_ACA_Info e
			ON e.clientDSMemberKey = a.clientDSMemberKey
		WHERE ((c.metric IS NOT NULL) OR (d.metric IS NOT NULL))
			AND ((c.value IS NOT NULL) OR (d.value IS NOT NULL))
	

EXEC Process_Log 'DS_NewMember_ACA_Inputs', 'Process Complete';	

END;


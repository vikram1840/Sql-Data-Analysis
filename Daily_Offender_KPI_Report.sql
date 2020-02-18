
---- This SQL Query Written to Fetch Daily KPI Report FROM RDBMS WHERE Data stored in respective table After ETL processing done .



---------- DECLARE String Variable AND assign value ex: name of RNCS.

	DECLARE @RNCName VARCHAR(max)= '''RNC1'' , ''RNC2'' , ''RNC3'' , ''RNC4'' , ''RNC5'''
	DECLARE @Columnname VARCHAR(max) = ' ' 
	DECLARE @SQL1 VARCHAR(max) 
	DECLARE @SQL2 VARCHAR(max) 

---------- DECLARE Date datatype AND convert that to 45 days before today'date

			DECLARE @MyDate1 AS DATE=DateADD(DAY,-45,Convert(date,GETDATE()))

----------- SELECT All available dates in Table AND make a list of them to cosnider as column name. This will use while implementing PIVOT for daily trends.

				SELECT   @Columnname  += QUOTENAME(EX.start_date ) + ','
				FROM (SELECT DISTINCT Start_date FROM COPS_PM_ERICSSON_3G.data.KPICounter_RNC_Sector_Daily with (NOLOCK) )EX
				ORDER BY start_date desc
				Set @columnname = LEFT(@columnname,LEN(@columnname)-1)

-------- to check Whether column name printed correctly or not
					PRINT @columnname

--------- Assign SQL Queries to Variable using SET Statement

set @SQL1= '
			SELECT * 
			FROM (
							SELECT D.*,c.Start_Date,c.[PS Retainability]
							FROM 
							
							(
									SELECT Start_Date,RNCName,SiteName, SectorName,RNCName + ''_'' + SectorName as Conc,
									ROUND(CASE WHEN SUM ( CAST ([utrancell.Pmnonormalrabreleasepacket] as float) + CAST ([utrancell.Pmnosystemrabreleasepacket] as float) - CAST ([utrancell.pmNoSystemRabReleasePacketUra] as float) + CAST ([utrancell.pmChSwitchSuccFachUra] as float) + CAST ([utrancell.pmChSwitchSuccDchUra] as float) +  CAST ([utrancell.pmChSwitchSuccHsUra] as float)) = 0 THEN 0 ELSE (100-(100 * (SUM ( CAST ([utrancell.Pmnosystemrabreleasepacket] as float) - CAST ([utrancell.pmNoSystemRabReleasePacketUra] as float) - CAST ([utrancell.pmChSwitchAttemptFachUra] as float) + CAST ([utrancell.pmChSwitchSuccFachUra] as float) -  CAST([utrancell.pmChSwitchAttemptDchUra] as float) + CAST ([utrancell.pmChSwitchSuccDchUra] as float) -  CAST([utrancell.pmChSwitchAttemptHsUra] as float) + CAST ([utrancell.pmChSwitchSuccHsUra] as float)))/ SUM( CAST ([utrancell.Pmnonormalrabreleasepacket] as float) + CAST ([utrancell.Pmnosystemrabreleasepacket] as float) - CAST ([utrancell.pmNoSystemRabReleasePacketUra] as float) + CAST ([utrancell.pmChSwitchSuccFachUra] as float) + CAST ([utrancell.pmChSwitchSuccDchUra] as float) + CAST ([utrancell.pmChSwitchSuccHsUra] as float))))END,2) as [PS Retainability]
									FROM 3G_PM_TABLE.data.KPICounter_RNC_Sector_Daily WITH (NOLOCK)
									WHERE Start_date > ' + ''''+ CONVERT(VARCHAR(12),@mydate1,101)  + ''''+'
									AND RNCName in  (' + @RNCName + ')
									GROUP BY Start_date,RNCName,Sitename,Sectorname 
							 ) c

-------------- Inner Join Trends with last Day Data other Data like RCA, Their ranking,Percentage contribution
inner join
							(		
					
								SELECT ''PS_RET'' AS KPI,RNCName,SiteName, SectorName,RNCName + ''_'' + SectorName as Conc, CONVERT(VARCHAR(20),Convert(Date,Getdate()-1),101) AS Offender_Start_Date,
								ROUND(CASE WHEN SUM ( CAST ([utrancell.Pmnonormalrabreleasepacket] as float) + CAST ([utrancell.Pmnosystemrabreleasepacket] as float) - CAST ([utrancell.pmNoSystemRabReleasePacketUra] as float) + CAST ([utrancell.pmChSwitchSuccFachUra] as float) + CAST ([utrancell.pmChSwitchSuccDchUra] as float) +  CAST ([utrancell.pmChSwitchSuccHsUra] as float)) = 0 THEN 0 ELSE (100-(100 * (SUM ( CAST ([utrancell.Pmnosystemrabreleasepacket] as float) - CAST ([utrancell.pmNoSystemRabReleasePacketUra] as float) - CAST ([utrancell.pmChSwitchAttemptFachUra] as float) + CAST ([utrancell.pmChSwitchSuccFachUra] as float) -  CAST([utrancell.pmChSwitchAttemptDchUra] as float) + CAST ([utrancell.pmChSwitchSuccDchUra] as float) -  CAST([utrancell.pmChSwitchAttemptHsUra] as float) + CAST ([utrancell.pmChSwitchSuccHsUra] as float)))/ SUM( CAST ([utrancell.Pmnonormalrabreleasepacket] as float) + CAST ([utrancell.Pmnosystemrabreleasepacket] as float) - CAST ([utrancell.pmNoSystemRabReleasePacketUra] as float) + CAST ([utrancell.pmChSwitchSuccFachUra] as float) + CAST ([utrancell.pmChSwitchSuccDchUra] as float) + CAST ([utrancell.pmChSwitchSuccHsUra] as float))))END,2) as [D_PS_Retainability],
								Round( 100*((86400-  (SUM(ISNULL([UtranCell.pmCellDowntimeAuto],0))+SUM(ISNULL([UtranCell.pmCellDowntimeMan],0))))/86400),2) [Availability(%)],
								ROUND((CASE WHEN (SUM(CAST([utrancell.pmTotNoRrcConnectReqPs] as float)- CAST([utrancell.pmNoLoadSharingRrcConnPs] as Float))=0 or SUM(CAST([utrancell.pmNoRabEstablishAttemptPacketInteractive] as float))=0) THEN 0 Else 100*(SUM(CAST([utrancell.pmNoRabEstablishSuccessPacketInteractive] as float))/SUM(CAST([utrancell.pmNoRabEstablishAttemptPacketInteractive] as float)))*(SUM(CAST([utrancell.pmTotNoRrcConnectReqPsSucc] as float))/SUM(CAST([utrancell.pmTotNoRrcConnectReqPs] as float)- CAST([utrancell.pmNoLoadSharingRrcConnPs] as Float))) END),2) as [PS Accessibility],
								ROUND( CASE WHEN SUM(CAST([utrancell.pmTotNoRrcConnectReqPs] as float)- CAST([utrancell.pmNoLoadSharingRrcConnPs] as Float))=0 THEN 0 ELSE 100*(SUM(CAST([utrancell.pmTotNoRrcConnectReqPsSucc] as float))/SUM(CAST([utrancell.pmTotNoRrcConnectReqPs] as float)- CAST([utrancell.pmNoLoadSharingRrcConnPs] as Float))) END,2)  as [PS RRC_Succ%],
								ROUND( CASE WHEN SUM(CAST([utrancell.pmNoRabEstablishAttemptPacketInteractive] as float))=0 THEN 0 ELSE 100*(SUM(CAST([utrancell.pmNoRabEstablishSuccessPacketInteractive] as float))/ SUM(CAST([utrancell.pmNoRabEstablishAttemptPacketInteractive] as float))) END,2)  as [PS RAB_SUCC%],
								Round((SUM(CAST([utrancell.pmTotNoRrcConnectReqPs] as float)-CAST([utrancell.pmNoLoadSharingRrcConnPs] as Float)-CAST([utrancell.pmTotNoRrcConnectReqPsSucc] as float))+ SUM(CAST([utrancell.pmNoRabEstablishAttemptPacketInteractive] as float)-CAST([utrancell.pmNoRabEstablishSuccessPacketInteractive] as float))),2) as [Total_PS_Failure],
								(sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) ) as [PS_Drop],
								ROUND(ISNULL(SUM( CASE WHEN CAST([utrancell.pmSamplesBestCs12Establish] as float)=0 THEN 0 ELSE (CAST([utrancell.pmSumBestCs12Establish] as float)/CAST([utrancell.pmSamplesBestCs12Establish] as float) * [cstraffic.csFactor]) END), 0) + 
								ISNULL(SUM( CASE WHEN CAST([utrancell.pmSamplesBestAmr12200RabEstablish] as float)=0 THEN 0  ELSE (CAST([utrancell.pmSumBestAmr12200RabEstablish] as float)/CAST([utrancell.pmSamplesBestAmr12200RabEstablish] as float)* [cstraffic.amFactor]) END ),0) +
								ISNULL(SUM( CASE WHEN CAST([utrancell.pmSamplesBestAmr5900RabEstablish] as float)=0 THEN 0 ELSE (CAST([utrancell.pmSumBestAmr5900RabEstablish] as float)/CAST([utrancell.pmSamplesBestAmr5900RabEstablish] as float)* [cstraffic.amFactor5900]) END ),0)+
								ISNULL(SUM( CASE WHEN CAST([utrancell.pmSamplesBestAmrNbMmRabEstablish] as float)=0 THEN 0 ELSE (CAST([utrancell.pmSumBestAmrNbMmRabEstablish] as float)/CAST([utrancell.pmSamplesBestAmrNbMmRabEstablish] as float)* [cstraffic.csFactor]) END ),0),2) as [CS TRAFFIC],
								ROUND( CASE WHEN SUM(CAST([utrancell.pmDlTrafficVolumePsIntHs] as float))=0 THEN 0 ELSE (SUM(CAST([utrancell.pmDlTrafficVolumePsIntHs] as float))/(1024*8)) END,2) as [HS Data Volume_MB],
								ROUND(SUM(ISNULL([utrancell.pmCellDowntimeAuto],0))/60,2) as [Cell Down (Auto)] , 
								ROUND(SUM(ISNULL([utrancell.pmCellDowntimeman],0))/60,2) as [Cell Down (Man)] ,
								Round(case when sum(cast([utrancell.pmSamplesUlRssi] as float)) = 0 then NULL else -112 + sum(cast([utrancell.pmSumUlRssi] as float)/10)/sum(cast([utrancell.pmSamplesUlRssi] as float))end,2) [RSSI] ,
								ROUND(SUM(CAST([utrancell.pmNoCellFachDisconnectAbnorm] as float)),2) AS [FACH Drops],
								ROUND(SUM(CAST(isnull([utrancell.pmChSwitchAttemptFachUra],0) as float))-   SUM(CAST(isnull([utrancell.pmChSwitchSuccFachUra],0) as float)),2) AS [Drop FACH_URA],
								ROUND(SUM(CAST(isnull([utrancell.pmChSwitchAttemptDchUra],0) as float))-   SUM(CAST(isnull([utrancell.pmChSwitchSuccDchUra],0) as float)),2) as [Drop DCH_URA],
								ROUND(SUM(CAST(isnull([utrancell.pmChSwitchAttemptHsUra],0) as float))-   SUM(CAST(isnull([utrancell.pmChSwitchSuccHsUra],0) as float)),2) as [Drop HS_URA],

							   -----------------------Rank the network Element based upon failure count and using DENSE_RANK function 

								DENSE_RANK() OVER(PARTITION BY Start_date,RNCName ORDER BY ((sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) ))  DESC) RNC_RANK_PS_Drop,
								DENSE_RANK() OVER(PARTITION BY Start_date ORDER BY ((sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) ))  DESC) Network_RANK_PS_Drop,

								---------------------------- Percentage Contribution using OVER and PARTITION BY Clause

								Round(100*((sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) )/SUM((sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) )) OVER (PARTITION BY Start_Date,RNCName ORDER BY (sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) ) Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),2) AS [%Contribution_RNC_PS_Drop],
								Round(100*((sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) )/SUM((sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) )) OVER (PARTITION BY Start_Date ORDER BY (sum(CAST([utrancell.pmNoSystemRabReleasePacket] as Float)) - sum(CAST([utrancell.pmNoSystemRabReleasePacketUra] as Float)) - (sum(CAST([utrancell.pmChSwitchAttemptFachUra] as Float))- sum(CAST([utrancell.pmChSwitchSuccFachUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptDchUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccDchUra] as Float))) - (sum(CAST([utrancell.pmChSwitchAttemptHsUra] as Float)) - sum(CAST([utrancell.pmChSwitchSuccHsUra] as Float))) ) Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),2) AS [%Contribution_Network_PS_Drop]
								FROM 3G_PM_TABLE.data.KPICounter_RNC_Sector_Daily WITH (NOLOCK)
								WHERE START_Date = CONVERT(VARCHAR(20),Convert(Date,Getdate()-1),101)
								AND RNCName in  (' + @RNCName + ')
								GROUP BY Start_Date,RNCName,SiteName, SectorName,RNCName + ''_'' + SectorName

							) D

							on C.RNCName=D.RNCname
							AND C.Sectorname=D.sectorname
							AND C.Conc=D.conc
		  )src    '

SET @SQL2=   '
				PIVOT
					(
						Sum([PS Retainability]) for Start_date in (' + @columnname +  ')
					) As PVT
				ORDER BY Network_RANK_PS_Drop  '

				--------------------- To check error if any syntax error at some specific line use PRINT Statemnet
					PRINT(@SQL1 + @SQL2)

				--------------------- To Execute Dynamic SQL Query

							EXEC( @SQL1 + @SQL2)











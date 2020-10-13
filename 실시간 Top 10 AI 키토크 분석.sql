with realKeytalk as (
SELECT
date,
  clientId,
  visitId,
  device.deviceCategory,
hit_cd103.value as Keytalk, --키토크명
COUNTIF( hits.eventInfo.eventCategory = 'Ecommerce' and hits.eventInfo.eventAction='checkout') as checkout_Num, --기여
COUNTIF( hits.eventInfo.eventCategory = 'Ecommerce' and hits.eventInfo.eventAction='transaction') as transaction_Num --거래 
FROM `197019233.ga_realtime_sessions_2*` 
  LEFT JOIN UNNEST(hits) as hits
LEFT JOIN UNNEST(hits.customDimensions) as hit_cd103 on hit_cd103.index = 103
where
hit_cd103.value IS NOT NULL and 
clientId is not null and
device.deviceCategory = 'mobile' and
exportKey in 
(   SELECT exportKey 
	FROM   
		(     
		SELECT exportKey, exportTimeUsec, 
		MAX(exportTimeUsec) OVER (PARTITION BY visitKey) AS maxexportTimeUsec
		 FROM `197019233.ga_realtime_sessions_2*`  
		  )  
 WHERE exportTimeUsec >= maxexportTimeUsec )
 group by clientId,visitId,date,hit_cd103.value,device.deviceCategory
),  searchKey as (
      select 
      date,
      clientId,
      visitId,
      COUNTIF( hits.eventInfo.eventCategory = '검색결과') as search_Num
      FROM `197019233.ga_realtime_sessions_2*` 
      LEFT JOIN UNNEST(hits) as hits
      where clientId is not null
      group by clientId,visitId,date
      having search_Num > 0 
  ),profileKey as (
  select 
  date,
  clientId,
  visitId,
  COUNTIF( hits.eventInfo.eventCategory like '프로필_%') as profile_Num --프로필
  FROM `197019233.ga_realtime_sessions_2*` 
  LEFT JOIN UNNEST(hits) as hits
  group by clientId,visitId,date
 ),
 alltest AS (
  select
  realKeytalk.date as date,
  realKeytalk.clientId as clientId,
  realKeytalk.visitId as visitId,
  keytalk as keytalk,
  searchKey.search_Num as search_Num,
  realKeytalk.checkout_Num as checkout_Num,
  realKeytalk.transaction_Num as transaction_Num
  from realKeytalk
  inner join searchKey
  on realKeytalk.clientId = searchKey.clientId
  and realKeytalk.visitId = searchKey.visitId

  )
  select a.date as date,
  a.clientId as clientId,
  a.visitId as visitId,
  a.keytalk as keytalk,
  a.search_Num as search_Num,
  a.checkout_Num as checkout_Num,
  a.transaction_Num as transaction_Num,profileKey.profile_Num as profile_Num
  from alltest a inner join profileKey on a.clientId = profileKey.clientId

WITH 
ADJ_TEMPLATE_MAPPING AS
(
select 1 id ,1 template_id ,1 column_id,'ISL Adj Reason Desc' column_name  from dual union
select 2,1,2,'ISL Adj Reason Component Name'  from dual union
select 3,1,3,'Region'  from dual union
select 4,1,4,'Attribution'  from dual union
select 5,1,5,'Category'  from dual union
select 6,1,6,'GL Account'  from dual union
select 7,1,7,'Cusip'  from dual union
select 8,1,8,'ISL Facts Parent Code'  from dual union
select 9,1,9,'TAPS Account'  from dual union
select 10,1,10,'ISL Facts Cash Non-Cash Ind'  from dual union
select 11,1,11,'ISL Facts Borrow Loan Ind'  from dual union
select 12,1,12,'Product Number'  from dual union
select 13,1,13,'ISL Facts Strategy Code'  from dual union
select 14,1,14,'customer category'  from dual union
select 15,1,15,'ISL Facts Collateral Type Ind'  from dual union
select 16,1,16,'ISL Facts Collateral Currency'  from dual union
select 17,1,17,'ISL Facts Transaction Currency'  from dual union
select 18,1,18,'ISL Adjustment Type'  from dual union
select 19,1,19,'Adjustment Expiry Date'  from dual union
select 20,1,20,'Planet Ind'  from dual union
select 21,1,21,'Hedging Indicator'  from dual union
select 22,1,22,'Adj Category'  from dual union
select 23,1,23,'DR Or CR'  from dual union
select 24,1,24,'Event Date'  from dual union
select 25,1,25,'Annc Pay Date'  from dual union
select 26,1,26,'Announcement Id'  from dual union
select 27,1,27,'Event Rate'  from dual union
select 28,1,28,'Quantity'  from dual union
select 29,1,29,'MX'  from dual union
select 30,1,30,'USD'  from dual union
select 31,1,31,'Base'  from dual union
select 32,1,32,'comment' from dual
)
,REPORCESS_TEMPLATE_DATA (ADJUSTMENT_ID , FIELD_NO, ROW_DATA)
AS
(
  SELECT base.ADJUSTMENT_ID, 
  base.FIELD_NO, 
  base.ROW_DATA 
  FROM (
        select A.ADJUSTMENT_ID ,ROW_NUMBER() over (partition by A.ADJUSTMENT_ID) FIELD_NO,A.value ROW_DATA
                FROM DSL.ADJ_FIELD A, ADJ_TEMPLATE_MAPPING M  
                WHERE M.COLUMN_NAME = A.NAME
                and ADJUSTMENT_ID IN                
                (SELECT C.ADJUSTMENT_ID
                        FROM DSL.ADJ_REQUEST A ,DSL.ADJ_TYPE B ,   DSL.ADJ_ADJUSTMENT C, DSL.ADJ_RESULT D
                        WHERE A.TYPE_CD = B.UUID
                        AND A.REQUEST_ID = C.REQUEST_ID
                        AND B.SHORT_CODE = 'ISLDRCRGENERIC' 
                        AND C.ADJUSTMENT_ID = D.ADJUSTMENT_ID
                        AND D.STATUS <> 'UNHANDLED EXCEPTION'
                        AND A.RECEIVE_TIME >= TO_DATE ('5/3/2018 10:23:53 AM','MM/DD/YYYY HH:MI:SS AM')
                ) 
                order by M.COLUMN_ID
            ) base 
  WHERE FIELD_NO = 1
  
  UNION ALL
  
  SELECT t1.ADJUSTMENT_ID, t1.FIELD_NO, ROW_DATA || '	' || t1.DATA
  FROM REPORCESS_TEMPLATE_DATA t0, 
  (select  A.ADJUSTMENT_ID ,ROW_NUMBER() over (partition by A.ADJUSTMENT_ID) FIELD_NO,A.value DATA
            FROM DSL.ADJ_FIELD A, ADJ_TEMPLATE_MAPPING M
            WHERE M.COLUMN_NAME = A.NAME 
            and  ADJUSTMENT_ID IN  
             (SELECT C.ADJUSTMENT_ID
                        FROM DSL.ADJ_REQUEST A ,DSL.ADJ_TYPE B ,   DSL.ADJ_ADJUSTMENT C, DSL.ADJ_RESULT D
                        WHERE A.TYPE_CD = B.UUID
                        AND A.REQUEST_ID = C.REQUEST_ID
                        AND B.SHORT_CODE = 'ISLDRCRGENERIC'
                        AND C.ADJUSTMENT_ID = D.ADJUSTMENT_ID
                        AND D.STATUS <> 'UNHANDLED EXCEPTION'
                        AND A.RECEIVE_TIME >= TO_DATE ('5/3/2018 10:23:53 AM','MM/DD/YYYY HH:MI:SS AM')
                ) 
            order by M.COLUMN_ID 
            ) t1
  WHERE t0. ADJUSTMENT_ID = t1. ADJUSTMENT_ID
    AND t0.FIELD_NO + 1 = t1.FIELD_NO
)
SELECT ROW_NUMBER() OVER () AS ROWNUM,'HEADER' AS ADJUSTMENT_ID, 'ISL Adj Reason Desc	ISL Adj Reason Component Name	Region	Attribution	Category	GL Account	Cusip	ISL Facts Parent Code	TAPS Account	ISL Facts Cash Non-Cash Ind	ISL Facts Borrow Loan Ind	Product Number	ISL Facts Strategy Code	customer category	ISL Facts Collateral Type Ind	ISL Facts Collateral Currency	ISL Facts Transaction Currency	ISL Adjustment Type	Adjustment Expiry Date	Planet Ind	Hedging Indicator	Adj Category	DR Or CR	Event Date	Annc Pay Date	Announcement Id	Event Rate	Quantity	MX	USD	Base	comment' AS ROW_DATA
FROM DUAL
UNION
SELECT ROW_NUMBER() OVER ()+1 AS ROWNUM ,ADJUSTMENT_ID, ROW_DATA 
FROM REPORCESS_TEMPLATE_DATA WHERE FIELD_NO = 32
ORDER BY ROWNUM;

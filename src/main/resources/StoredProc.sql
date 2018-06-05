create or replace PROCEDURE MARKETING_REPORT_DATA IS                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             

v_logFileName VARCHAR2 (256)         :=    'MARKETING_REPORT_DATA_'            || TO_CHAR (SYSDATE, 'MMDDYYYY')       || '.log';

BEGIN
DELETE FROM MKT_TRANS_DATA;
Proc_Log ( v_logFileName, TO_CHAR (SYSDATE, 'MM/DD/YYYY HH:MI:SS AM') || ': STARTING MARKETING_REPORT_DATA...... ');

 
INSERT
INTO PNP_ADMIN.MKT_TRANS_DATA --final table
  (
    TEMPLATE_NAME, -- coorespondance template
    MARKETING_ID, -- used for tableau report
    FILE_NAME, --
    JCN_NO,
    JCN_CYCLE,
    DISPLAY_TEMP_NAME,
    SUBJECT,
    CATEGORY,
    SENT_DATE,
    ATTEMPTS,
    DELIVERED,
    BOUNCED,
    SCRUBBED,
    DELIVERY_RATE,
    UNIQUE_CLICKS,
    TOTAL_CLICKS,
    UNIQUE_OPENS,
    TOTAL_OPENS,
    UNIQUE_OPEN_RATE,
    UNIQUE_CLICK_RATE,
    OPT_OUT,
  --  TOTAL_OPTOUT,
    OPT_OUT_RATE,
    SPOTLIGHT,
    SPORTS,
    PAYPERVIEW,
 --   NewProducts,
    VOD,
    PRODENHANCE,
    PREMIUMVD,
    EQUIP,
    TACTIC,
    LOB
    
  )
SELECT TI.TEMPLATE_NAME,
  MARKETING_ID ,
   TI.INPUT_ID FILE_NAME,
 -- SUBSTR( TI.INPUT_ID, 3, INSTR(TI.INPUT_ID,'_')-3) JCN_NO,
 -- SUBSTR( input_id, INSTR(input_id,'_')+1, 3) JCN_CYCLE,
  (CASE WHEN (ti.input_id LIKE '%.shp') THEN (SUBSTR( TI.INPUT_ID, 3, INSTR(TI.INPUT_ID,'_')-3)) ELSE null end) JCN_NO,
(CASE WHEN (ti.input_id LIKE '%.shp') THEN (SUBSTR( input_id, INSTR(input_id,'_')+1, 3)) ELSE null end) JCN_CYCLE,
ti.DISP_TEMPLATE_NAME DISPLAY_TEMP_NAME,
  et.SUBJECT ,
  ET.CATEGORY,
  MIN (TI.SENT_DATE) SENT_DATE,
  SUM (
  CASE
    WHEN (STATUS_ID = 3
    OR STATUS_ID    = -9)
    THEN 1
    ELSE 0
  END) ATTEMPTS,
  SUM (
  CASE
    WHEN (STATUS_ID = 3)
    THEN 1
    ELSE 0
  END) DELIVERED,
  SUM (
  CASE
    WHEN (STATUS_ID = -9)
    THEN 1
    ELSE 0
  END) BOUNCED,
  SUM (
  CASE
    WHEN (STATUS_ID < 0
    AND STATUS_ID  != -9)
    THEN 1
    ELSE 0
  END) SCRUBBED,
  ROUND ( SUM (
  CASE
    WHEN (STATUS_ID = 3)
    THEN 1
    ELSE 0
  END) * 100 / NULLIF ( SUM (
  CASE
    WHEN (STATUS_ID = 3
    OR STATUS_ID    = -9)
    THEN 1
    ELSE 0
  END), 0), 2) DELIVERY_RATE,
  SUM (
  CASE
    WHEN (CLICKED > 0)
    THEN 1
    ELSE 0
  END) UNIQUE_CLICKS,
  SUM (CLICKED) TOTAL_CLICKS,
  SUM (
  CASE
    WHEN (OPENED > 0)
    THEN 1
    ELSE 0
  END) UNIQUE_OPENS,
  SUM (OPENED) TOTAL_OPENS,
  ROUND ( SUM (
  CASE
    WHEN (OPENED > 0)
    THEN 1
    ELSE 0
  END) * 100 / NULLIF (SUM (
  CASE
    WHEN (STATUS_ID = 3)
    THEN 1
    ELSE 0
  END), 0), 2) UNIQUE_OPEN_RATE100,
  ROUND ( SUM (
  CASE
    WHEN (CLICKED > 0)
    THEN 1
    ELSE 0
  END) * 100 / NULLIF (SUM (
  CASE
    WHEN (OPENED > 0)
    THEN 1
    ELSE 0
  END), 0), 2) UNIQUE_CLICK_RATE100,
 /* SUM (
  CASE
    WHEN (OPT_OUT > 0)
    THEN 1
    ELSE 0
  END) OPT_OUT,*/
  count(OOUT.EMAIL_ADDRESS) OPT_OUT,
   ROUND (count(OOUT.EMAIL_ADDRESS)/NULLIF (SUM (
  CASE
    WHEN (STATUS_ID = 3)
    THEN 1
    ELSE 0
  END), 0), 2) OPT_OUT1000,
  --SUM (OPT_OUT) TOTAL_OPTOUT,
 /* ROUND ( SUM (
  CASE
    WHEN (OPT_OUT > 0)
    THEN 1
    ELSE 0
  END) * 1000 / NULLIF (SUM (
  CASE
    WHEN (STATUS_ID = 3)
    THEN 1
    ELSE 0
  END), 0), 2) OPT_OUT1000,*/
SUM(decode(PRI_SPOTLIGHT,'N',1,0)) SPOTLIGHT,
SUM(decode(PRI_SPORTS,'N',1,0)) SPORTS,
SUM(decode(PRI_PAY_PER_VIEW,'N',1,0))  PAYPERVIEW,
--SUM(decode(OIN.PRI_NEWPRODUCTS_SPECIALOFFERS,'N',1,0)) NEWPRODUCTS,
SUM(decode(PRI_MOVIE_VOD,'N',1,0))  VOD,
SUM(decode(PRI_PRODUCT_ENHANCE,'N',1,0))  PRODENHANCE,
SUM(decode(PRI_PREMIUM_VIDEO,'N',1,0))  PREMIUMVD,
SUM(decode(PRI_EQUIP_ACCESSORIES,'N',1,0))  EQUIP,
TI.DEL_MEDIUM TACTIC,
(DECODE(ET.LOB,'C','CONSUMER','B','BUSINESS','BC','BOTH')) LOB
FROM TRANSACTION_IDENTIFIER ti, -- these four tables are joined (some left outer joins, opt outs for example)
  ecrm_templates et,
  email_optin_tracking oin,
  email_optout_tracking oout
WHERE et.seq_id = ti.template_seq_id and
ti.tx_id=oin.tx_id(+) and
ti.tx_id=oout.tx_id(+)
AND ti.app_name = 'AEDW'
AND TI.SENT_DATE BETWEEN TRUNC (SYSDATE - 30) AND SYSDATE
GROUP BY TI.TEMPLATE_NAME,
  MARKETING_ID,
  TI.INPUT_ID,
  ti.DISP_TEMPLATE_NAME,
  et.SUBJECT,
  TRUNC(TI.SENT_DATE),
  et.template_id,
  ET.CATEGORY,
  TI.DEL_MEDIUM,
  ET.LOB;
-- run for last 30 days, takes 4-5 minutes.

Proc_Log ( v_logFileName,     TO_CHAR (SYSDATE, 'MM/DD/YYYY HH:MI:SS AM') || ': Insertion of data completed  ') ;

COMMIT;

UPDATE Pnp_Admin.Mkt_Trans_Data T --extra data points added after the fact. updating same table
SET T.Campaign_Name=
  CASE
    WHEN (T.File_Name LIKE '%.shp')
    THEN
      (SELECT Campaign_Name FROM Campaign_Info WHERE Jcn_No=T.Jcn_No
      )
    ELSE T.File_Name
  END;
  

Proc_Log ( v_logFileName,     TO_CHAR (SYSDATE, 'MM/DD/YYYY HH:MI:SS AM') || ': UPDATION OF CAMPAIGN NAME COMPLETED ') ;

UPDATE PNP_ADMIN.MKT_TRANS_DATA T
SET CATEGRY_NAME =
  (SELECT category_name
  FROM EOD_ADMIN.ecrm_template_category
  WHERE category_code=T.category
  ) ;
 
 Proc_Log (      v_logFileName,      TO_CHAR (SYSDATE, 'MM/DD/YYYY HH:MI:SS AM')      || ': UPDATION OF CATEGORY NAME COMPLETED...ENDING....');
 
COMMIT; --makes the transaction permanent

 
EXCEPTION
   WHEN OTHERS
   THEN
      ROLLBACK;
      Proc_Log (
         v_logFileName,
            TO_CHAR (SYSDATE, 'MM/DD/YYYY HH:MI:SS AM')
         || ': Exception while INSERTING DATA : '
         || SQLCODE
         || ' SQLErr: '
         || SQLERRM);
  

END MARKETING_REPORT_DATA; --overall, creating a view for reporting
/

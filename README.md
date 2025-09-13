PTRF029A.mb2
WITH DEALS AS
(        SELECT DEL.ENTITY,
                DEL.DEAL_NO,
                DEL.LAST_RELEASE_AMD
           FROM &1.IXQDBDEL DEL
LEFT OUTER JOIN &1.IXQDBCNA CNA
            ON  DEL.ENTITY = CNA.ENTITY
            AND DEL.CUSTOMER_ID = CNA.CUSTOMER_ID
      LEFT JOIN &1.IXQDBSTP BKF_STP
            ON  BKF_STP.ENTITY = DEL.ENTITY
            AND BKF_STP.DEAL_NO = DEL.DEAL_NO
            AND SUBSTR(DEL.LAST_REL_STEP_ID, 1, 3) = 'BKF'
            AND BKF_STP.STEP_ID = DEL.LAST_REL_STEP_ID
          WHERE DEL.ENTITY <> '006'
            AND SUBSTR(DEL.DEAL_NO, 1, 5) NOT IN ('00000', '99999')
            AND CNA.ACC_HOLD_BRANCH &3 '99747'
            AND (BKF_STP.ACTUAL_REL_DATE IS NULL
                 OR (CASE WHEN BKF_STP.ACTUAL_REL_DATE > 0
                          THEN BKF_STP.ACTUAL_REL_DATE
                          ELSE (CASE WHEN BKF_STP.OFFERING_DATE > 0
                                     THEN BKF_STP.OFFERING_DATE
                                     ELSE (
                                        CASE WHEN BKF_STP.REL_DATE_A > 0
                                             THEN BKF_STP.REL_DATE_A
                                             ELSE BKF_STP.REL_DATE_B
                                             END)
                                     END)
                          END) > &2)
)
SELECT
MSO.ENTITY AS Entity, 
MSO.DEAL_NO AS DEAL_No, 
MSO.MSG_TYPE AS Message_Type, 
MSO.MESSAGE_NO AS Message_No,   
CASE
    WHEN INSTR(CAST(MSO.MSG_TEXT AS CHAR(200)),':20:') > 0 
    THEN SUBSTR(MSO.MSG_TEXT, 
         INSTR(CAST(MSO.MSG_TEXT AS CHAR(200)),':20:') + 4, 16)
    ELSE  '  '
END AS SENDER_REFERENCE_NUM,                     
MSO.TAG_108 AS UNIQUE_REF_NUMBER,                 
SUBSTR(MSO.MSG_TEXT, 7, 8) AS SENDER_BIC,            
SUBSTR(MSO.MSG_TEXT, 37, 8) AS RECEIVER_BIC,  
MSO.TIME_CREATED AS TIME_CREATED,
MSO.MSG_TEXT AS Message_Text
FROM &1.IXQDBMSO MSO
INNER JOIN DEALS
       ON  DEALS.ENTITY = MSO.ENTITY
       AND DEALS.DEAL_NO = MSO.DEAL_NO
WHERE MSG_TYPE = 'S'
;

PTRF029A.mbe
 OPTION COPY
 INREC IFTHEN=(WHEN=INIT,FINDREP=(IN=C'"',OUT=C'$')),
       IFTHEN=(WHEN=INIT,BUILD=(ENTITY,
     C'|',DEAL_NO,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=16),
     C'|',MSG_TYPE,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=3),
     C'|',MESSAGE_NO,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=5),
     C'|',SENDER_REFERENCE_NUM,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=18),
     C'|',UNIQUE_REF_NUMBER,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=18),
     C'|',SENDER_BIC,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=10),
     C'|',RECEIVER_BIC,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=10),
     C'|',TIME_CREATED,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=28),
     C'|',MSG_TEXT,
          JFY=(SHIFT=LEFT,LEAD=C'"',TRAIL=C'"',LENGTH=10242))),
       IFTHEN=(WHEN=INIT,BUILD=(01,20000,SQZ=(SHIFT=LEFT,PAIR=QUOTE))),
       IFTHEN=(WHEN=INIT,FINDREP=(IN=C'"',OUT=C'')),
       IFTHEN=(WHEN=INIT,FINDREP=(IN=C'$',OUT=C'"')),
       IFTHEN=(WHEN=INIT,FINDREP=(IN=C'0000-00-00',OUT=C''))
   OUTFIL REMOVECC,FTOV,VLTRIM=C' ',
                HEADER1=('ENTITY|',
                         'DEAL_NO|',
                         'MSG_TYPE|',
                         'MESSAGE_NO|',
                         'SENDER_REFERENCE_NUM|',
                         'UNIQUE_REF_NUMBER|',
                         'SENDER_BIC|',
                         'RECEIVER_BIC|',
                         'TIME_CREATED|',
                         'MSG_TEXT')

PTRF029B.mbe
ENTITY,01,03,CH
DEAL_NO,04,14,CH
MSG_TYPE,18,01,CH
MESSAGE_NO,19,03,CH
SENDER_REFERENCE_NUM,22,16,CH
UNIQUE_REF_NUMBER,41,16,CH
SENDER_BIC,57,08,CH
RECEIVER_BIC,65,08,CH
TIME_CREATED,73,26,CH
MSG_TEXT,99,10240,CH


UTIJKE01.jcl
//Y8965520  JOB (SNA9578X,MVS),'UTIKJE02',CLASS=Q,MSGCLASS=U,
//   MSGLEVEL=(1,1),NOTIFY=&SYSUID
//*
//*********************************************************************
//* Test step for unload of EPTRF029 - Outgoing-Messages
//* - DELFICS  : deletes the output files
//* - UTSY11A1 : adapts the DB2 sysin (date and environment)
//* - UTIKJE01 : unload
//*********************************************************************
//*--------------------------------------------------------------------
//         EXPORT SYMLIST=(*)
//         SET    SUFFIX=D240218
//         SET    OPE='<>'
//         SET    DATEJ8=20230319
//*--------------------------------------------------------------------
//* Deletion of output files
//*--------------------------------------------------------------------
//DELFICS  EXEC PGM=IDCAMS
//         EXPORT SYMLIST=(*)
//SYSIN    DD  *,SYMBOLS=JCLONLY
        DELETE Y896552.KPTRF029.UTSY11A1.&SUFFIX. NONVSAM PURGE
        DELETE Y896552.KPTRF029.UTIKJE02.DELMSC.&SUFFIX. NONVSAM PURGE
        SET MAXCC=0
/*
//*     DELETE Y896552.KPTRF029.UTSY11A1.&SUFFIX. NONVSAM PURGE
//*     DELETE Y896552.KPTRF029.UTIKJE02.DELMSC.&SUFFIX. NONVSAM PURGE
//*     SET MAXCC=0
//SYSPRINT DD  SYSOUT=*
//SYSOUT   DD  SYSOUT=*
//*--------------------------------------------------------------------
//* Creation of the DB2 SYSIN
//*--------------------------------------------------------------------
//UTSY11A1 EXEC PGM=SY00011,
// PARM=('IXEI,&DATEJ8,&OPE')
//*
//DD00011A DD  DISP=SHR,
//             DSN=Y896552.PTRGIT.MB2(PTRF029A)
//*
//DD00011S DD  DISP=(NEW,CATLG,DELETE),
//             DSN=Y896552.KPTRF029.UTSY11A1.&SUFFIX.,
//             SPACE=(TRK,(1,1),RLSE),
//             MGMTCLAS=MA0010
//*        DB2 query file
//*
//SYSOUT   DD  SYSOUT=*
//CEEDUMP  DD  SYSOUT=E
//*--------------------------------------------------------------------
//* Unloading IMEX client data
//*--------------------------------------------------------------------
//UTIKJE01 EXEC PGM=SY00100A,
//         PARM=(07,IKJEFT1B)
//SYSIN    DD  DISP=SHR,
//             DSN=Y896552.KPTRF029.UTSY11A1.&SUFFIX.
//*        Command or program to execute
//SYSTSIN  DD  *
  DSN SYSTEM(DB2D)
  RUN PROGRAM(DSNTIAUL)  PARM ('SQL')
  END
//*
//SYSREC00 DD  DISP=(NEW,CATLG,DELETE),
//             DSN=Y896552.KPTRF029.UTIKJE02.OMG.&SUFFIX.,
//             SPACE=(CYL,(99,25),RLSE),
//             MGMTCLAS=MA0030
//*        FICHIER UNLOAD
//*
//SYSTSPRT DD  SYSOUT=*
//SYSTSPRT DD  SYSOUT=*
//*        SYSOUT RESULT
//SGOUT    DD  SYSOUT=*
//SYSPRINT DD  SYSOUT=*
//SYSPUNCH DD  SYSOUT=*
//*
//*

UTSORTA1.jcl
//Y8965520  JOB (SNA9578X,MVS),'UTSORTA1',CLASS=Q,MSGCLASS=U,
//   MSGLEVEL=(1,1),NOTIFY=&SYSUID
//*
//*********************************************************************
//* JCL for testing the conversion of the Outgoing-Messages dataset to 
//* CSV format:
//* - DELFICS: deletes the output files
//* - SETUP: sort step creating the test file
//* - UTSORTA1: sort step to be tested (this is the one that performs 
//*   the CSV formatting
//* - To come: ASSERT?
//*********************************************************************
//*
//DELFICS  EXEC PGM=IDCAMS
//         EXPORT SYMLIST=(*)
//SYSIN    DD  *,SYMBOLS=JCLONLY
        DELETE Y896552.KPTRF029.UTSORTA1.INPUT NONVSAM PURGE
        DELETE Y896552.KPTRF029.UTSORTA1.OUTPUT NONVSAM PURGE
        SET MAXCC=0
/*
//SYSPRINT DD  SYSOUT=*
//SYSOUT   DD  SYSOUT=*
//*
//*--------------------------------------------------------------
//SETUP    EXEC PGM=SORT
//SYSOUT   DD  SYSOUT=*
//*        FICHIER UNLOAD DEALS
//SORTIN   DD  *
001;BASIC CASE
002;SIMPLE QUOTE
003;DOUBLE QUOTE
004;NO WHITE SPACE
//*
//SORTOUT  DD  DISP=(NEW,CATLG,DELETE),
//             RECFM=FB,LRECL=10400,DSORG=PS,
//             SPACE=(CYL,(99,25),RLSE),
//             MGMTCLAS=MA0030,
//             DSN=Y896552.KPTRF029.UTSORTA1.INPUT
//*
//SYSIN    DD  *
  OPTION COPY
  OUTREC IFTHEN=(WHEN=(01,03,CH,EQ,C'001'),
                BUILD=(C'BASIC CASE',
                       642:X)),
         IFTHEN=(WHEN=(01,03,CH,EQ,C'002'),
                BUILD=(C'SIMPLE ''QUOTE',
                       642:X)),
         IFTHEN=(WHEN=(01,03,CH,EQ,C'003'),
                BUILD=(C'DOUBLE "QUOTE',
                       642:X)),
         IFTHEN=(WHEN=(01,03,CH,EQ,C'004'),
                BUILD=(01:642C'a'))
//*
//*--------------------------------------------------------------
//UTSORTA1 EXEC PGM=SORT
//SYSOUT   DD  SYSOUT=*
//*        FICHIER UNLOAD
//SORTIN   DD  DISP=SHR,
//*             DSN=Y896552.KPTRF029.UTSORTA1.INPUT
//            DSN=Y896552.KPTRF029.UTIKJE02.OMG.D240218
//SORTOUT  DD  DISP=(NEW,CATLG,DELETE),
//             DSN=Y896552.KPTRF029.UTSORTA1.OUTPUT,
//             RECFM=VB,LRECL=10400,DSORG=PS,
//             SPACE=(CYL,(99,25),RLSE),
//             MGMTCLAS=MA0030
//*
//SYSIN    DD  DISP=SHR,
//             DSN=Y896552.PTRGIT.MBE(PTRF029A)
//*SYSIN    DD  *
//*
//SYMNAMES DD  DISP=SHR,
//             DSN=Y896552.PTRGIT.MBE(PTRF029B)
//*
//
//*

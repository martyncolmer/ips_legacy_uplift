create database ips collate SQL_Latin1_General_CP1_CI_AS
go

create table AUDIT_LOG
(
  AUDIT_ID          numeric(10)    not null,
  ACTIONED_BY       nvarchar(20)   not null,
  ACTION            nvarchar(30)   not null,
  OBJECT            nvarchar(100)  not null,
  LOG_DATE          date           not null,
  AUDIT_LOG_DETAILS nvarchar(1000) not null
)
go

create table COLUMN_LOOKUP
(
  LOOKUP_COLUMN nvarchar(50)  not null,
  LOOKUP_KEY    numeric(2)    not null,
  DISPLAY_VALUE nvarchar(100) not null
)
go

create table DATA_SOURCE
(
  DATA_SOURCE_ID   numeric(10)  not null,
  DATA_SOURCE_NAME nvarchar(30) not null
)
go

create table DELTAS
(
  DELTA_NUMBER numeric(38)   not null,
  RUN_DATE     date          not null,
  BACKOUT_DATE date,
  DESCRIPTION  nvarchar(100) not null
)
go

create table EXPORT_COLUMN
(
  EXPORT_TYPE_ID  numeric(10)  not null,
  COLUMN_SOURCE   nvarchar(3)  not null,
  COLUMN_ORDER_NO numeric(4)   not null,
  COLUMN_DESC     nvarchar(30) not null,
  COLUMN_TYPE     nvarchar(20),
  COLUMN_LENGTH   numeric(38)
)
go

create table EXPORT_DATA_DETAILS
(
  ED_ID          nvarchar(40) not null,
  ED_NAME        nvarchar(30) not null,
  EXPORT_TYPE_ID numeric(10)  not null,
  FORMAT_ID      numeric(10)  not null,
  DATE_CREATED   date         not null,
  ED_STATUS      numeric(2)   not null,
  USER_ID        nvarchar(20) not null
)
go

create table EXPORT_DATA_DOWNLOAD
(
  RUN_ID            nvarchar(40) not null,
  DOWNLOADABLE_DATA varchar(max),
  FILENAME          nvarchar(40),
  SOURCE_TABLE      nvarchar(40),
  DATE_CREATED      smalldatetime
)
go

create table EXPORT_TYPE
(
  EXPORT_TYPE_ID   numeric(10)   not null,
  EXPORT_TYPE_NAME nvarchar(30)  not null,
  EXPORT_TYPE_DEF  nvarchar(max) not null
)
go

create table FORMAT_TYPE
(
  FORMAT_ID   numeric(10)    not null,
  FORMAT_NAME nvarchar(30)   not null,
  FORMAT_DEF  nvarchar(2000) not null
)
go

create table NON_RESPONSE_DATA
(
  YEAR           bigint,
  MONTH          bigint,
  DATA_SOURCE_ID bigint,
  PORTROUTE      bigint,
  WEEKDAY        bigint,
  ARRIVEDEPART   bigint,
  AM_PM_NIGHT    bigint,
  SAMPINTERVAL   bigint,
  MIGTOTAL       bigint,
  ORDTOTAL       bigint,
  RUN_ID         varchar(max)
)
go

create table PROCESS_NAME
(
  PN_ID        numeric(10)  not null,
  PROCESS_NAME nvarchar(30) not null
)
go

create table PROCESS_VARIABLE
(
  RUN_ID              nvarchar(40)   not null,
  PROCESS_VARIABLE_ID numeric(10)    not null,
  PV_NAME             nvarchar(30)   not null,
  PV_DESC             nvarchar(1000) not null,
  PV_DEF              nvarchar(max)  not null
)
go

create table PROCESS_VARIABLE_BACKUP
(
  RUN_ID              nvarchar(40)   not null,
  PROCESS_VARIABLE_ID numeric(10)    not null,
  PV_NAME             nvarchar(30)   not null,
  PV_DESC             nvarchar(1000) not null,
  PV_DEF              nvarchar(max)  not null
)
go

create table PROCESS_VARIABLE_LOG
(
  PROCESS_VARIABLE_ID numeric(10)    not null,
  PVL_DATE            date           not null,
  PVL_REASON          nvarchar(1000) not null
)
go

create table PROCESS_VARIABLE_PY
(
  RUN_ID              nvarchar(40)   not null,
  PROCESS_VARIABLE_ID numeric(10)    not null,
  PV_NAME             nvarchar(30)   not null,
  PV_DESC             nvarchar(1000) not null,
  PV_DEF              nvarchar(max)  not null
)
go

create table PROCESS_VARIABLE_PY_BACKUP
(
  RUN_ID              nvarchar(40)   not null,
  PROCESS_VARIABLE_ID numeric(10)    not null,
  PV_NAME             nvarchar(30)   not null,
  PV_DESC             nvarchar(1000) not null,
  PV_DEF              nvarchar(max)  not null
)
go

create table PROCESS_VARIABLE_PY_BACKUP_2
(
  RUN_ID              nvarchar(40)   not null,
  PROCESS_VARIABLE_ID numeric(10)    not null,
  PV_NAME             nvarchar(30)   not null,
  PV_DESC             nvarchar(1000) not null,
  PV_DEF              nvarchar(max)  not null
)
go

create table PROCESS_VARIABLE_SET
(
  RUN_ID     nvarchar(40) not null,
  NAME       nvarchar(30) not null,
  [USER]     nvarchar(50),
  START_DATE date         not null,
  END_DATE   date         not null
)
go

create table PROCESS_VARIABLE_TESTING
(
  RUN_ID              nvarchar(40)   not null,
  PROCESS_VARIABLE_ID numeric(10)    not null,
  PV_NAME             nvarchar(30)   not null,
  PV_DESC             nvarchar(1000) not null,
  PV_DEF              nvarchar(max)  not null
)
go

create table PS_FINAL
(
  RUN_ID            nvarchar(40) not null,
  SERIAL            numeric(15)  not null,
  SHIFT_WT          numeric(9, 3),
  NON_RESPONSE_WT   numeric(9, 3),
  MINS_WT           numeric(9, 3),
  TRAFFIC_WT        numeric(9, 3),
  UNSAMP_TRAFFIC_WT numeric(9, 3),
  IMBAL_WT          numeric(9, 3),
  FINAL_WT          numeric(12, 3)
)
go

create table PS_IMBALANCE
(
  RUN_ID       nvarchar(40) not null,
  FLOW         numeric(2),
  SUM_PRIOR_WT numeric(12, 3),
  SUM_IMBAL_WT numeric(12, 3)
)
go

create table PS_INSTRUCTION
(
  PN_ID          numeric(10)    not null,
  PS_INSTRUCTION nvarchar(2000) not null
)
go

create table PS_MINIMUMS
(
  RUN_ID                nvarchar(40) not null,
  MINS_PORT_GRP_PV      nvarchar(10),
  ARRIVEDEPART          numeric(1),
  MINS_CTRY_GRP_PV      numeric(6),
  MINS_NAT_GRP_PV       numeric(6),
  MINS_CTRY_PORT_GRP_PV nvarchar(10),
  MINS_CASES            numeric(6),
  FULLS_CASES           numeric(6),
  PRIOR_GROSS_MINS      numeric(12, 3),
  PRIOR_GROSS_FULLS     numeric(12, 3),
  PRIOR_GROSS_ALL       numeric(12, 3),
  MINS_WT               numeric(9, 3),
  POST_SUM              numeric(12, 3),
  CASES_CARRIED_FWD     numeric(6)
)
go

create table PS_NON_RESPONSE
(
  RUN_ID           nvarchar(40) not null,
  NR_PORT_GRP_PV   nvarchar(10) not null,
  ARRIVEDEPART     numeric(1)   not null,
  WEEKDAY_END_PV   numeric(1),
  MEAN_RESPS_SH_WT numeric(9, 3),
  COUNT_RESPS      numeric(6),
  PRIOR_SUM        numeric(12, 3),
  GROSS_RESP       numeric(12, 3),
  GNR              numeric(12, 3),
  MEAN_NR_WT       numeric(9, 3)
)
go

create table PS_SHIFT_DATA
(
  RUN_ID            nvarchar(40) not null,
  SHIFT_PORT_GRP_PV nvarchar(10) not null,
  ARRIVEDEPART      numeric(1)   not null,
  WEEKDAY_END_PV    numeric(1),
  AM_PM_NIGHT_PV    numeric(1),
  MIGSI             numeric(3),
  POSS_SHIFT_CROSS  numeric(5),
  SAMP_SHIFT_CROSS  numeric(5),
  MIN_SH_WT         numeric(9, 3),
  MEAN_SH_WT        numeric(9, 3),
  MAX_SH_WT         numeric(9, 3),
  COUNT_RESPS       numeric(6),
  SUM_SH_WT         numeric(12, 3)
)
go

create table PS_TRAFFIC
(
  RUN_ID             nvarchar(40) not null,
  SAMP_PORT_GRP_PV   nvarchar(10) not null,
  ARRIVEDEPART       numeric(1)   not null,
  FOOT_OR_VEHICLE_PV numeric(2),
  CASES              numeric(6),
  TRAFFICTOTAL       numeric(12, 3),
  SUM_TRAFFIC_WT     numeric(12, 3),
  TRAFFIC_WT         numeric(9, 3)
)
go

create table PS_UNSAMPLED_OOH
(
  RUN_ID                nvarchar(40) not null,
  UNSAMP_PORT_GRP_PV    nvarchar(10) not null,
  ARRIVEDEPART          numeric(1)   not null,
  UNSAMP_REGION_GRP_PV  nvarchar(10),
  CASES                 numeric(6),
  SUM_PRIOR_WT          numeric(12, 3),
  SUM_UNSAMP_TRAFFIC_WT numeric(12, 3),
  UNSAMP_TRAFFIC_WT     numeric(9, 3)
)
go

create table QUERY_RESPONSE
(
  TASK_ID       nvarchar(40) not null,
  RESPONSE_CODE nvarchar(10),
  RESPONSE_MSG  nvarchar(max)
)
go

create table RESPONSE
(
  RUN_ID        nvarchar(40) not null,
  STEP_NUMBER   int          not null,
  RESPONSE_CODE int          not null,
  MESSAGE       nvarchar(250),
  OUTPUT        nvarchar(4000),
  TIME_STAMP    datetime
)
go

create table RESPONSE_ARCHIVE
(
  RUN_ID        nvarchar(40) not null,
  STEP_NUMBER   int          not null,
  RESPONSE_CODE int          not null,
  MESSAGE       nvarchar(250),
  OUTPUT        nvarchar(4000),
  TIME_STAMP    datetime
)
go

create table RUN
(
  RUN_ID      nvarchar(40)  not null
    constraint RUN_pk
      primary key nonclustered,
  RUN_NAME    nvarchar(30)  not null,
  RUN_DESC    nvarchar(250) not null,
  USER_ID     nvarchar(20),
  START_DATE  date          not null,
  END_DATE    date          not null,
  RUN_STATUS  numeric(2)    not null,
  RUN_TYPE_ID numeric(3)
)
go

create unique index RUN_RUN_ID_uindex
  on RUN (RUN_ID)
go

create table RUN_DATA_MAP
(
  RUN_ID      nvarchar(40) not null
    constraint RUN_DATA_MAP_pk
      primary key nonclustered,
  VERSION_ID  numeric(10)  not null,
  DATA_SOURCE nvarchar(60) not null
)
go

create unique index RUN_DATA_MAP_RUN_ID_uindex
  on RUN_DATA_MAP (RUN_ID)
go

create table RUN_STEPS
(
  RUN_ID      nvarchar(40) not null,
  STEP_NUMBER numeric(2)   not null,
  STEP_NAME   nvarchar(80) not null,
  STEP_STATUS numeric(2)   not null
)
go

create table RUN_TYPE
(
  RUN_TYPE_ID         numeric(3)    not null,
  RUN_TYPE_NAME       nvarchar(30)  not null,
  RUN_TYPE_DEFINITION nvarchar(max) not null
)
go

create table SAS_AIR_MILES
(
  SERIAL    numeric(15) not null,
  DIRECTLEG numeric(6),
  OVLEG     numeric(6),
  UKLEG     numeric(6)
)
go

create table SAS_DATA_EXPORT
(
  SAS_PROCESS_ID numeric(10)    not null,
  SDE_LABEL      nvarchar(80)   not null,
  SDE_DATA       varbinary(max) not null
)
go

create table SAS_FARES_IMP
(
  SERIAL         numeric(15) not null,
  FARE           numeric(6),
  FAREK          numeric(2),
  SPEND          numeric(7),
  SPENDIMPREASON numeric(1)
)
go

create table SAS_FARES_SPV
(
  SERIAL                 numeric(15) not null,
  FARES_IMP_FLAG_PV      numeric(1),
  FARES_IMP_ELIGIBLE_PV  numeric(1),
  DISCNT_PACKAGE_COST_PV numeric(6),
  DISCNT_F1_PV           numeric(4, 3),
  DISCNT_F2_PV           numeric(4, 3),
  FAGE_PV                numeric(2),
  TYPE_PV                numeric(2),
  OPERA_PV               numeric(2),
  UKPORT1_PV             numeric(4),
  UKPORT2_PV             numeric(4),
  UKPORT3_PV             numeric(4),
  UKPORT4_PV             numeric(4),
  OSPORT1_PV             numeric(8),
  OSPORT2_PV             numeric(8),
  OSPORT3_PV             numeric(8),
  OSPORT4_PV             numeric(8),
  APD_PV                 numeric(4),
  QMFARE_PV              numeric(8),
  DUTY_FREE_PV           numeric(4)
)
go

create table SAS_FINAL_WT
(
  SERIAL   numeric(15) not null,
  FINAL_WT numeric(12, 3)
)
go

create table SAS_IMBALANCE_SPV
(
  SERIAL               numeric(15) not null,
  IMBAL_PORT_GRP_PV    numeric(3),
  IMBAL_PORT_SUBGRP_PV numeric(3),
  IMBAL_PORT_FACT_PV   numeric(5, 3),
  IMBAL_CTRY_GRP_PV    numeric(3),
  IMBAL_CTRY_FACT_PV   numeric(5, 3),
  IMBAL_ELIGIBLE_PV    numeric(1),
  PURPOSE_PV           numeric(8),
  FLOW_PV              numeric(2)
)
go

create table SAS_IMBALANCE_WT
(
  SERIAL   numeric(15) not null,
  IMBAL_WT numeric(9, 3)
)
go

create table SAS_MINIMUMS_SPV
(
  SERIAL                numeric(15) not null,
  MINS_PORT_GRP_PV      nvarchar(10),
  MINS_CTRY_GRP_PV      numeric(6),
  MINS_NAT_GRP_PV       numeric(6),
  MINS_CTRY_PORT_GRP_PV nvarchar(10),
  MINS_QUALITY_PV       numeric(1),
  MINS_FLAG_PV          numeric(1)
)
go

create table SAS_MINIMUMS_WT
(
  SERIAL  numeric(15) not null,
  MINS_WT numeric(9, 3)
)
go

create table SAS_NON_RESPONSE_DATA
(
  REC_ID         int identity,
  PORTROUTE      numeric(4) not null,
  WEEKDAY        numeric(1),
  ARRIVEDEPART   numeric(1),
  AM_PM_NIGHT    numeric(1),
  SAMPINTERVAL   numeric(4),
  MIGTOTAL       numeric(10),
  ORDTOTAL       numeric(10),
  NR_PORT_GRP_PV nvarchar(10),
  WEEKDAY_END_PV numeric(1),
  AM_PM_NIGHT_PV numeric(1)
)
go

create table SAS_NON_RESPONSE_PV
(
  REC_ID         numeric(10) not null,
  WEEKDAY_END_PV numeric(1),
  NR_PORT_GRP_PV nvarchar(10)
)
go

create table SAS_NON_RESPONSE_SPV
(
  SERIAL         numeric(15) not null,
  NR_PORT_GRP_PV nvarchar(10),
  MIG_FLAG_PV    numeric(1),
  NR_FLAG_PV     numeric(1)
)
go

create table SAS_NON_RESPONSE_WT
(
  SERIAL          numeric(15) not null,
  NON_RESPONSE_WT numeric(9, 3)
)
go

create table SAS_PARAMETERS
(
  PARAMETER_SET_ID numeric(10)  not null,
  PARAMETER_NAME   nvarchar(32) not null,
  PARAMETER_VALUE  nvarchar(4000)
)
go

create table SAS_PROCESS_VARIABLE
(
  PROCVAR_NAME  nvarchar(30)  not null,
  PROCVAR_RULE  nvarchar(max) not null,
  PROCVAR_ORDER numeric(2)    not null
)
go

create table SAS_PS_FINAL
(
  SERIAL            numeric(15) not null,
  SHIFT_WT          numeric(9, 3),
  NON_RESPONSE_WT   numeric(9, 3),
  MINS_WT           numeric(9, 3),
  TRAFFIC_WT        numeric(9, 3),
  UNSAMP_TRAFFIC_WT numeric(9, 3),
  IMBAL_WT          numeric(9, 3),
  FINAL_WT          numeric(12, 3)
)
go

create table SAS_PS_IMBALANCE
(
  FLOW         numeric(2),
  SUM_PRIOR_WT numeric(12, 3),
  SUM_IMBAL_WT numeric(12, 3)
)
go

create table SAS_PS_MINIMUMS
(
  MINS_PORT_GRP_PV      nvarchar(10),
  ARRIVEDEPART          numeric(1),
  MINS_CTRY_GRP_PV      numeric(6),
  MINS_NAT_GRP_PV       numeric(6),
  MINS_CTRY_PORT_GRP_PV nvarchar(10),
  MINS_CASES            numeric(6),
  FULLS_CASES           numeric(6),
  PRIOR_GROSS_MINS      numeric(12, 3),
  PRIOR_GROSS_FULLS     numeric(12, 3),
  PRIOR_GROSS_ALL       numeric(12, 3),
  MINS_WT               numeric(9, 3),
  POST_SUM              numeric(12, 3),
  CASES_CARRIED_FWD     numeric(6)
)
go

create table SAS_PS_NON_RESPONSE
(
  NR_PORT_GRP_PV   nvarchar(10) not null,
  ARRIVEDEPART     numeric(1)   not null,
  WEEKDAY_END_PV   numeric(1),
  MEAN_RESPS_SH_WT numeric(9, 3),
  COUNT_RESPS      numeric(6),
  PRIOR_SUM        numeric(12, 3),
  GROSS_RESP       numeric(12, 3),
  GNR              numeric(12, 3),
  MEAN_NR_WT       numeric(9, 3)
)
go

create table SAS_PS_SHIFT_DATA
(
  SHIFT_PORT_GRP_PV nvarchar(10) not null,
  ARRIVEDEPART      numeric(1)   not null,
  WEEKDAY_END_PV    numeric(1),
  AM_PM_NIGHT_PV    numeric(1),
  MIGSI             numeric(3),
  POSS_SHIFT_CROSS  numeric(5),
  SAMP_SHIFT_CROSS  numeric(5),
  MIN_SH_WT         numeric(9, 3),
  MEAN_SH_WT        numeric(9, 3),
  MAX_SH_WT         numeric(9, 3),
  COUNT_RESPS       numeric(6),
  SUM_SH_WT         numeric(12, 3)
)
go

create table SAS_PS_TRAFFIC
(
  SAMP_PORT_GRP_PV   nvarchar(10) not null,
  ARRIVEDEPART       numeric(1)   not null,
  FOOT_OR_VEHICLE_PV numeric(2),
  CASES              numeric(6),
  TRAFFICTOTAL       numeric(12, 3),
  SUM_TRAFFIC_WT     numeric(12, 3),
  TRAFFIC_WT         numeric(9, 3)
)
go

create table SAS_PS_UNSAMPLED_OOH
(
  UNSAMP_PORT_GRP_PV    nvarchar(10) not null,
  ARRIVEDEPART          numeric(1)   not null,
  UNSAMP_REGION_GRP_PV  nvarchar(10),
  CASES                 numeric(6),
  SUM_PRIOR_WT          numeric(12, 3),
  SUM_UNSAMP_TRAFFIC_WT numeric(12, 3),
  UNSAMP_TRAFFIC_WT     numeric(9, 3)
)
go

create table SAS_RAIL_IMP
(
  SERIAL numeric(15) not null,
  SPEND  numeric(7)
)
go

create table SAS_RAIL_SPV
(
  SERIAL               numeric(15) not null,
  RAIL_CNTRY_GRP_PV    numeric(3),
  RAIL_EXERCISE_PV     numeric(6),
  RAIL_IMP_ELIGIBLE_PV numeric(1)
)
go

create table SAS_REGIONAL_IMP
(
  SERIAL          numeric(15) not null,
  VISIT_WT        numeric(6, 3),
  STAY_WT         numeric(6, 3),
  EXPENDITURE_WT  numeric(6, 3),
  VISIT_WTK       nvarchar(10),
  STAY_WTK        nvarchar(10),
  EXPENDITURE_WTK nvarchar(10),
  NIGHTS1         numeric(3),
  NIGHTS2         numeric(3),
  NIGHTS3         numeric(3),
  NIGHTS4         numeric(3),
  NIGHTS5         numeric(3),
  NIGHTS6         numeric(3),
  NIGHTS7         numeric(3),
  NIGHTS8         numeric(3),
  STAY1K          nvarchar(10),
  STAY2K          nvarchar(10),
  STAY3K          nvarchar(10),
  STAY4K          nvarchar(10),
  STAY5K          nvarchar(10),
  STAY6K          nvarchar(10),
  STAY7K          nvarchar(10),
  STAY8K          nvarchar(10)
)
go

create table SAS_REGIONAL_SPV
(
  SERIAL               numeric(15) not null,
  PURPOSE_PV           numeric(8),
  STAYIMPCTRYLEVEL1_PV numeric(8),
  STAYIMPCTRYLEVEL2_PV numeric(8),
  STAYIMPCTRYLEVEL3_PV numeric(8),
  STAYIMPCTRYLEVEL4_PV numeric(8),
  REG_IMP_ELIGIBLE_PV  numeric(1)
)
go

create table SAS_RESPONSE
(
  SAS_PROCESS_ID numeric(10) not null,
  RESPONSE_CODE  numeric(5)  not null,
  ERROR_MSG      nvarchar(250),
  STACK_TRACE    nvarchar(4000),
  WARNINGS       nvarchar(4000),
  TIME_STAMP     datetime
)
go

create table SAS_SHIFT_DATA
(
  REC_ID            int identity,
  PORTROUTE         numeric(4)  not null,
  WEEKDAY           numeric(1)  not null,
  ARRIVEDEPART      numeric(1)  not null,
  TOTAL             numeric(10) not null,
  AM_PM_NIGHT       numeric(1)  not null,
  SHIFT_PORT_GRP_PV nvarchar(10),
  AM_PM_NIGHT_PV    numeric(1),
  WEEKDAY_END_PV    numeric(1)
)
go

create table SAS_SHIFT_DATA_RICER
(
  REC_ID            numeric(10) not null,
  PORTROUTE         numeric(4)  not null,
  WEEKDAY           numeric(1)  not null,
  ARRIVEDEPART      numeric(1)  not null,
  TOTAL             numeric(10) not null,
  AM_PM_NIGHT       numeric(1)  not null,
  SHIFT_PORT_GRP_PV nvarchar(10),
  AM_PM_NIGHT_PV    numeric(1),
  WEEKDAY_END_PV    numeric(1)
)
go

create table SAS_SHIFT_PV
(
  REC_ID            numeric(10) not null,
  SHIFT_PORT_GRP_PV nvarchar(10),
  AM_PM_NIGHT_PV    numeric(1),
  WEEKDAY_END_PV    numeric(1)
)
go

create table SAS_SHIFT_SPV
(
  SERIAL            numeric(15) not null,
  SHIFT_PORT_GRP_PV nvarchar(10),
  AM_PM_NIGHT_PV    numeric(1),
  WEEKDAY_END_PV    numeric(1),
  SHIFT_FLAG_PV     numeric(1),
  CROSSINGS_FLAG_PV numeric(1)
)
go

create table SAS_SHIFT_WT
(
  SERIAL   numeric(15) not null,
  SHIFT_WT numeric(9, 3)
)
go

create table SAS_SPEND_IMP
(
  SERIAL   numeric(15) not null,
  NEWSPEND numeric(7),
  SPENDK   numeric(2)
)
go

create table SAS_SPEND_SPV
(
  SERIAL                numeric(15) not null,
  SPEND_IMP_FLAG_PV     numeric(1),
  SPEND_IMP_ELIGIBLE_PV numeric(1),
  UK_OS_PV              numeric(2),
  PUR1_PV               numeric(8),
  PUR2_PV               numeric(8),
  PUR3_PV               numeric(8),
  DUR1_PV               numeric(8),
  DUR2_PV               numeric(8)
)
go

create table SAS_STAY_IMP
(
  SERIAL numeric(15) not null,
  STAY   numeric(3),
  STAYK  numeric(1)
)
go

create table SAS_STAY_SPV
(
  SERIAL               numeric(15) not null,
  STAY_IMP_FLAG_PV     numeric(1),
  STAY_IMP_ELIGIBLE_PV numeric(1),
  STAYIMPCTRYLEVEL1_PV numeric(8),
  STAYIMPCTRYLEVEL2_PV numeric(8),
  STAYIMPCTRYLEVEL3_PV numeric(8),
  STAYIMPCTRYLEVEL4_PV numeric(8),
  STAY_PURPOSE_GRP_PV  numeric(2)
)
go

create table SAS_SURVEY_COLUMN
(
  VERSION_ID    numeric(10)  not null,
  COLUMN_NO     numeric(4)   not null,
  COLUMN_DESC   nvarchar(30) not null,
  COLUMN_TYPE   nvarchar(20) not null,
  COLUMN_LENGTH numeric(5)   not null
)
go

create table SAS_SURVEY_SUBSAMPLE
(
  RUN_ID                 nvarchar(40) not null,
  SERIAL                 numeric(15)  not null,
  AGE                    numeric(3),
  AM_PM_NIGHT            numeric(1),
  ANYUNDER16             numeric(2),
  APORTLATDEG            numeric(2),
  APORTLATMIN            numeric(2),
  APORTLATSEC            numeric(2),
  APORTLATNS             nchar(1),
  APORTLONDEG            numeric(3),
  APORTLONMIN            numeric(2),
  APORTLONSEC            numeric(2),
  APORTLONEW             nchar(1),
  ARRIVEDEPART           numeric(1),
  BABYFARE               numeric(4, 2),
  BEFAF                  numeric(6),
  CHANGECODE             numeric(6),
  CHILDFARE              numeric(4, 2),
  COUNTRYVISIT           numeric(4),
  CPORTLATDEG            numeric(2),
  CPORTLATMIN            numeric(2),
  CPORTLATSEC            numeric(2),
  CPORTLATNS             nchar(1),
  CPORTLONDEG            numeric(3),
  CPORTLONMIN            numeric(2),
  CPORTLONSEC            numeric(2),
  CPORTLONEW             nchar(1),
  INTDATE                nchar(8),
  DAYTYPE                numeric(1),
  DIRECTLEG              numeric(6),
  DVEXPEND               numeric(6),
  DVFARE                 numeric(6),
  DVLINECODE             numeric(6),
  DVPACKAGE              numeric(1),
  DVPACKCOST             numeric(6),
  DVPERSONS              numeric(3),
  DVPORTCODE             numeric(6),
  EXPENDCODE             nvarchar(4),
  EXPENDITURE            numeric(6),
  FARE                   numeric(6),
  FAREK                  numeric(2),
  FLOW                   numeric(2),
  HAULKEY                numeric(2),
  INTENDLOS              numeric(2),
  KIDAGE                 numeric(2),
  LOSKEY                 numeric(2),
  MAINCONTRA             numeric(1),
  MIGSI                  numeric(3),
  INTMONTH               numeric(2),
  NATIONALITY            numeric(4),
  NATIONNAME             nvarchar(50),
  NIGHTS1                numeric(3),
  NIGHTS2                numeric(3),
  NIGHTS3                numeric(3),
  NIGHTS4                numeric(3),
  NIGHTS5                numeric(3),
  NIGHTS6                numeric(3),
  NIGHTS7                numeric(3),
  NIGHTS8                numeric(3),
  NUMADULTS              numeric(3),
  NUMDAYS                numeric(3),
  NUMNIGHTS              numeric(3),
  NUMPEOPLE              numeric(3),
  PACKAGEHOL             numeric(1),
  PACKAGEHOLUK           numeric(1),
  PERSONS                numeric(2),
  PORTROUTE              numeric(4),
  PACKAGE                numeric(2),
  PROUTELATDEG           numeric(2),
  PROUTELATMIN           numeric(2),
  PROUTELATSEC           numeric(2),
  PROUTELATNS            nchar(1),
  PROUTELONDEG           numeric(3),
  PROUTELONMIN           numeric(2),
  PROUTELONSEC           numeric(2),
  PROUTELONEW            nchar(1),
  PURPOSE                numeric(2),
  QUARTER                numeric(1),
  RESIDENCE              numeric(4),
  RESPNSE                numeric(2),
  SEX                    numeric(1),
  SHIFTNO                numeric(6),
  SHUTTLE                numeric(1),
  SINGLERETURN           numeric(1),
  TANDTSI                numeric(8),
  TICKETCOST             numeric(6),
  TOWNCODE1              numeric(6),
  TOWNCODE2              numeric(6),
  TOWNCODE3              numeric(6),
  TOWNCODE4              numeric(6),
  TOWNCODE5              numeric(6),
  TOWNCODE6              numeric(6),
  TOWNCODE7              numeric(6),
  TOWNCODE8              numeric(6),
  TRANSFER               numeric(6),
  UKFOREIGN              numeric(1),
  VEHICLE                numeric(1),
  VISITBEGAN             numeric(8),
  WELSHNIGHTS            numeric(3),
  WELSHTOWN              numeric(6),
  AM_PM_NIGHT_PV         numeric(1),
  APD_PV                 numeric(4),
  ARRIVEDEPART_PV        numeric(1),
  CROSSINGS_FLAG_PV      numeric(1),
  STAYIMPCTRYLEVEL1_PV   numeric(8),
  STAYIMPCTRYLEVEL2_PV   numeric(8),
  STAYIMPCTRYLEVEL3_PV   numeric(8),
  STAYIMPCTRYLEVEL4_PV   numeric(8),
  DAY_PV                 numeric(2),
  DISCNT_F1_PV           numeric(4, 3),
  DISCNT_F2_PV           numeric(4, 3),
  DISCNT_PACKAGE_COST_PV numeric(6),
  DUR1_PV                numeric(3),
  DUR2_PV                numeric(3),
  DUTY_FREE_PV           numeric(4),
  FAGE_PV                numeric(2),
  FARES_IMP_ELIGIBLE_PV  numeric(1),
  FARES_IMP_FLAG_PV      numeric(1),
  FLOW_PV                numeric(2),
  FOOT_OR_VEHICLE_PV     numeric(2),
  HAUL_PV                nvarchar(2),
  IMBAL_CTRY_FACT_PV     numeric(5, 3),
  IMBAL_CTRY_GRP_PV      numeric(3),
  IMBAL_ELIGIBLE_PV      numeric(1),
  IMBAL_PORT_FACT_PV     numeric(5, 3),
  IMBAL_PORT_GRP_PV      numeric(3),
  IMBAL_PORT_SUBGRP_PV   numeric(3),
  LOS_PV                 numeric(3),
  LOSDAYS_PV             numeric(3),
  MIG_FLAG_PV            numeric(1),
  MINS_CTRY_GRP_PV       numeric(6),
  MINS_CTRY_PORT_GRP_PV  nvarchar(10),
  MINS_FLAG_PV           numeric(1),
  MINS_NAT_GRP_PV        numeric(6),
  MINS_PORT_GRP_PV       nvarchar(6),
  MINS_QUALITY_PV        numeric(1),
  NR_FLAG_PV             numeric(1),
  NR_PORT_GRP_PV         nvarchar(10),
  OPERA_PV               numeric(2),
  OSPORT1_PV             numeric(8),
  OSPORT2_PV             numeric(8),
  OSPORT3_PV             numeric(8),
  OSPORT4_PV             numeric(8),
  PUR1_PV                numeric(8),
  PUR2_PV                numeric(8),
  PUR3_PV                numeric(8),
  PURPOSE_PV             numeric(8),
  QMFARE_PV              numeric(8),
  RAIL_CNTRY_GRP_PV      numeric(3),
  RAIL_EXERCISE_PV       numeric(6),
  RAIL_IMP_ELIGIBLE_PV   numeric(1),
  REG_IMP_ELIGIBLE_PV    numeric(1),
  SAMP_PORT_GRP_PV       nvarchar(10),
  SHIFT_FLAG_PV          numeric(1),
  SHIFT_PORT_GRP_PV      nvarchar(10),
  SPEND_IMP_FLAG_PV      numeric(1),
  SPEND_IMP_ELIGIBLE_PV  numeric(1),
  STAY_IMP_ELIGIBLE_PV   numeric(1),
  STAY_IMP_FLAG_PV       numeric(1),
  STAY_PURPOSE_GRP_PV    numeric(2),
  TOWNCODE_PV            nvarchar(10),
  TOWN_IMP_ELIGIBLE_PV   numeric(1),
  TYPE_PV                numeric(2),
  UK_OS_PV               numeric(1),
  UKPORT1_PV             numeric(4),
  UKPORT2_PV             numeric(4),
  UKPORT3_PV             numeric(4),
  UKPORT4_PV             numeric(4),
  UNSAMP_PORT_GRP_PV     nvarchar(10),
  UNSAMP_REGION_GRP_PV   nvarchar(10),
  WEEKDAY_END_PV         numeric(1),
  DIRECT                 numeric(6),
  EXPENDITURE_WT         numeric(6, 3),
  EXPENDITURE_WTK        nvarchar(10),
  FAREKEY                nvarchar(5),
  OVLEG                  numeric(6),
  SPEND                  numeric(7),
  SPEND1                 numeric(7),
  SPEND2                 numeric(7),
  SPEND3                 numeric(7),
  SPEND4                 numeric(7),
  SPEND5                 numeric(7),
  SPEND6                 numeric(7),
  SPEND7                 numeric(7),
  SPEND8                 numeric(7),
  SPEND9                 numeric(7),
  SPENDIMPREASON         numeric(1),
  SPENDK                 numeric(2),
  STAY                   numeric(3),
  STAYK                  numeric(1),
  STAY1K                 nvarchar(10),
  STAY2K                 nvarchar(10),
  STAY3K                 nvarchar(10),
  STAY4K                 nvarchar(10),
  STAY5K                 nvarchar(10),
  STAY6K                 nvarchar(10),
  STAY7K                 nvarchar(10),
  STAY8K                 nvarchar(10),
  STAY9K                 nvarchar(10),
  STAYTLY                numeric(6),
  STAY_WT                numeric(6, 3),
  STAY_WTK               nvarchar(10),
  TYPEINTERVIEW          numeric(3),
  UKLEG                  numeric(6),
  VISIT_WT               numeric(6, 3),
  VISIT_WTK              nvarchar(10),
  SHIFT_WT               numeric(9, 3),
  NON_RESPONSE_WT        numeric(9, 3),
  MINS_WT                numeric(9, 3),
  TRAFFIC_WT             numeric(9, 3),
  UNSAMP_TRAFFIC_WT      numeric(9, 3),
  IMBAL_WT               numeric(9, 3),
  FINAL_WT               numeric(12, 3)
)
go

create table SAS_SURVEY_VALUE
(
  VERSION_ID   numeric(10)   not null,
  SERIAL_NO    numeric(15)   not null,
  COLUMN_NO    numeric(4)    not null,
  COLUMN_VALUE nvarchar(100) not null
)
go

create table SAS_TOWN_STAY_IMP
(
  SERIAL numeric(15) not null,
  SPEND1 numeric(7),
  SPEND2 numeric(7),
  SPEND3 numeric(7),
  SPEND4 numeric(7),
  SPEND5 numeric(7),
  SPEND6 numeric(7),
  SPEND7 numeric(7),
  SPEND8 numeric(7)
)
go

create table SAS_TOWN_STAY_SPV
(
  SERIAL               numeric(15) not null,
  PURPOSE_PV           numeric(8),
  STAYIMPCTRYLEVEL1_PV numeric(8),
  STAYIMPCTRYLEVEL2_PV numeric(8),
  STAYIMPCTRYLEVEL3_PV numeric(8),
  STAYIMPCTRYLEVEL4_PV numeric(8),
  TOWN_IMP_ELIGIBLE_PV numeric(1)
)
go

create table SAS_TRAFFIC_DATA
(
  REC_ID             int identity,
  PORTROUTE          numeric(4),
  ARRIVEDEPART       numeric(1),
  TRAFFICTOTAL       numeric(10),
  PERIODSTART        nvarchar(10),
  PERIODEND          nvarchar(10),
  AM_PM_NIGHT        numeric(1),
  HAUL               nvarchar(2),
  VEHICLE            numeric(1),
  SAMP_PORT_GRP_PV   nvarchar(10),
  FOOT_OR_VEHICLE_PV numeric(2),
  HAUL_PV            nvarchar(2)
)
go

create table SAS_TRAFFIC_PV
(
  REC_ID             numeric(10) not null,
  SAMP_PORT_GRP_PV   nvarchar(10),
  FOOT_OR_VEHICLE_PV numeric(2),
  HAUL_PV            nvarchar(2)
)
go

create table SAS_TRAFFIC_SPV
(
  SERIAL             numeric(15) not null,
  SAMP_PORT_GRP_PV   nvarchar(10),
  FOOT_OR_VEHICLE_PV numeric(2),
  HAUL_PV            nvarchar(2)
)
go

create table SAS_TRAFFIC_WT
(
  SERIAL     numeric(15) not null,
  TRAFFIC_WT numeric(9, 3)
)
go

create table SAS_UNSAMPLED_OOH_DATA
(
  REC_ID               int identity,
  PORTROUTE            numeric(4),
  REGION               numeric(3),
  ARRIVEDEPART         numeric(1),
  UNSAMP_TOTAL         numeric(10),
  UNSAMP_PORT_GRP_PV   nvarchar(10),
  UNSAMP_REGION_GRP_PV nvarchar(10)
)
go

create table SAS_UNSAMPLED_OOH_PV
(
  REC_ID               numeric(10)  not null,
  UNSAMP_PORT_GRP_PV   nvarchar(10) not null,
  UNSAMP_REGION_GRP_PV nvarchar(10) not null,
  HAUL_PV              nvarchar(2)
)
go

create table SAS_UNSAMPLED_OOH_SPV
(
  SERIAL               numeric(15) not null,
  UNSAMP_PORT_GRP_PV   nvarchar(10),
  UNSAMP_REGION_GRP_PV nvarchar(10),
  HAUL_PV              nvarchar(2)
)
go

create table SAS_UNSAMPLED_OOH_WT
(
  SERIAL            numeric(15) not null,
  UNSAMP_TRAFFIC_WT numeric(9, 3)
)
go

create table SERIALISED_RUN
(
  RUN_ID  nvarchar(40)   not null,
  SER_OBJ varbinary(max) not null
)
go

create table SERIALISED_WORKFLOW
(
  WORKFLOW_ID nvarchar(40)   not null,
  SER_OBJ     varbinary(max) not null
)
go

create table SHIFT_DATA
(
  YEAR           bigint,
  MONTH          bigint,
  DATA_SOURCE_ID bigint,
  PORTROUTE      bigint,
  WEEKDAY        bigint,
  ARRIVEDEPART   bigint,
  TOTAL          bigint,
  AM_PM_NIGHT    bigint,
  RUN_ID         varchar(max)
)
go

create table SPSS_METADATA
(
  NAME   nvarchar(30),
  TYPE   nvarchar(30),
  LENGTH numeric(3)
)
go

create table SQL_QUERY
(
  TASK_ID       nvarchar(40)  not null,
  QUERY_STRING  nvarchar(max) not null,
  QUERY_MESSAGE nvarchar(4000)
)
go

create table STATE_MAINTENANCE
(
  STATE_ID    numeric(10) not null,
  USER_ID     nvarchar(20),
  WORKFLOW_ID numeric(10) not null,
  ACTION      nvarchar(30),
  OBJECT      nvarchar(100),
  STATUS      numeric(1),
  COMMENTS    nvarchar(500)
)
go

create table STEP
(
  STEP_ID         nvarchar(40)  not null,
  STEP_DEFINITION nvarchar(max) not null,
  STEP_MESSAGE    nvarchar(4000)
)
go

create table SURVEY_COLUMN
(
  VERSION_ID    numeric(10)  not null,
  COLUMN_NO     numeric(4)   not null,
  COLUMN_DESC   nvarchar(30) not null,
  COLUMN_TYPE   nvarchar(20) not null,
  COLUMN_LENGTH numeric(5)   not null
)
go

create table SURVEY_SUBSAMPLE
(
  RUN_ID                 nvarchar(40) not null,
  SERIAL                 numeric(15)  not null,
  AGE                    numeric(3),
  AM_PM_NIGHT            numeric(1),
  ANYUNDER16             numeric(2),
  APORTLATDEG            numeric(2),
  APORTLATMIN            numeric(2),
  APORTLATSEC            numeric(2),
  APORTLATNS             nchar(1),
  APORTLONDEG            numeric(3),
  APORTLONMIN            numeric(2),
  APORTLONSEC            numeric(2),
  APORTLONEW             nchar(1),
  ARRIVEDEPART           numeric(1),
  BABYFARE               numeric(4, 2),
  BEFAF                  numeric(6),
  CHANGECODE             numeric(6),
  CHILDFARE              numeric(4, 2),
  COUNTRYVISIT           numeric(4),
  CPORTLATDEG            numeric(2),
  CPORTLATMIN            numeric(2),
  CPORTLATSEC            numeric(2),
  CPORTLATNS             nchar(1),
  CPORTLONDEG            numeric(3),
  CPORTLONMIN            numeric(2),
  CPORTLONSEC            numeric(2),
  CPORTLONEW             nchar(1),
  INTDATE                nchar(8),
  DAYTYPE                numeric(1),
  DIRECTLEG              numeric(6),
  DVEXPEND               numeric(6),
  DVFARE                 numeric(6),
  DVLINECODE             numeric(6),
  DVPACKAGE              numeric(1),
  DVPACKCOST             numeric(6),
  DVPERSONS              numeric(3),
  DVPORTCODE             numeric(6),
  EXPENDCODE             nvarchar(4),
  EXPENDITURE            numeric(6),
  FARE                   numeric(6),
  FAREK                  numeric(2),
  FLOW                   numeric(2),
  HAULKEY                numeric(2),
  INTENDLOS              numeric(2),
  KIDAGE                 numeric(2),
  LOSKEY                 numeric(2),
  MAINCONTRA             numeric(1),
  MIGSI                  numeric(3),
  INTMONTH               numeric(2),
  NATIONALITY            numeric(4),
  NATIONNAME             nvarchar(50),
  NIGHTS1                numeric(3),
  NIGHTS2                numeric(3),
  NIGHTS3                numeric(3),
  NIGHTS4                numeric(3),
  NIGHTS5                numeric(3),
  NIGHTS6                numeric(3),
  NIGHTS7                numeric(3),
  NIGHTS8                numeric(3),
  NUMADULTS              numeric(3),
  NUMDAYS                numeric(3),
  NUMNIGHTS              numeric(3),
  NUMPEOPLE              numeric(3),
  PACKAGEHOL             numeric(1),
  PACKAGEHOLUK           numeric(1),
  PERSONS                numeric(2),
  PORTROUTE              numeric(4),
  PACKAGE                numeric(2),
  PROUTELATDEG           numeric(2),
  PROUTELATMIN           numeric(2),
  PROUTELATSEC           numeric(2),
  PROUTELATNS            nchar(1),
  PROUTELONDEG           numeric(3),
  PROUTELONMIN           numeric(2),
  PROUTELONSEC           numeric(2),
  PROUTELONEW            nchar(1),
  PURPOSE                numeric(2),
  QUARTER                numeric(1),
  RESIDENCE              numeric(4),
  RESPNSE                numeric(2),
  SEX                    numeric(1),
  SHIFTNO                numeric(6),
  SHUTTLE                numeric(1),
  SINGLERETURN           numeric(1),
  TANDTSI                numeric(8),
  TICKETCOST             numeric(6),
  TOWNCODE1              numeric(6),
  TOWNCODE2              numeric(6),
  TOWNCODE3              numeric(6),
  TOWNCODE4              numeric(6),
  TOWNCODE5              numeric(6),
  TOWNCODE6              numeric(6),
  TOWNCODE7              numeric(6),
  TOWNCODE8              numeric(6),
  TRANSFER               numeric(6),
  UKFOREIGN              numeric(1),
  VEHICLE                numeric(1),
  VISITBEGAN             numeric(8),
  WELSHNIGHTS            numeric(3),
  WELSHTOWN              numeric(6),
  AM_PM_NIGHT_PV         numeric(1),
  APD_PV                 numeric(4),
  ARRIVEDEPART_PV        numeric(1),
  CROSSINGS_FLAG_PV      numeric(1),
  STAYIMPCTRYLEVEL1_PV   numeric(8),
  STAYIMPCTRYLEVEL2_PV   numeric(8),
  STAYIMPCTRYLEVEL3_PV   numeric(8),
  STAYIMPCTRYLEVEL4_PV   numeric(8),
  DAY_PV                 numeric(2),
  DISCNT_F1_PV           numeric(4, 3),
  DISCNT_F2_PV           numeric(4, 3),
  DISCNT_PACKAGE_COST_PV numeric(6),
  DUR1_PV                numeric(3),
  DUR2_PV                numeric(3),
  DUTY_FREE_PV           numeric(4),
  FAGE_PV                numeric(2),
  FARES_IMP_ELIGIBLE_PV  numeric(1),
  FARES_IMP_FLAG_PV      numeric(1),
  FLOW_PV                numeric(2),
  FOOT_OR_VEHICLE_PV     numeric(2),
  HAUL_PV                nvarchar(2),
  IMBAL_CTRY_FACT_PV     numeric(5, 3),
  IMBAL_CTRY_GRP_PV      numeric(3),
  IMBAL_ELIGIBLE_PV      numeric(1),
  IMBAL_PORT_FACT_PV     numeric(5, 3),
  IMBAL_PORT_GRP_PV      numeric(3),
  IMBAL_PORT_SUBGRP_PV   numeric(3),
  LOS_PV                 numeric(3),
  LOSDAYS_PV             numeric(3),
  MIG_FLAG_PV            numeric(1),
  MINS_CTRY_GRP_PV       numeric(6),
  MINS_CTRY_PORT_GRP_PV  nvarchar(10),
  MINS_FLAG_PV           numeric(1),
  MINS_NAT_GRP_PV        numeric(6),
  MINS_PORT_GRP_PV       nvarchar(6),
  MINS_QUALITY_PV        numeric(1),
  NR_FLAG_PV             numeric(1),
  NR_PORT_GRP_PV         nvarchar(10),
  OPERA_PV               numeric(2),
  OSPORT1_PV             numeric(8),
  OSPORT2_PV             numeric(8),
  OSPORT3_PV             numeric(8),
  OSPORT4_PV             numeric(8),
  PUR1_PV                numeric(8),
  PUR2_PV                numeric(8),
  PUR3_PV                numeric(8),
  PURPOSE_PV             numeric(8),
  QMFARE_PV              numeric(8),
  RAIL_CNTRY_GRP_PV      numeric(3),
  RAIL_EXERCISE_PV       numeric(6),
  RAIL_IMP_ELIGIBLE_PV   numeric(1),
  REG_IMP_ELIGIBLE_PV    numeric(1),
  SAMP_PORT_GRP_PV       nvarchar(10),
  SHIFT_FLAG_PV          numeric(1),
  SHIFT_PORT_GRP_PV      nvarchar(10),
  SPEND_IMP_FLAG_PV      numeric(1),
  SPEND_IMP_ELIGIBLE_PV  numeric(1),
  STAY_IMP_ELIGIBLE_PV   numeric(1),
  STAY_IMP_FLAG_PV       numeric(1),
  STAY_PURPOSE_GRP_PV    numeric(2),
  TOWNCODE_PV            nvarchar(10),
  TOWN_IMP_ELIGIBLE_PV   numeric(1),
  TYPE_PV                numeric(2),
  UK_OS_PV               numeric(1),
  UKPORT1_PV             numeric(4),
  UKPORT2_PV             numeric(4),
  UKPORT3_PV             numeric(4),
  UKPORT4_PV             numeric(4),
  UNSAMP_PORT_GRP_PV     nvarchar(10),
  UNSAMP_REGION_GRP_PV   nvarchar(10),
  WEEKDAY_END_PV         numeric(1),
  DIRECT                 numeric(6),
  EXPENDITURE_WT         numeric(6, 3),
  EXPENDITURE_WTK        nvarchar(10),
  FAREKEY                nvarchar(5),
  OVLEG                  numeric(6),
  SPEND                  numeric(7),
  SPEND1                 numeric(7),
  SPEND2                 numeric(7),
  SPEND3                 numeric(7),
  SPEND4                 numeric(7),
  SPEND5                 numeric(7),
  SPEND6                 numeric(7),
  SPEND7                 numeric(7),
  SPEND8                 numeric(7),
  SPEND9                 numeric(7),
  SPENDIMPREASON         numeric(1),
  SPENDK                 numeric(2),
  STAY                   numeric(3),
  STAYK                  numeric(1),
  STAY1K                 nvarchar(10),
  STAY2K                 nvarchar(10),
  STAY3K                 nvarchar(10),
  STAY4K                 nvarchar(10),
  STAY5K                 nvarchar(10),
  STAY6K                 nvarchar(10),
  STAY7K                 nvarchar(10),
  STAY8K                 nvarchar(10),
  STAY9K                 nvarchar(10),
  STAYTLY                numeric(6),
  STAY_WT                numeric(6, 3),
  STAY_WTK               nvarchar(10),
  TYPEINTERVIEW          numeric(3),
  UKLEG                  numeric(6),
  VISIT_WT               numeric(6, 3),
  VISIT_WTK              nvarchar(10),
  SHIFT_WT               numeric(9, 3),
  NON_RESPONSE_WT        numeric(9, 3),
  MINS_WT                numeric(9, 3),
  TRAFFIC_WT             numeric(9, 3),
  UNSAMP_TRAFFIC_WT      numeric(9, 3),
  IMBAL_WT               numeric(9, 3),
  FINAL_WT               numeric(12, 3)
)
go

create table SURVEY_VALUE
(
  VERSION_ID   numeric(10)   not null,
  SERIAL_NO    numeric(15)   not null,
  COLUMN_NO    numeric(4)    not null,
  COLUMN_VALUE nvarchar(100) not null
)
go

create table TASK
(
  TASK_ID      nvarchar(40) not null,
  PARENT_ID    nvarchar(40),
  SERVICE_NAME nvarchar(30),
  TASK_NAME    nvarchar(30) not null,
  DATE_CREATED date         not null,
  TASK_STATUS  numeric(2)   not null
)
go

create table TASK_CHILD
(
  TASK_ID  nvarchar(40) not null,
  CHILD_ID nvarchar(40) not null
)
go

create table TASK_NODE
(
  TASK_ID      nvarchar(40) not null,
  PARENT_ID    nvarchar(40),
  CHILD_ID     nvarchar(40),
  TASK_STATUS  numeric(2)   not null,
  DATE_CREATED date         not null
)
go

create table TASK_SAS_MAP
(
  TASK_ID          nvarchar(40) not null,
  PARAMETER_SET_ID numeric(10)  not null
)
go

create table TRAFFIC_DATA
(
  YEAR           bigint,
  MONTH          bigint,
  DATA_SOURCE_ID bigint,
  PORTROUTE      bigint,
  ARRIVEDEPART   bigint,
  TRAFFICTOTAL   bigint,
  PERIODSTART    varchar(max),
  PERIODEND      varchar(max),
  AM_PM_NIGHT    varchar(max),
  HAUL           varchar(max),
  RUN_ID         varchar(max)
)
go

create table UD_SAS_OUTPUTS_VW
(
  TASK_ID            nvarchar(160),
  STAT_ACT           nvarchar(400),
  STAT_UNIT          nvarchar(400),
  DATASET_TYPE       nvarchar(400),
  PERIOD_TYPE        nvarchar(max),
  PERIOD_NAME        nvarchar(960),
  OUTPUT             nvarchar(960),
  FILE_NAME          nvarchar(1924),
  VIEW_NAME          nvarchar(120),
  OVERFLOW_VIEW_NAME nvarchar(120)
)
go

create table UD_VAR_METADATA_VW
(
  VAR_NAME            nvarchar(960),
  VAR_SAS_FORMAT_NAME nvarchar(960),
  VAR_VALID_FROM_DATE date,
  VAR_DATA_TYPE       nvarchar(424),
  VAR_DATA_LENGTH     numeric(38),
  VAR_DATA_PRECISION  numeric(38),
  VAR_DESCRIPTION     nvarchar(max),
  VAR_TYPE_FLAG       nvarchar(4),
  VAR_LABEL           nvarchar(960),
  VAR_LABEL_VALUE     nvarchar(960)
)
go

create table UD_VW00019703
(
  UD_RECORD_KEY           nvarchar(120),
  UD_STATISTICAL_ACTIVITY nvarchar(12),
  UD_STATISTICAL_UNIT     nvarchar(24),
  UD_REF_DATE             date,
  UD_SAS_NAME             nvarchar(48),
  UD_EPERSNO              nvarchar(960),
  UD_ERELHRP              numeric(38),
  UD_ESPOUSE              nvarchar(960),
  UD_EFATHER              nvarchar(960),
  UD_EMOTHER              nvarchar(960),
  UD_ESEX                 numeric(38),
  UD_EYOB                 nvarchar(960),
  UD_EDOB                 numeric(38),
  UD_EMARSTAT             numeric(38),
  UD_ENAT                 nvarchar(960),
  UD_EYRSRES              nvarchar(960),
  UD_ECOB                 nvarchar(960),
  UD_EPROXY               numeric(38),
  UD_EWKSTATR             numeric(38),
  UD_ERESAWYR             nvarchar(960),
  UD_ESTATR               numeric(38),
  UD_EFISAL               numeric(38),
  UD_ENACE08              nvarchar(960),
  UD_EISCOMR15            nvarchar(960),
  UD_ESUPVIS              numeric(38),
  UD_ENUMPR               nvarchar(960),
  UD_ECONWRK              nvarchar(960),
  UD_EREGWK13             nvarchar(960),
  UD_EYRSTRTR             nvarchar(960),
  UD_EMNSTRTR             nvarchar(960),
  UD_EHOWGET              numeric(38),
  UD_EFTPTWKR             numeric(38),
  UD_EYPTJOB              numeric(38),
  UD_EPERMR               numeric(38),
  UD_EWHYTMP16            numeric(38),
  UD_EDURTMPR             numeric(38),
  UD_ETMPCON              numeric(38),
  UD_ESHIFTR              numeric(38),
  UD_EEVENR               numeric(38),
  UD_ENIGHTR              numeric(38),
  UD_ESATR                numeric(38),
  UD_ESUNR                numeric(38),
  UD_EUSUHRR              nvarchar(960),
  UD_EACTHRR              nvarchar(960),
  UD_EACTPOT              nvarchar(960),
  UD_EACTUOT              nvarchar(960),
  UD_EWHYDIFR             nvarchar(960),
  UD_EMHRSR               numeric(38),
  UD_EWAYHRS              numeric(38),
  UD_ENUMHRSR             nvarchar(960),
  UD_EHOMER               numeric(38),
  UD_EADDWKR              numeric(38),
  UD_EADDREA              numeric(38),
  UD_ESECJOBR             numeric(38),
  UD_ESTAT2R              numeric(38),
  UD_ENACE208             nvarchar(960),
  UD_EACTHR2R             nvarchar(960),
  UD_EEVWKR               numeric(38),
  UD_EYRLASTR             nvarchar(960),
  UD_EMNLASTR             nvarchar(960),
  UD_EWHYLFTR             nvarchar(960),
  UD_ESTATLR              numeric(38),
  UD_ENACEL08             nvarchar(960),
  UD_EISCOLR15            nvarchar(960),
  UD_ELOOKR               numeric(38),
  UD_ENOLWM               numeric(38),
  UD_ETYMPSR              numeric(38),
  UD_ELKTIMR              numeric(38),
  UD_EMETH1R              numeric(38),
  UD_EMETH2R              numeric(38),
  UD_EMETH3R              numeric(38),
  UD_EMETH4R              numeric(38),
  UD_EMETH5R              numeric(38),
  UD_EMETH6R              numeric(38),
  UD_EMETH7R              numeric(38),
  UD_EMETH8R              numeric(38),
  UD_EMETH9R              numeric(38),
  UD_EMETH10R             numeric(38),
  UD_EMETH11R             numeric(38),
  UD_EMETH12R             numeric(38),
  UD_EMETH13R             numeric(38),
  UD_ELIKWKR              numeric(38),
  UD_EAVALWKR             numeric(38),
  UD_EAVALREA             numeric(38),
  UD_EBEFORER             numeric(38),
  UD_ENECARE              numeric(38),
  UD_EREGPUB              numeric(38),
  UD_ESTATUS              nvarchar(960),
  UD_EDUCSTA16            numeric(38),
  UD_GAP2                 nvarchar(960),
  UD_GAP3                 nvarchar(960),
  UD_ECOURA16             numeric(38),
  UD_ECOURL               numeric(38),
  UD_ECOURP               numeric(38),
  UD_ECOURF16             nvarchar(960),
  UD_ECOURW               numeric(38),
  UD_GAP4                 nvarchar(960),
  UD_GAP6                 nvarchar(960),
  UD_GAP5                 nvarchar(960),
  UD_ESITONE              numeric(38),
  UD_ESTATOR              numeric(38),
  UD_ENACEO08             nvarchar(960),
  UD_ECTYO                nvarchar(960),
  UD_EREGO13              nvarchar(960),
  UD_INCDECIL             nvarchar(960),
  UD_EYEAR                nvarchar(960),
  UD_EREFWK               nvarchar(960),
  UD_EINTWK               numeric(38),
  UD_ESTATE               nvarchar(960),
  UD_EREGN13              nvarchar(1020),
  UD_DEGURBA              numeric(38),
  UD_ESERIAL              nvarchar(960),
  UD_ETYPHLD              numeric(38),
  UD_ETYPINS              numeric(38),
  UD_EWEIGH17             nvarchar(960),
  UD_EQWT17               nvarchar(960),
  UD_EQHHWT               nvarchar(960),
  UD_EWAVE                numeric(38),
  UD_EINTQUES             numeric(38),
  UD_EHATLEV15            nvarchar(960),
  UD_EHATYR15             numeric(38),
  UD_EHATVOC15            numeric(38),
  UD_EHATFLD16            nvarchar(960),
  UD_EDUCLEV16            numeric(38),
  UD_EDUCVOC15            numeric(38),
  UD_MAINCLNT             numeric(38),
  UD_WORKORG              numeric(38),
  UD_REASSE               numeric(38),
  UD_SEDIFFIC             numeric(38),
  UD_REASNOEM             numeric(38),
  UD_BPARTNER             numeric(38),
  UD_PLANEMPL             numeric(38),
  UD_JBSATISF             numeric(38),
  UD_AUTONOMY             numeric(38),
  UD_PREFSTAP             numeric(38),
  UD_OBSTACSE             numeric(38),
  UD_GAP                  nvarchar(960),
  UD_EREGWK133            nvarchar(960),
  UD_EREGO133             nvarchar(960),
  UD_EREGN133             nvarchar(1020),
  UD_EDUMMY               nvarchar(960),
  UD_GAP10                nvarchar(960),
  UD_STRATA               nvarchar(960),
  UD_GAP7                 nvarchar(960),
  UD_PSU                  nvarchar(960),
  UD_GAP8                 nvarchar(960),
  UD_GAP9                 nvarchar(960),
  UD_FSWEIGHT             nvarchar(960),
  UD_ACTHR2               numeric(38),
  UD_ADD                  numeric(38),
  UD_ADDJOB               numeric(38),
  UD_AGE                  numeric(38),
  UD_APPR12               numeric(38),
  UD_APPRCURR             numeric(38),
  UD_APPRLEV              numeric(38),
  UD_ATTEND               numeric(38),
  UD_BENFTS               numeric(38),
  UD_CAMEMT               numeric(38),
  UD_CAMEYR               numeric(38),
  UD_CAMEYR2              numeric(38),
  UD_CASENO               nvarchar(960),
  UD_CONTUK               numeric(38),
  UD_COURSE               numeric(38),
  UD_COUNTRY              numeric(38),
  UD_CRY12                numeric(38),
  UD_CRYO7                numeric(38),
  UD_CURCODE              nvarchar(960),
  UD_DEGREE71             numeric(38),
  UD_DEGREE72             numeric(38),
  UD_DEGREE73             numeric(38),
  UD_DEGREE74             numeric(38),
  UD_DEGREE75             numeric(38),
  UD_DIFJOB               numeric(38),
  UD_DIPTYP               numeric(38),
  UD_DOBY                 numeric(38),
  UD_ED13WK               numeric(38),
  UD_ED4WK                numeric(38),
  UD_EDAGE                numeric(38),
  UD_ENROLL               numeric(38),
  UD_EVERWK               numeric(38),
  UD_FDCMBD01             numeric(38),
  UD_FDCMBMA              numeric(38),
  UD_FDSINCOM             numeric(38),
  UD_FDSNGDEG             nvarchar(960),
  UD_FIFSAL               numeric(38),
  UD_FTPTWK               numeric(38),
  UD_FUTUR13              numeric(38),
  UD_FUTUR4               numeric(38),
  UD_GOVTOR               numeric(38),
  UD_HGHNOW               numeric(38),
  UD_HHLD                 numeric(38),
  UD_HIGHO                numeric(38),
  UD_HIQUAL15             numeric(38),
  UD_HWLNG                numeric(38),
  UD_INECAC05             numeric(38),
  UD_IOUTCOME             numeric(38),
  UD_JBAWAY               numeric(38),
  UD_JOBBEG               numeric(38),
  UD_LEFTM                numeric(38),
  UD_LEFTYR               numeric(38),
  UD_LIKEWK               numeric(38),
  UD_LKYT4                numeric(38),
  UD_LOOK4                numeric(38),
  UD_LOOKM111             numeric(38),
  UD_LOOKM112             numeric(38),
  UD_LOOKM113             numeric(38),
  UD_LSSOTH               numeric(38),
  UD_M3CRY                numeric(38),
  UD_M3CRYO               numeric(38),
  UD_NATO7                numeric(38),
  UD_NFE13WK              numeric(38),
  UD_NFE4WK               numeric(38),
  UD_NOPLNWK              numeric(38),
  UD_NTNLTY12             numeric(38),
  UD_NUMOL5               numeric(38),
  UD_NVQLE11              numeric(38),
  UD_OWNBUS               numeric(38),
  UD_OYCRY                numeric(38),
  UD_OYCRYO               numeric(38),
  UD_OYEQM3               numeric(38),
  UD_PERSNO               numeric(38),
  UD_PREFHR               numeric(38),
  UD_PWT16                numeric(38),
  UD_QRTR                 numeric(38),
  UD_QULHI11              numeric(38),
  UD_QULNOW               numeric(38),
  UD_QUOTA                numeric(38),
  UD_REDYL11              numeric(38),
  UD_REDYRS               numeric(38),
  UD_REFDTE               nvarchar(960),
  UD_REFWKM               numeric(38),
  UD_REFWKY               numeric(38),
  UD_RELBUS               numeric(38),
  UD_RESBBY               numeric(38),
  UD_RESMTH               numeric(38),
  UD_RESTME               numeric(38),
  UD_SCHM12               numeric(38),
  UD_SCNOW11              numeric(38),
  UD_SECJOB               numeric(38),
  UD_SEX                  numeric(38),
  UD_SOC10L               numeric(38),
  UD_SOC10M               numeric(38),
  UD_SOLO2                numeric(38),
  UD_SOLOR                numeric(38),
  UD_START                numeric(38),
  UD_STAT2                numeric(38),
  UD_STATR                numeric(38),
  UD_SUBCOD1              nvarchar(960),
  UD_T4PURP               numeric(38),
  UD_T4WORK               numeric(38),
  UD_TAUT4WK              numeric(38),
  UD_TAUTHRS              numeric(38),
  UD_TCNW11               numeric(38),
  UD_THISQTR              numeric(38),
  UD_THISWV               numeric(38),
  UD_TOTAC1               numeric(38),
  UD_TOTAC2               numeric(38),
  UD_TSUB4COD             nvarchar(960),
  UD_TYPSCH12             numeric(38),
  UD_UALAD99              nvarchar(960),
  UD_UNDEMP               numeric(38),
  UD_UNDST                numeric(38),
  UD_W1YR                 numeric(38),
  UD_WAIT                 numeric(38),
  UD_WARD03               nvarchar(1020),
  UD_WAVFND               numeric(38),
  UD_WEEK                 numeric(38),
  UD_WKPL99               numeric(38),
  UD_Y2JOB                numeric(38),
  UD_YLESS6               numeric(38),
  UD_YNOTFT               numeric(38),
  UD_YPTCIA               numeric(38),
  UD_YPTJOB               numeric(38),
  UD_YSTART               numeric(38),
  UD_YTETJB               numeric(38),
  UD_PCODE                nvarchar(960),
  UD_UALDO                nvarchar(960),
  UD_UALDWK               nvarchar(960),
  UD_ILODEFR              numeric(38),
  UD_GRSSWK               numeric(38),
  UD_GOVTOF               numeric(38),
  UD_INDE07M              numeric(38),
  UD_HOURPAY              numeric(38),
  UD_FTPT                 numeric(38),
  UD_SUMHRS               numeric(38),
  UD_TTACHR               numeric(38),
  UD_DURUN                numeric(38),
  UD_REDUND               numeric(38),
  UD_UNDY981              numeric(38),
  UD_UNDY982              numeric(38),
  UD_UNDY983              numeric(38),
  UD_UNDY984              numeric(38),
  UD_UNDY985              numeric(38),
  UD_UNDY986              numeric(38),
  UD_UNDY987              numeric(38),
  UD_UNDY988              numeric(38),
  UD_UNDY989              numeric(38),
  UD_UNDHRS               numeric(38),
  UD_SUBCODE              nvarchar(960),
  UD_DEGNOW               numeric(38),
  UD_YERQAL1              numeric(38),
  UD_YERQAL2              numeric(38),
  UD_YERQAL3              numeric(38),
  UD_CALWEEK              numeric(38),
  UD_CRYFTH               numeric(38),
  UD_CRYFSPC              nvarchar(960),
  UD_CRYFFRM              numeric(38),
  UD_CRYMTH               numeric(38),
  UD_CRYMSPC              nvarchar(960),
  UD_CRYMFRM              numeric(38),
  UD_FATHEDU              numeric(38),
  UD_MOTHEDU              numeric(38),
  UD_WKOTHCRY             numeric(38),
  UD_CRYSIX               numeric(38),
  UD_WCHCRY               nvarchar(960),
  UD_WCHCRYFR             numeric(38),
  UD_COMEUK1              numeric(38),
  UD_COMEUK2              numeric(38),
  UD_COMEUK3              numeric(38),
  UD_COMEUK4              numeric(38),
  UD_COMEUK5              numeric(38),
  UD_COMEUKMN             numeric(38),
  UD_JOBINUK              numeric(38),
  UD_OVRQUAL              numeric(38),
  UD_OBSKILM              numeric(38),
  UD_OBSKILS              numeric(38),
  UD_OBJOBM               numeric(38),
  UD_OBJOBS               numeric(38),
  UD_LANGSKIL             numeric(38),
  UD_LANGCOUR             numeric(38),
  UD_FINDJOB              numeric(38),
  UD_OCOD10M              numeric(38),
  UD_EDUCFLD              nvarchar(960),
  UD_EHATFLD              nvarchar(960),
  UD_ENUTS133             nvarchar(960),
  UD_ENUTS132             nvarchar(960),
  UD_EREGN10              nvarchar(1020),
  UD_EREGN103             nvarchar(1020),
  UD_EREGO10              nvarchar(960),
  UD_EREGO103             nvarchar(960),
  UD_EREGWK10             nvarchar(960),
  UD_EREGWK103            nvarchar(960),
  UD_ERELHOH              numeric(38),
  UD_EHOHID               numeric(38),
  UD_EISCOMR              nvarchar(960),
  UD_EISCOLR              nvarchar(960),
  UD_COMMAIN              numeric(38),
  UD_HDSICO               numeric(38),
  UD_SNGHD                nvarchar(960),
  UD_HICOMBMA             numeric(38),
  UD_FDSICO               numeric(38),
  UD_UNCOMBMA             numeric(38),
  UD_NDSICO               numeric(38),
  UD_SNGDEGN              nvarchar(960),
  UD_LFSSAMP              numeric(38),
  UD_NONFORM4             numeric(38),
  UD_WHYTMP6              numeric(38),
  UD_TCNWACD              numeric(38),
  UD_TCNWLEV              numeric(38),
  UD_SCNWACD              numeric(38),
  UD_SCNWLEV              numeric(38),
  UD_WBAC                 numeric(38),
  UD_OCRN11               numeric(38),
  UD_OCRNACD              numeric(38),
  UD_OCRNLEV              numeric(38),
  UD_CGNW11               numeric(38),
  UD_CGNWACD              numeric(38),
  UD_CGNWLEV              numeric(38),
  UD_QCFNOW               numeric(38),
  UD_QCFLVNW              numeric(38),
  UD_HSTNOWN              numeric(38),
  UD_HSTNOWS              numeric(38),
  UD_NVNWACD              numeric(38),
  UD_NVNWLEV              numeric(38),
  UD_STAT                 numeric(38),
  UD_INCNOW               numeric(38),
  UD_NET99                numeric(38),
  UD_NETPRD               numeric(38),
  UD_GROSS99              numeric(38),
  UD_GRSPRD               numeric(38),
  UD_HWMNYC               numeric(38),
  UD_WASIT75              numeric(38),
  UD_WRKHRSW              numeric(38),
  UD_WHHRSWD              numeric(38),
  UD_EWHYMSE              numeric(38),
  UD_DIFFDV               numeric(38),
  UD_NOMEM                numeric(38),
  UD_PART                 numeric(38),
  UD_PART2                numeric(38),
  UD_PART3                numeric(38),
  UD_PLNEMP2              numeric(38),
  UD_PLNSB                numeric(38),
  UD_PLNEMP               numeric(38),
  UD_SATJOB               numeric(38),
  UD_AUT1JB               numeric(38),
  UD_AUT2JB               numeric(38),
  UD_RTHRE                numeric(38),
  UD_ERTHSE               numeric(38),
  UD_WNMSE                numeric(38),
  UD_FRTHSE               numeric(38)
)
go

create table UNSAMPLED_OOH_DATA
(
  YEAR           bigint,
  MONTH          bigint,
  DATA_SOURCE_ID bigint,
  PORTROUTE      bigint,
  REGION         float,
  ARRIVEDEPART   bigint,
  UNSAMP_TOTAL   bigint,
  RUN_ID         varchar(max)
)
go

create table WORKFLOW
(
  WORKFLOW_ID numeric(10) not null,
  NAME        nvarchar(30),
  PERIOD nvarchar(6
)
  )
go

create table WORKFLOW_DEFINITION
(
  WORKFLOW_DEF_ID numeric(3)    not null,
  WF_DEFINITION   nvarchar(max) not null
)
go

create table WORKSPACE_MAINTENANCE
(
  WORKSPACE_ID numeric(10) not null,
  WORKFLOW_ID  numeric(10) not null,
  WORKSPACE    nvarchar(60),
  STATE        nvarchar(20),
  DATE_CREATED date
)
go

create table poprowvec_traffic
(
  [index] bigint,
  C_group bigint,
  T_1     float,
  T_2     float,
  T_3     float,
  T_4     float,
  T_5     float,
  T_6     float,
  T_7     float,
  T_8     float,
  T_9     float,
  T_10    float,
  T_11    float,
  T_12    float,
  T_13    float,
  T_14    float,
  T_15    float,
  T_16    float,
  T_17    float,
  T_18    float,
  T_19    float,
  T_20    float,
  T_21    float,
  T_22    float,
  T_23    float,
  T_24    float,
  T_25    float,
  T_26    float,
  T_27    float,
  T_28    float,
  T_29    float,
  T_30    float,
  T_31    float,
  T_32    float,
  T_33    float,
  T_34    float,
  T_35    float,
  T_36    float,
  T_37    float,
  T_38    float,
  T_39    float,
  T_40    float,
  T_41    float,
  T_42    float,
  T_43    float,
  T_44    float,
  T_45    float,
  T_46    float,
  T_47    float,
  T_48    float,
  T_49    float,
  T_50    float,
  T_51    float,
  T_52    float,
  T_53    float,
  T_54    float,
  T_55    float,
  T_56    float,
  T_57    float,
  T_58    float,
  T_59    float,
  T_60    float,
  T_61    float,
  T_62    float,
  T_63    float,
  T_64    float,
  T_65    float,
  T_66    float,
  T_67    float,
  T_68    float,
  T_69    float,
  T_70    float,
  T_71    float,
  T_72    float,
  T_73    float,
  T_74    float,
  T_75    float,
  T_76    float,
  T_77    float
)
go

create index ix_poprowvec_traffic_index
  on poprowvec_traffic ([index])
go

create table poprowvec_unsamp
(
  C_group bigint,
  T_1     float,
  T_2     float,
  T_3     float,
  T_4     float,
  T_5     float,
  T_6     float,
  T_7     float,
  T_8     float,
  T_9     float,
  T_10    float,
  T_11    float,
  T_12    float,
  T_13    float,
  T_14    float,
  T_15    float,
  T_16    float,
  T_17    float,
  T_18    float,
  T_19    float,
  T_20    float,
  T_21    float,
  T_22    float,
  T_23    float,
  T_24    float,
  T_25    float,
  T_26    float,
  T_27    float,
  T_28    float,
  T_29    float,
  T_30    float,
  T_31    float,
  T_32    float,
  T_33    float,
  T_34    float,
  T_35    float,
  T_36    float,
  T_37    float,
  T_38    float,
  T_39    float,
  T_40    float,
  T_41    float,
  T_42    float,
  T_43    float,
  T_44    float,
  T_45    float,
  T_46    float,
  T_47    float,
  T_48    float,
  T_49    float,
  T_50    float,
  T_51    float,
  T_52    float,
  T_53    float,
  T_54    float,
  T_55    float,
  T_56    float,
  T_57    float,
  T_58    float,
  T_59    float,
  T_60    float,
  T_61    float,
  T_62    float,
  T_63    float,
  T_64    float,
  T_65    float,
  T_66    float,
  T_67    float,
  T_68    float,
  T_69    float,
  T_70    float,
  T_71    float,
  T_72    float,
  T_73    float,
  T_74    float,
  T_75    float,
  T_76    float,
  T_77    float,
  T_78    float,
  T_79    float,
  T_80    float,
  T_81    float,
  T_82    float,
  T_83    float,
  T_84    float,
  T_85    float,
  T_86    float,
  T_87    float,
  T_88    float,
  T_89    float,
  T_90    float,
  T_91    float,
  T_92    float,
  T_93    float,
  T_94    float,
  T_95    float,
  T_96    float,
  T_97    float,
  T_98    float,
  T_99    float,
  T_100   float,
  T_101   float,
  T_102   float,
  T_103   float,
  T_104   float,
  T_105   float,
  T_106   float,
  T_107   float,
  T_108   float,
  T_109   float,
  T_110   float,
  T_111   float,
  T_112   float,
  T_113   float,
  T_114   float,
  T_115   float,
  T_116   float,
  T_117   float,
  T_118   float,
  T_119   float,
  T_120   float,
  T_121   float,
  T_122   float,
  T_123   float,
  T_124   float,
  T_125   float,
  T_126   float,
  T_127   float,
  T_128   float,
  T_129   float,
  T_130   float,
  T_131   float,
  T_132   float,
  T_133   float,
  T_134   float,
  T_135   float,
  T_136   float,
  T_137   float,
  T_138   float,
  T_139   float,
  T_140   float,
  T_141   float,
  T_142   float,
  T_143   float,
  T_144   float,
  T_145   float,
  T_146   float,
  T_147   float,
  T_148   float,
  T_149   float,
  T_150   float,
  T_151   float,
  T_152   float,
  T_153   float,
  T_154   float,
  T_155   float,
  T_156   float,
  T_157   float,
  T_158   float,
  T_159   float,
  T_160   float,
  T_161   float,
  T_162   float,
  T_163   float,
  T_164   float,
  T_165   float,
  T_166   float,
  T_167   float,
  T_168   float,
  T_169   float,
  T_170   float,
  T_171   float,
  T_172   float,
  T_173   float,
  T_174   float,
  T_175   float,
  T_176   float,
  T_177   float,
  T_178   float,
  T_179   float,
  T_180   float,
  T_181   float,
  T_182   float,
  T_183   float,
  T_184   float,
  T_185   float,
  T_186   float,
  T_187   float,
  T_188   float,
  T_189   float,
  T_190   float,
  T_191   float,
  T_192   float,
  T_193   float,
  T_194   float,
  T_195   float,
  T_196   float,
  T_197   float,
  T_198   float,
  T_199   float,
  T_200   float,
  T_201   float,
  T_202   float,
  T_203   float
)
go

create index ix_poprowvec_unsamp_C_group
  on poprowvec_unsamp (C_group)
go

create table r_traffic
(
  rownames         varchar(255),
  SERIAL           float,
  ARRIVEDEPART     int,
  PORTROUTE        int,
  SAMP_PORT_GRP_PV varchar(255),
  SHIFT_WT         float,
  NON_RESPONSE_WT  float,
  MINS_WT          float,
  TRAFFIC_WT       float,
  trafDesignWeight float,
  T1               int,
  T_               varchar(255),
  tw_weight        float
)
go

create table r_unsampled
(
  rownames              varchar(255),
  SERIAL                float,
  ARRIVEDEPART          int,
  PORTROUTE             int,
  UNSAMP_PORT_GRP_PV    varchar(255),
  UNSAMP_REGION_GRP_PV  int,
  SHIFT_WT              float,
  NON_RESPONSE_WT       float,
  MINS_WT               float,
  UNSAMP_TRAFFIC_WT     float,
  OOHDesignWeight       float,
  T1                    int,
  T_                    varchar(255),
  unsamp_traffic_weight float
)
go

create table sqlResult
(
  rownames       varchar(255),
  RUN_ID         varchar(255),
  YEAR           int,
  MONTH          int,
  DATA_SOURCE_ID int,
  PORTROUTE      int,
  ARRIVEDEPART   int,
  TRAFFICTOTAL   float,
  PERIODSTART    int,
  PERIODEND      int,
  AM_PM_NIGHT    int,
  HAUL           varchar(255),
  VEHICLE        int
)
go

create table survey_traffic_aux
(
  SERIAL           float,
  ARRIVEDEPART     int,
  PORTROUTE        int,
  SAMP_PORT_GRP_PV varchar(255),
  SHIFT_WT         float,
  NON_RESPONSE_WT  float,
  MINS_WT          float,
  TRAFFIC_WT       varchar(5),
  trafDesignWeight float,
  T1               int
)
go

create table survey_unsamp_aux
(
  SERIAL               float,
  ARRIVEDEPART         int,
  PORTROUTE            int,
  UNSAMP_PORT_GRP_PV   varchar(255),
  UNSAMP_REGION_GRP_PV int,
  SHIFT_WT             float,
  NON_RESPONSE_WT      float,
  MINS_WT              float,
  UNSAMP_TRAFFIC_WT    varchar(5),
  OOHDesignWeight      float,
  T1                   int
)
go

insert into DATA_SOURCE(DATA_SOURCE_ID, DATA_SOURCE_NAME)
values
       (1, 'Sea'),
       (2, 'Air'),
       (3, 'Tunnel'),
       (4, 'Shift'),
       (5, 'Non Response'),
       (6, 'Unsampled')
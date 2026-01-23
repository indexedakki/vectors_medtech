import copy
import numpy as np
from sqlalchemy import Column, Integer, String, DateTime, and_, create_engine, select
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.orm import declarative_base
import pandas as pd
import os
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import re
from collections import defaultdict
from datetime import datetime, timedelta

# Create logger start
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create directory for logs
log_dir = 'app_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create a TimedRotatingFileHandler
file_handler = TimedRotatingFileHandler(os.path.join(log_dir, 'JAIDA_Cognitive_Ingestion.log'), when='midnight', interval=1,
                                        backupCount=100)

# Create a file handler
file_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)
# Create logger end

Base = declarative_base()



class ExplosionReportData(Base):
    __tablename__ = "explosion_report_data"
    pk_id = Column(Integer, primary_key=True)
    M_SUPER_PARNT_UNI_CUST_NO = Column(String, nullable=True)
    LGCY_BUYG_GRP_NO = Column(String, nullable=True)
    IDN_NAME = Column(String, nullable=True)
    INDIV_UCN = Column(String, nullable=True)
    MEMBER_SHIPTO_UCN = Column(String, nullable=True)
    DISTN_MODE_CD = Column(String, nullable=True)
    COT_CD = Column(String, nullable=True)
    ROLE_SUMMARY = Column(String, nullable=True)
    CUST_STAT_CD = Column(String, nullable=True)
    CUST_LN1_NM = Column(String, nullable=True)
    CUST_LN2_NM = Column(String, nullable=True)
    ADDR_LN1_TXT = Column(String, nullable=True)
    ADDR_LN2_TXT = Column(String, nullable=True)
    CITY_NM = Column(String, nullable=True)
    STT_PROVN_CD = Column(String, nullable=True)
    POSTL_CD = Column(String, nullable=True)
    M_EFFTV_DT = Column(String, nullable=True)
    M_END_DT = Column(String, nullable=True)
    M_AFFLN_HIER_CD = Column(String, nullable=True)
    MEMB_IND = Column(String, nullable=True)
    NOTES_TXT = Column(String, nullable=True)
    RELSHP_MBR_TYP_CD = Column(String, nullable=True)
    BG_SUBTYP = Column(String, nullable=True)
    ACTIVE_SECTOR = Column(String, nullable=True)
    DEA = Column(String, nullable=True)
    HIN = Column(String, nullable=True)
    GLN = Column(String, nullable=True)
    PAYFROM_UCN = Column(String, nullable=True)
    PAYFROM_CUST = Column(String, nullable=True)
    MULTIPLE_UCN_COUNT = Column(Integer, nullable=True)
    MULTIPLE_SUP_PRNT_IND = Column(String, nullable=True)

    def __init__(self, **kwargs):
       # Initialize properties with default values (if needed)
       self.M_SUPER_PARNT_UNI_CUST_NO = kwargs.get('M_SUPER_PARNT_UNI_CUST_NO')
       self.LGCY_BUYG_GRP_NO = kwargs.get('LGCY_BUYG_GRP_NO')
       self.IDN_NAME = kwargs.get('IDN_NAME')
       self.INDIV_UCN = kwargs.get('INDIV_UCN')
       self.MEMBER_SHIPTO_UCN = kwargs.get('MEMBER_SHIPTO_UCN')
       self.DISTN_MODE_CD = kwargs.get('DISTN_MODE_CD')
       self.COT_CD = kwargs.get('COT_CD')
       self.ROLE_SUMMARY = kwargs.get('ROLE_SUMMARY')
       self.CUST_STAT_CD = kwargs.get('CUST_STAT_CD')
       self.CUST_LN1_NM = kwargs.get('CUST_LN1_NM')
       self.CUST_LN2_NM = kwargs.get('CUST_LN2_NM')
       self.ADDR_LN1_TXT = kwargs.get('ADDR_LN1_TXT')
       self.ADDR_LN2_TXT = kwargs.get('ADDR_LN2_TXT')
       self.CITY_NM = kwargs.get('CITY_NM')
       self.STT_PROVN_CD = kwargs.get('STT_PROVN_CD')
       self.POSTL_CD = kwargs.get('POSTL_CD')
       self.M_EFFTV_DT = kwargs.get('M_EFFTV_DT')
       self.M_END_DT = kwargs.get('M_END_DT')
       self.M_AFFLN_HIER_CD = kwargs.get('M_AFFLN_HIER_CD')
       self.MEMB_IND = kwargs.get('MEMB_IND')
       self.NOTES_TXT = kwargs.get('NOTES_TXT')
       self.RELSHP_MBR_TYP_CD = kwargs.get('RELSHP_MBR_TYP_CD')
       self.BG_SUBTYP = kwargs.get('BG_SUBTYP')
       self.ACTIVE_SECTOR = kwargs.get('ACTIVE_SECTOR')
       self.DEA = kwargs.get('DEA')
       self.HIN = kwargs.get('HIN')
       self.GLN = kwargs.get('GLN')
       self.PAYFROM_UCN = kwargs.get('PAYFROM_UCN')
       self.PAYFROM_CUST = kwargs.get('PAYFROM_CUST')
       self.MULTIPLE_UCN_COUNT = kwargs.get('MULTIPLE_UCN_COUNT')
       self.MULTIPLE_SUP_PRNT_IND = kwargs.get('MULTIPLE_SUP_PRNT_IND')

    def update(self, **kwargs):
       # Initialize properties with default values (if needed)
       self.M_SUPER_PARNT_UNI_CUST_NO = kwargs.get('M_SUPER_PARNT_UNI_CUST_NO')
       self.LGCY_BUYG_GRP_NO = kwargs.get('LGCY_BUYG_GRP_NO')
       self.IDN_NAME = kwargs.get('IDN_NAME')
       self.INDIV_UCN = kwargs.get('INDIV_UCN')
       self.MEMBER_SHIPTO_UCN = kwargs.get('MEMBER_SHIPTO_UCN')
       self.DISTN_MODE_CD = kwargs.get('DISTN_MODE_CD')
       self.COT_CD = kwargs.get('COT_CD')
       self.ROLE_SUMMARY = kwargs.get('ROLE_SUMMARY')
       self.CUST_STAT_CD = kwargs.get('CUST_STAT_CD')
       self.CUST_LN1_NM = kwargs.get('CUST_LN1_NM')
       self.CUST_LN2_NM = kwargs.get('CUST_LN2_NM')
       self.ADDR_LN1_TXT = kwargs.get('ADDR_LN1_TXT')
       self.ADDR_LN2_TXT = kwargs.get('ADDR_LN2_TXT')
       self.CITY_NM = kwargs.get('CITY_NM')
       self.STT_PROVN_CD = kwargs.get('STT_PROVN_CD')
       self.POSTL_CD = kwargs.get('POSTL_CD')
       self.M_EFFTV_DT = kwargs.get('M_EFFTV_DT')
       self.M_END_DT = kwargs.get('M_END_DT')
       self.M_AFFLN_HIER_CD = kwargs.get('M_AFFLN_HIER_CD')
       self.MEMB_IND = kwargs.get('MEMB_IND')
       self.NOTES_TXT = kwargs.get('NOTES_TXT')
       self.RELSHP_MBR_TYP_CD = kwargs.get('RELSHP_MBR_TYP_CD')
       self.BG_SUBTYP = kwargs.get('BG_SUBTYP')
       self.ACTIVE_SECTOR = kwargs.get('ACTIVE_SECTOR')
       self.DEA = kwargs.get('DEA')
       self.HIN = kwargs.get('HIN')
       self.GLN = kwargs.get('GLN')
       self.PAYFROM_UCN = kwargs.get('PAYFROM_UCN')
       self.PAYFROM_CUST = kwargs.get('PAYFROM_CUST')
       self.MULTIPLE_UCN_COUNT = kwargs.get('MULTIPLE_UCN_COUNT')
       self.MULTIPLE_SUP_PRNT_IND = kwargs.get('MULTIPLE_SUP_PRNT_IND')

class InventoryData(Base):
    __tablename__ = "inventory_data"
    pk_id = Column(Integer, primary_key=True)
    ContentID = Column(String, nullable=True)
    FilePath = Column(String, nullable=True)
    FileName = Column(String, nullable=True)
    Policy_Number = Column(String, nullable=True)
    Article_Number = Column(String, nullable=True)
    Title = Column(String, nullable=True)
    Customer_Name = Column(String, nullable=True)
    UCN = Column(String, nullable=True)
    Parent_UCN = Column(String, nullable=True)
    Contract_Type = Column(String, nullable=True)
    Article_Type = Column(String, nullable=True)
    Record_Type = Column(String, nullable=True)
    RecordNumber = Column(String, nullable=True)
    RecordContainer_RecordNumber = Column(String, nullable=True)
    Effective_Date = Column(DateTime, nullable=True)
    End_Date = Column(DateTime, nullable=True)
    Master_Language = Column(String, nullable=True)
    Translation = Column(String, nullable=True)
    Category_Audience = Column(String, nullable=True)
    Category_Geography = Column(String, nullable=True)
    Country = Column(String, nullable=True)
    Excluding_Country = Column(String, nullable=True)
    Region = Column(String, nullable=True)
    GS_Process_Name = Column(String, nullable=True)
    TR_Sub_Process = Column(String, nullable=True)
    GS_Process_Function = Column(String, nullable=True)
    GS_Process_SubProcess1 = Column(String, nullable=True)
    URL_Name = Column(String, nullable=True)
    Version_Number = Column(String, nullable=True)
    Article_Owner_Full_Name = Column(String, nullable=True)
    Created_Date = Column(DateTime, nullable=True)
    Last_Modified_Date = Column(DateTime, nullable=True)
    First_Published_Date = Column(DateTime, nullable=True)
    Date_of_Last_Review = Column(DateTime, nullable=True)
    Reviewer_Name = Column(String, nullable=True)
    Service_Catalog_Name = Column(String, nullable=True)
    Service_Catalog_Business_Function = Column(String, nullable=True)
    Service_Catalog_SubProcess1 = Column(String, nullable=True)
    Service_Catalog_SubProcess2 = Column(String, nullable=True)
    Permissions = Column(String, nullable=True)
    Copy_Approval_Numbers = Column(String, nullable=True)
    Priority = Column(String, nullable=True)
    Expiration_Date = Column(DateTime, nullable=True)
    Keywords = Column(String, nullable=True)
    Summary = Column(String, nullable=True)
    SDFC_Link = Column(String, nullable=True)
    AskGS_Link = Column(String, nullable=True)
    DocType = Column(String, nullable=True)
    Thumbnail_URL = Column(String, nullable=True)
    Skillset_Keywords = Column(String, nullable=True)
    External_Url = Column(String, nullable=True)
    Record_Revision_Count = Column(String, nullable=True)
    Product_Lines = Column(String, nullable=True)
    Amendments = Column(String, nullable=True)
    Record_Creator = Column(String, nullable=True)
    Record_Notes = Column(String, nullable=True)
    Thesaurus = Column(String, nullable=True)
    Original_Contract_Type = Column(String, nullable=True)
    Related_Records = Column(String, nullable=True)
    Ingestion_Date = Column(DateTime, nullable=True)
    Sub_Section_List = Column(String, nullable=True)
    Operating_Company = Column(String, nullable=True)
    HPE_Title = Column(String, nullable=True)
    Entity_Type = Column(String, nullable=True)
    Entity_Version = Column(String, nullable=True)
    Eligible_Participants = Column(String, nullable=True)
    Pricing_Type = Column(String, nullable=True)
    Demo = Column(String, nullable=True, default='NO')


    def __init__(self, **kwargs):
        self.ContentID = kwargs.get('ContentID')
        self.FilePath = kwargs.get('FilePath')
        self.FileName = kwargs.get('FileName')
        self.Policy_Number = kwargs.get('Policy_Number')
        self.Article_Number = kwargs.get('Article_Number')
        self.Title = kwargs.get('Title')
        self.Customer_Name = kwargs.get('Customer_Name')
        self.UCN = kwargs.get('UCN')
        self.Parent_UCN = kwargs.get('Parent_UCN')
        self.Contract_Type = kwargs.get('Contract_Type')
        self.Article_Type = kwargs.get('Article_Type')
        self.Record_Type = kwargs.get('Record_Type')
        self.RecordNumber = kwargs.get('RecordNumber')
        self.RecordContainer_RecordNumber = kwargs.get('RecordContainer_RecordNumber')
        self.Effective_Date = kwargs.get('Effective_Date')
        self.End_Date = kwargs.get('End_Date')
        self.Master_Language = kwargs.get('Master_Language')
        self.Translation = kwargs.get('Translation')
        self.Category_Audience = kwargs.get('Category_Audience')
        self.Category_Geography = kwargs.get('Category_Geography')
        self.Country = kwargs.get('Country')
        self.Excluding_Country = kwargs.get('Excluding_Country')
        self.Region = kwargs.get('Region')
        self.GS_Process_Name = kwargs.get('GS_Process_Name')
        self.TR_Sub_Process = kwargs.get('TR_Sub_Process')
        self.GS_Process_Function = kwargs.get('GS_Process_Function')
        self.GS_Process_SubProcess1 = kwargs.get('GS_Process_SubProcess1')
        self.URL_Name = kwargs.get('URL_Name')
        self.Version_Number = kwargs.get('Version_Number')
        self.Article_Owner_Full_Name = kwargs.get('Article_Owner_Full_Name')
        self.Created_Date = kwargs.get('Created_Date')
        self.Last_Modified_Date = kwargs.get('Last_Modified_Date')
        self.First_Published_Date = kwargs.get('First_Published_Date')
        self.Date_of_Last_Review = kwargs.get('Date_of_Last_Review')
        self.Reviewer_Name = kwargs.get('Reviewer_Name')
        self.Service_Catalog_Name = kwargs.get('Service_Catalog_Name')
        self.Service_Catalog_Business_Function = kwargs.get('Service_Catalog_Business_Function')
        self.Service_Catalog_SubProcess1 = kwargs.get('Service_Catalog_SubProcess1')
        self.Service_Catalog_SubProcess2 = kwargs.get('Service_Catalog_SubProcess2')
        self.Permissions = kwargs.get('Permissions')
        self.Copy_Approval_Numbers = kwargs.get('Copy_Approval_Numbers')
        self.Priority = kwargs.get('Priority')
        self.Expiration_Date = kwargs.get('Expiration_Date')
        self.Keywords = kwargs.get('Keywords')
        self.Summary = kwargs.get('Summary')
        self.SDFC_Link = kwargs.get('SDFC_Link')
        self.AskGS_Link = kwargs.get('AskGS_Link')
        self.DocType = kwargs.get('DocType')
        self.Thumbnail_URL = kwargs.get('Thumbnail_URL')
        self.Skillset_Keywords = kwargs.get('Skillset_Keywords')
        self.External_Url = kwargs.get('External_Url')
        self.Record_Revision_Count = kwargs.get('Record_Revision_Count')
        self.Product_Lines = kwargs.get('Product_Lines')
        self.Amendments = kwargs.get('Amendments')
        self.Record_Creator = kwargs.get('Record_Creator')
        self.Record_Notes = kwargs.get('Record_Notes')
        self.Thesaurus = kwargs.get('Thesaurus')
        self.Original_Contract_Type = kwargs.get('Original_Contract_Type')
        self.Related_Records = kwargs.get('Related_Records')
        self.Ingestion_Date = kwargs.get('Ingestion_Date')
        self.Sub_Section_List = kwargs.get('Sub_Section_List')
        self.Operating_Company = kwargs.get('Operating_Company')
        self.HPE_Title = kwargs.get('HPE_Title')
        self.Entity_Type = kwargs.get('Entity_Type')
        self.Entity_Version = kwargs.get('Entity_Version')
        self.Eligible_Participants = kwargs.get('Eligible_Participants')
        self.Pricing_Type = kwargs.get('Pricing_Type')
        self.Demo = kwargs.get('Demo')


    def update(self, **kwargs):
        self.ContentID = kwargs.get('ContentID')
        self.FilePath = kwargs.get('FilePath')
        self.FileName = kwargs.get('FileName')
        self.Policy_Number = kwargs.get('Policy_Number')
        self.Article_Number = kwargs.get('Article_Number')
        self.Title = kwargs.get('Title')
        self.Customer_Name = kwargs.get('Customer_Name')
        self.UCN = kwargs.get('UCN')
        self.Parent_UCN = kwargs.get('Parent_UCN')
        self.Contract_Type = kwargs.get('Contract_Type')
        self.Article_Type = kwargs.get('Article_Type')
        self.Record_Type = kwargs.get('Record_Type')
        self.RecordNumber = kwargs.get('RecordNumber')
        self.RecordContainer_RecordNumber = kwargs.get('RecordContainer_RecordNumber')
        self.Effective_Date = kwargs.get('Effective_Date')
        self.End_Date = kwargs.get('End_Date')
        self.Master_Language = kwargs.get('Master_Language')
        self.Translation = kwargs.get('Translation')
        self.Category_Audience = kwargs.get('Category_Audience')
        self.Category_Geography = kwargs.get('Category_Geography')
        self.Country = kwargs.get('Country')
        self.Excluding_Country = kwargs.get('Excluding_Country')
        self.Region = kwargs.get('Region')
        self.GS_Process_Name = kwargs.get('GS_Process_Name')
        self.TR_Sub_Process = kwargs.get('TR_Sub_Process')
        self.GS_Process_Function = kwargs.get('GS_Process_Function')
        self.GS_Process_SubProcess1 = kwargs.get('GS_Process_SubProcess1')
        self.URL_Name = kwargs.get('URL_Name')
        self.Version_Number = kwargs.get('Version_Number')
        self.Article_Owner_Full_Name = kwargs.get('Article_Owner_Full_Name')
        self.Created_Date = kwargs.get('Created_Date')
        self.Last_Modified_Date = kwargs.get('Last_Modified_Date')
        self.First_Published_Date = kwargs.get('First_Published_Date')
        self.Date_of_Last_Review = kwargs.get('Date_of_Last_Review')
        self.Reviewer_Name = kwargs.get('Reviewer_Name')
        self.Service_Catalog_Name = kwargs.get('Service_Catalog_Name')
        self.Service_Catalog_Business_Function = kwargs.get('Service_Catalog_Business_Function')
        self.Service_Catalog_SubProcess1 = kwargs.get('Service_Catalog_SubProcess1')
        self.Service_Catalog_SubProcess2 = kwargs.get('Service_Catalog_SubProcess2')
        self.Permissions = kwargs.get('Permissions')
        self.Copy_Approval_Numbers = kwargs.get('Copy_Approval_Numbers')
        self.Priority = kwargs.get('Priority')
        self.Expiration_Date = kwargs.get('Expiration_Date')
        self.Keywords = kwargs.get('Keywords')
        self.Summary = kwargs.get('Summary')
        self.SDFC_Link = kwargs.get('SDFC_Link')
        self.AskGS_Link = kwargs.get('AskGS_Link')
        self.DocType = kwargs.get('DocType')
        self.Thumbnail_URL = kwargs.get('Thumbnail_URL')
        self.Skillset_Keywords = kwargs.get('Skillset_Keywords')
        self.External_Url = kwargs.get('External_Url')
        self.Record_Revision_Count = kwargs.get('Record_Revision_Count')
        self.Product_Lines = kwargs.get('Product_Lines')
        self.Amendments = kwargs.get('Amendments')
        self.Record_Creator = kwargs.get('Record_Creator')
        self.Record_Notes = kwargs.get('Record_Notes')
        self.Thesaurus = kwargs.get('Thesaurus')
        self.Original_Contract_Type = kwargs.get('Original_Contract_Type')
        self.Related_Records = kwargs.get('Related_Records')
        self.Ingestion_Date = kwargs.get('Ingestion_Date')
        self.Sub_Section_List = kwargs.get('Sub_Section_List')
        self.Operating_Company = kwargs.get('Operating_Company')
        self.HPE_Title = kwargs.get('HPE_Title')
        self.Entity_Type = kwargs.get('Entity_Type')
        self.Entity_Version = kwargs.get('Entity_Version')
        self.Eligible_Participants = kwargs.get('Eligible_Participants')
        self.Pricing_Type = kwargs.get('Pricing_Type')
        self.Demo = kwargs.get('Demo')

class BindersData(Base):
    __tablename__ = "binders_data"
    binders_id = Column(Integer, primary_key=True)
    parent_ucn = Column(String, nullable=False)
    ucn = Column(String, nullable=False)
    contract_type = Column(String, nullable=True)
    policy_number = Column(String, nullable=True)
    trim_number = Column(String, nullable=False)
    binder_identifier = Column(String, nullable=False)
    parent_content_id = Column(String, nullable=True)
    parent_record_number = Column(String, nullable=True)
    child_content_ids = Column(String, nullable=True)
    child_record_numbers = Column(String, nullable=True)
    sub_section_list = Column(String, nullable=True)
    summary = Column(String, nullable=True)
    comments = Column(String, nullable=True)
    status = Column(Integer, nullable=False)

    def __init__(self, **kwargs):
        self.parent_ucn = kwargs.get('parent_ucn')
        self.ucn = kwargs.get('ucn')
        self.contract_type = kwargs.get('contract_type')
        self.policy_number = kwargs.get('policy_number')
        self.trim_number = kwargs.get('trim_number')
        self.binder_identifier = kwargs.get('binder_identifier')
        self.parent_content_id = kwargs.get('parent_content_id')
        self.child_content_ids = kwargs.get('child_content_ids')
        self.sub_section_list = kwargs.get('sub_section_list')
        self.summary = kwargs.get('summary')
        self.comments = kwargs.get('comments')
        self.status = kwargs.get('status')
        self.parent_record_number = kwargs.get('parent_record_number')
        self.child_record_numbers = kwargs.get('child_record_numbers')

    def update(self, **kwargs):
        self.parent_ucn = kwargs.get('parent_ucn')
        self.ucn = kwargs.get('ucn')
        self.contract_type = kwargs.get('contract_type')
        self.policy_number = kwargs.get('policy_number')
        self.trim_number = kwargs.get('trim_number')
        self.binder_identifier = kwargs.get('binder_identifier')
        self.parent_content_id = kwargs.get('parent_content_id')
        self.child_content_ids = kwargs.get('child_content_ids')
        self.sub_section_list = kwargs.get('sub_section_list')
        self.summary = kwargs.get('summary')
        self.comments = kwargs.get('comments')
        self.status = kwargs.get('status')
        self.parent_record_number = kwargs.get('parent_record_number')
        self.child_record_numbers = kwargs.get('child_record_numbers')

engine = create_engine('sqlite:///GSC_Data-DEV.db', pool_recycle=3600, echo = True)

Base.metadata.create_all(bind=engine)
SessionMaker = sessionmaker(bind=engine)
session = SessionMaker()

def convertRowToDict(model, results):
    result_dicts = [{column.name: getattr(row, column.name) for column in model.__table__.columns} for row in results]
    return [] if result_dicts is None else result_dicts

"""   DAO METHODS """
# Check if the UCN is PARENT
def isItParentUCN(ucn):
    row = session.scalars(select(ExplosionReportData).where(ExplosionReportData.M_SUPER_PARNT_UNI_CUST_NO == ucn)).first()
    return None if row is None else row.__dict__

# Check if the UCN is INDIVIDUAL
def isItIndividualUCN(ucn):
    row = session.scalars(select(ExplosionReportData).where(ExplosionReportData.INDIV_UCN == ucn, ExplosionReportData.MEMBER_SHIPTO_UCN == ucn)).first()
    return None if row is None else row.__dict__

# Check if the UCN is SHIP TO
def isItShipToUCN(ucn):
    row = session.scalars(select(ExplosionReportData).where(ExplosionReportData.MEMBER_SHIPTO_UCN == ucn)).first()
    return None if row is None else row.__dict__

# Find UCN Type (PARENT, INDIVIDUAL or SHIPTO)
def findUCNType(ucn):
    res = isItParentUCN(ucn)
    if res is not None:
        return 'PARENT'
    else:
        res = isItIndividualUCN(ucn)
        if res is not None:
            return 'INDIVIDUAL'
        else:
            res = isItShipToUCN(ucn)
            if res is not None:
                return 'SHIPTO'
            else:
                return 'UNKNOWN'

''' find ExplosionReportData and return '''
def findExplosionReportData(M_SUPER_PARNT_UNI_CUST_NO, INDIV_UCN, MEMBER_SHIPTO_UCN):
    res = session.query(
        ExplosionReportData
    ).filter(
        and_(
                ExplosionReportData.M_SUPER_PARNT_UNI_CUST_NO == M_SUPER_PARNT_UNI_CUST_NO, 
                ExplosionReportData.INDIV_UCN == INDIV_UCN,
                ExplosionReportData.MEMBER_SHIPTO_UCN == MEMBER_SHIPTO_UCN,
            )
        ).one_or_none()
    return res

''' save ExplosionReportData and return '''
def saveExplosionData(payload):
    data = ExplosionReportData(**payload)
    session.add(data)
    session.commit()
    return data

def processExplosionData():
    excel_file = r"IDN.xlsx"
    sheet_name = 'Filtered_IDN'

    source_excel_df = pd.read_excel(excel_file, sheet_name, dtype=str)
    source_excel_df = source_excel_df.replace({pd.NA: None})
    source_excel_df = source_excel_df.to_dict(orient='records')   

    for item in source_excel_df:
        explosionData = findExplosionReportData(M_SUPER_PARNT_UNI_CUST_NO=item.get('M_SUPER_PARNT_UNI_CUST_NO'), INDIV_UCN=item.get('INDIV_UCN'), MEMBER_SHIPTO_UCN=item.get('MEMBER_SHIPTO_UCN'))
        if explosionData is None:
            logger.info(f"************ Data missing  - {item.get('M_SUPER_PARNT_UNI_CUST_NO')} - {item.get('INDIV_UCN')} - {item.get('MEMBER_SHIPTO_UCN')}")
            explosionData = saveExplosionData(payload=item)
        else:
            pass
            explosionData.update(**item)
            session.commit()
# ===========================================================
# Function to dynamically generate an AND function
def generate_and_function(*conditions):
    return and_(*conditions)

''' find all InventoryData and return '''
def findInventoryDataAll():
    res = session.query(
        InventoryData
    ).order_by(InventoryData.Policy_Number, InventoryData.ContentID).all()
    return res

''' find InventoryData by Params and return '''
def findInventoryDataByParams(conditions = {}):
    if len(conditions) == 0:
        return []
    and_function = generate_and_function(*conditions)

    # print all tables and columns in the session
    print("Tables and columns in the session:")
    for table in Base.metadata.tables.values():
        print(f"Table: {table.name}")
        for column in table.columns:
            print(f"  Column: {column.name}")
    
    # print top 10 rows of all tables in the session
    print("Top 10 rows of all tables in the session:")
    for table in Base.metadata.tables.values():
        print(f"Table: {table.name}")
        result = session.execute(select(table).limit(10))
        for row in result:
            print(f"  Row: {row}")
    
    results = session.query(InventoryData).filter(and_function).all()
    results = convertRowToDict(InventoryData, results)
    return results

''' find InventoryData by Contract_Type and return '''
def findInventoryDataByContractType(Contract_Type):
    res = session.query(
        InventoryData
    ).filter(
        and_(
                InventoryData.Contract_Type == Contract_Type,
            )
        ).one_or_none()
    return res

''' find InventoryData by ContentID and return '''
def findInventoryDataByContentID(ContentID):
    res = session.query(
        InventoryData
    ).filter(
        and_(
                InventoryData.ContentID == ContentID,
            )
        ).one_or_none()
    return res

''' find InventoryData by FileName and return '''
def findInventoryDataByFileName(FileName):
    res = session.query(
        InventoryData
    ).filter(
        and_(
                InventoryData.FileName == FileName,
            )
        ).one_or_none()
    return res

''' save InventoryData and return '''
def saveInventoryData(payload):
    data = InventoryData(**payload)
    session.add(data)
    session.commit()
    return data

def processInventoryData():
    # excel_file = r"C:\JAIDA\GSIIH_Contracts - HPE\Metadata_Store\Banner Health\HPE_Metadata_BannerHealth_Final_REGENERATED_Qdrant.xlsx"
    # excel_file = r"c:\JAIDA\GSHR Content\HPE\Deltas_Banner_Health\HPE_Metadata_Deltas_Banner_Health_Qdrant.xlsx"
    # excel_file = r'c:\JAIDA\GSHR Content\HPE\Deltas_Banner_Health\HPE_Metadata_Banner_Health_Deltas-NEW-SQLITE.xlsx'
    # excel_file = r'c:\JAIDA\GSHR Content\HPE\InterMountain_Health\HPE_Metadata_InterMountain_Health_Qdrant.xlsx'
    # excel_file = r'c:\JAIDA\GSHR Content\HPE\Providence_ST.joseph\HPE_Metadata_Providence_ST.joseph-NEW.xlsx'
    # excel_file = r'c:\JAIDA\GSHR Content\HPE\Templates\HPE_Templates_Metadata.xlsx'
    # excel_file = r'c:\JAIDA\GSHR Content\HPE\Inventory_SQLITE.xlsx'
    # excel_file = r'c:\JAIDA\GSHR Content\HPE\HPE_POC_01_object_list_2025-01-03_20-58-07.xlsx'
    # excel_file = r'c:\JAIDA\GSHR Content\HPE\Templates\HPE_Templates_Metadata - Nested_Qdrant.xlsx'
    # excel_file = r'c:\JAIDA\GSHR Content\HPE\InterMountain_Health_PAs\HPE_Metadata_InterMountain_Health_PAs_HPE_Qdrant.xlsx'
    # excel_file = r"c:\JAIDA\GSHR Content\HPE\InterMountain_Health_MR829\HPE_Metadata_InterMountain_Health_MR829.xlsx"
    excel_file = r"metadata/Golden Record Lexora-01-08-2026_Processed 1.xlsx"
    sheet_name = 'Content Matrix'
    # BANNER HEALTH INTERMOUNTAIN HEALTH    PROVIDENCE  ST. JOSEPH HEALTH
    # temp_cust_name = 'PROVIDENCE  ST. JOSEPH HEALTH'

    source_excel_df = pd.read_excel(excel_file, sheet_name, dtype=str)
    source_excel_df = source_excel_df.replace({pd.NA: None})
    source_excel_df = source_excel_df[source_excel_df['DocType'].str.upper() == 'PDF']
    # source_excel_df = source_excel_df[source_excel_df['IsValidContractType'].str.upper() == 'YES']
    # source_excel_df = source_excel_df[source_excel_df['STATUS'].str.upper() == 'Ingested'.upper()]
    # source_excel_df = source_excel_df[source_excel_df['Related_Records'].notnull()]
    source_excel_df = source_excel_df.sort_values(by='ContentID')
    source_excel_df = source_excel_df.to_dict(orient='records')
    customer_name_tracker = {}

    for item in source_excel_df:
        # if item.get('Related_Records') is None:
        #     continue
        item['Ingestion_Date'] = None if item.get('Ingestion_Date') is None else datetime.strptime(item['Ingestion_Date'], "%Y-%m-%d %H:%M:%S")
        item['Effective_Date'] = None if item.get('Effective_Date') is None else datetime.strptime(item['Effective_Date'], "%Y-%m-%d %H:%M:%S")
        item['End_Date'] = None if item.get('End_Date') is None else datetime.strptime(item['End_Date'], "%Y-%m-%d %H:%M:%S")
        item['Created_Date'] = None if item.get('Created_Date') is None else datetime.strptime(item['Created_Date'], "%Y-%m-%d %H:%M:%S")
        item['Last_Modified_Date'] = None if item.get('Last_Modified_Date') is None else datetime.strptime(item['Last_Modified_Date'], "%Y-%m-%d %H:%M:%S")
        item['First_Published_Date'] = None if item.get('First_Published_Date') is None else datetime.strptime(item['First_Published_Date'], "%Y-%m-%d %H:%M:%S")
        item['Date_of_Last_Review'] = None if item.get('Date_of_Last_Review') is None else datetime.strptime(item['Date_of_Last_Review'], "%Y-%m-%d %H:%M:%S")
        item['Expiration_Date'] = None if item.get('Expiration_Date') is None else datetime.strptime(item['Expiration_Date'], "%Y-%m-%d %H:%M:%S")
        item['HPE_Title'] = item.get('Title')
        # TEMP CODE TO UPDATE CUSTOMER NAME FROM EXPLOSION REPORT
        # item['Customer_Name'] = temp_cust_name
        if item.get('UCN') is not None and item.get('Parent_UCN') is not None and item.get('UCN') == item.get('Parent_UCN'):
            if f"{item.get('UCN')}#{item.get('Parent_UCN')}" not in customer_name_tracker:
                res = session.query(
                    ExplosionReportData
                ).filter(
                    and_(
                            ExplosionReportData.M_SUPER_PARNT_UNI_CUST_NO == item.get('UCN'),
                        )
                    ).first()
                if res is not None:
                    customer_name_tracker[f"{item.get('UCN')}#{item.get('Parent_UCN')}"] = res.IDN_NAME
            if customer_name_tracker.get(f"{item.get('UCN')}#{item.get('Parent_UCN')}") is not None:
                item['Customer_Name'] = customer_name_tracker[f"{item.get('UCN')}#{item.get('Parent_UCN')}"]


        if item.get('STATUS') == 'Already Ingested':
            item['ContentID'] = item['Comments'].strip()
        inventoryData = findInventoryDataByContentID(ContentID=item.get('ContentID'))
        if inventoryData is None:
            logger.info(f"************ Data missing  - {item.get('ContentID')}")
            inventoryData = saveInventoryData(payload=item)
        else:
            if item.get('FileName') == inventoryData.FileName:
                inventoryData.update(**item)
                session.commit()
            else:
                logger.error(f"************ ContentID mismatch for  - {item.get('FileName')}. Input - {item.get('ContentID')}  DB = {inventoryData.ContentID}")


# =============================================================================
''' find all BindersData and return '''
def findBindersDataAll():
    res = session.query(
        BindersData
    ).all()
    return res

''' find BindersData by Params and return '''
def findBindersDataByParams(conditions = {}):
    if len(conditions) == 0:
        return []
    and_function = generate_and_function(*conditions)
    results = session.query(BindersData).filter(and_function).all()
    results= convertRowToDict(BindersData, results)
    return results

''' save BindersData and return '''
def saveBindersData(payload):
    data = BindersData(**payload)
    session.add(data)
    session.commit()
    return data
   
def processBindersDataCommercialContract(id_prefix = '', customer_name = '', parent_UCN = ''):
    conditions = [InventoryData.Record_Type == 'CONTRACT COMMERCIAL DOCUMENT']
    if len(id_prefix) > 0:
        conditions.append(InventoryData.ContentID.like(f"{id_prefix}%"))
    if len(customer_name) > 0:
        conditions.append(InventoryData.Customer_Name == customer_name)
    if len(parent_UCN) > 0:
        conditions.append(InventoryData.Parent_UCN == parent_UCN)
    # temp
    # conditions.append(InventoryData.pk_id >= 2368)
    records = findInventoryDataByParams(conditions)
    binders = {}
    for record in records:
        if not record.get('Contract_Type') in binders:
            binders[record.get('Contract_Type')] = {}
        prefix = record.get('Article_Number')[:record.get('Article_Number').index('.')]
        suffix = int(record.get('Article_Number')[record.get('Article_Number').index('.')+1:])
        if not prefix in binders[record.get('Contract_Type')]:
            binders[record.get('Contract_Type')][prefix] = {"parent": None, "children": [], "status": 1}
        if suffix == 1:
            binders[record.get('Contract_Type')][prefix]['parent'] = record
        else:
            binders[record.get('Contract_Type')][prefix]['children'].append(record)
    
    for ct, binderDict in binders.items():
        for prefix, binderObj in binderDict.items():
            contract_type = ct
            trim_number = prefix
            binder_identifier = prefix[prefix.index('~')+1:]
            ucn = None if binderObj.get('parent') is None and len(binderObj.get('children')) == 0 else binderObj.get('parent').get('UCN') if binderObj.get('parent') is not None else binderObj['children'][0].get('UCN') if len(binderObj['children']) > 0 else None
            policy_number = None if binderObj.get('parent') is None and len(binderObj.get('children')) == 0 else binderObj.get('parent').get('Policy_Number') if binderObj.get('parent') is not None else binderObj['children'][0].get('Policy_Number') if len(binderObj['children']) > 0 else None
            parent_content_id = None if binderObj.get('parent') is None else binderObj.get('parent').get('ContentID')
            parent_record_number = None if binderObj.get('parent') is None else binderObj.get('parent').get('Article_Number')
            # if binderObj.get('children') is not None and len(binderObj.get('children')) > 0:
            #     sorted_list = sorted(binderObj.get('children'), key=lambda x: x['Article_Number'])
            child_content_ids = None if binderObj.get('children') is None or len(binderObj.get('children')) == 0 else ','.join([str(item.get('ContentID')) for item in binderObj.get('children')])
            child_record_numbers = None if binderObj.get('children') is None or len(binderObj.get('children')) == 0 else ','.join([str(item.get('Article_Number')) for item in binderObj.get('children')])
            ucn_type = findUCNType(ucn)
            if ucn_type == 'PARENT':
                parent_ucn = ucn
            elif ucn_type == 'INDIVIDUAL':
                rows = session.scalars(select(ExplosionReportData).where(ExplosionReportData.INDIV_UCN == ucn)).fetchall()
                pucns = [it.M_SUPER_PARNT_UNI_CUST_NO for it in rows]
                ns = [it.IDN_NAME for it in rows]
                try:
                    indx = -1 if binderObj.get('parent') is None else ns.index(binderObj.get('parent').get('Customer_Name'))
                except ValueError as e:
                    print(f"***** EXCEPTION --- {e}")
                    indx = -1
                parent_ucn = pucns[indx] if indx != -1 else None if pucns is None or len(pucns) == 0 else pucns[0]
            elif ucn_type == 'SHIPTO':
                rows = session.scalars(select(ExplosionReportData).where(ExplosionReportData.MEMBER_SHIPTO_UCN == ucn)).fetchall()
                pucns = [it.M_SUPER_PARNT_UNI_CUST_NO for it in rows]
                ns = [it.IDN_NAME for it in rows]
                try:
                    indx = ns.index(binderObj.get('parent').get('Customer_Name'))
                except ValueError as e:
                    print(f"***** EXCEPTION --- {e}")
                    indx = -1
                parent_ucn = pucns[indx] if indx != -1 else None if pucns is None or len(pucns) == 0 else pucns[0]
            else:
                parent_ucn = ucn
            conditions = [BindersData.ucn == ucn, BindersData.contract_type == contract_type, BindersData.policy_number == policy_number, BindersData.trim_number == trim_number, BindersData.binder_identifier == binder_identifier, BindersData.parent_content_id == parent_content_id]
            payload = {'parent_ucn':parent_ucn, 'ucn': ucn, 'contract_type': contract_type, 'policy_number': policy_number, 'trim_number': trim_number, 'binder_identifier': binder_identifier, 
                       'parent_content_id': parent_content_id, 'child_content_ids': child_content_ids, 'summary': None, 'comments': None, 'status': binderObj.get('status'),
                       'parent_record_number': parent_record_number, 'child_record_numbers': child_record_numbers}
            if payload['parent_content_id'] is None:
                    payload['comments'] = 'Unable find the Parent/Master Contract'
                    payload['status'] = 0
            results = findBindersDataByParams(conditions)
            if len(results) == 0:                
                bindersData = saveBindersData(payload=payload)
            else:
                for rec in results:
                    session.query(BindersData).filter(BindersData.binders_id == rec.get('binders_id')).update(payload)
                    session.commit()

# Recursive function to retrieve hierarchical data
def fetchChildren(parent_artNumbr, Related_Records, children_t, relation_map = [], child_track=[], parents_t_Article_Numbers = []):
    if Related_Records is None:
        return []
    
    children = []
    pattern = r'\b\d+~\d+\.\d{1,3}\b'
    # Find all matches in the string
    matches = re.findall(pattern, Related_Records)
    for match in matches:
        if match in parents_t_Article_Numbers:
            continue
        if match in children_t:
            if match not in child_track:
                children.append(children_t[match])
                child_track.append(match)
            if f"{parent_artNumbr}#{match}" not in relation_map:
                relation_map.append(f"{parent_artNumbr}#{match}")
                children.extend(fetchChildren(match, children_t.get(match, {}).get('Related_Records'), children_t, relation_map, child_track, parents_t_Article_Numbers))
        else:
            conditions = [InventoryData.Article_Number == match]
            records = findInventoryDataByParams(conditions)
            for rec in records:
                if match not in child_track:
                    children.append(rec)
                    child_track.append(match)
                if f"{parent_artNumbr}#{match}" not in relation_map:
                    relation_map.append(f"{parent_artNumbr}#{match}")
                    children.extend(fetchChildren(rec.get('Article_Number'), rec.get('Related_Records'), children_t, relation_map, child_track, parents_t_Article_Numbers))
    return children

def separateParentChildren(childs_dict, parent_key):
    if childs_dict is None or len(childs_dict) == 0:
        return None, []
    elif parent_key is None:
        return None, list(childs_dict.values())
    else:
        p = childs_dict.get(parent_key)
        if p is None:
            cs = childs_dict
        else:            
            del childs_dict[parent_key]
            cs = list(childs_dict.values())
        return p, cs

def processBindersDataProdcutContract(id_prefix = '', customer_name = '', parent_UCN = ''):
    conditions = [InventoryData.Record_Type == 'CONTRACT PA DOCUMENT']
    if len(id_prefix) > 0:
        conditions.append(InventoryData.ContentID.like(f"{id_prefix}%"))
    if len(customer_name) > 0:
        conditions.append(InventoryData.Customer_Name == customer_name)
    if len(parent_UCN) > 0:
        conditions.append(InventoryData.Parent_UCN == parent_UCN)
    # temp
    # conditions.append(InventoryData.pk_id >= 2368)
    records = findInventoryDataByParams(conditions)
    # result_dicts = [{column.name: getattr(row, column.name) for column in InventoryData.__table__.columns} for row in records]
    sorted_list = sorted(records, key=lambda x: x['Article_Number'])
    sorted_dict = {item.get('Article_Number'): item for item in sorted_list}
    parents_t = {}
    children_t = {}
    word = 'Related to:'
    for artNumbr, obj in sorted_dict.items():
        if obj.get('End_Date') is not None or 'Add Prod Agree'.upper() in obj.get('HPE_Title').upper():
            parents_t[obj.get('Article_Number')] = obj
        else:
            children_t[obj.get('Article_Number')] = obj
            # if word in obj.get('Related_Records') or obj.get('End_Date') is not None:
            #     wcnt = obj.get('Related_Records').count(word)
            #     if wcnt > 1:
            #         parents_t[obj.get('Article_Number')] = obj
            #     else:
            #         children_t[obj.get('Article_Number')] = obj
    # Iterate through the parents and build the binder instance
    parents_t_Article_Numbers = list(parents_t.keys())
    for artNumbr, obj in parents_t.items():
        relation_map = []
        child_track = []
        prefix = artNumbr[:artNumbr.index('.')]
        childs = fetchChildren(artNumbr, obj.get('Related_Records'), children_t, relation_map, child_track, parents_t_Article_Numbers)
        childs_dict = {it.get('Article_Number'): it for it in childs}
        childs_dict = {key: childs_dict[key] for key in childs_dict if key not in parents_t_Article_Numbers}
        if artNumbr in childs_dict:
            del childs_dict[artNumbr]
        childs = list(childs_dict.values())
        del childs_dict
        # Now store the binder
        contract_type = obj.get('Contract_Type')
        trim_number = prefix
        binder_identifier = prefix[prefix.index('~')+1:]
        ucn = None if obj.get('UCN') is None and len(childs) == 0 else obj.get('UCN') if obj is not None else childs[0].get('UCN') if len(childs) > 0 else None
        policy_number = None if obj.get('Policy_Number') is None and len(childs) == 0 else obj.get('Policy_Number') if obj is not None else childs[0].get('Policy_Number') if len(childs) > 0 else None
        parent_content_id = obj.get('ContentID')        
        parent_record_number = obj.get('Article_Number')
        child_content_ids = None if childs is None or len(childs) == 0 else ','.join([str(item.get('ContentID')) for item in childs])
        child_record_numbers = None if childs is None or len(childs) == 0 else ','.join([str(item.get('Article_Number')) for item in childs])
        ucn_type = findUCNType(ucn)
        if ucn_type == 'PARENT':
            parent_ucn = ucn
        elif ucn_type == 'INDIVIDUAL':
            rows = session.scalars(select(ExplosionReportData).where(ExplosionReportData.INDIV_UCN == ucn)).fetchall()
            pucns = [it.M_SUPER_PARNT_UNI_CUST_NO for it in rows]
            ns = [it.IDN_NAME for it in rows]
            try:
                indx = ns.index(obj.get('Customer_Name'))
            except ValueError as e:
                print(f"***** EXCEPTION --- {e}")
                indx = -1
            parent_ucn = pucns[indx] if indx != -1 else None if pucns is None or len(pucns) == 0 else pucns[0]
        elif ucn_type == 'SHIPTO':
            rows = session.scalars(select(ExplosionReportData).where(ExplosionReportData.MEMBER_SHIPTO_UCN == ucn)).fetchall()
            pucns = [it.M_SUPER_PARNT_UNI_CUST_NO for it in rows]
            ns = [it.IDN_NAME for it in rows]
            try:
                indx = ns.index(obj.get('Customer_Name'))
            except ValueError as e:
                print(f"***** EXCEPTION --- {e}")
                indx = -1
            parent_ucn = pucns[indx] if indx != -1 else None if pucns is None or len(pucns) == 0 else pucns[0]
        else:
            parent_ucn = ucn
        conditions = [BindersData.ucn == ucn, BindersData.contract_type == contract_type, BindersData.trim_number == trim_number, BindersData.binder_identifier == binder_identifier, BindersData.parent_content_id == parent_content_id]
        payload = {'parent_ucn':parent_ucn, 'ucn': ucn, 'contract_type': contract_type, 'policy_number': policy_number, 'trim_number': trim_number, 'binder_identifier': binder_identifier, 
                    'parent_content_id': parent_content_id, 'child_content_ids': child_content_ids, 'summary': None, 'comments': None, 'status': 1,
                    'parent_record_number': parent_record_number, 'child_record_numbers': child_record_numbers}
        if payload['parent_content_id'] is None:
                payload['comments'] = 'Unable find the Parent/Master Contract'
                payload['status'] = 0
        records = findBindersDataByParams(conditions)
        if len(records) == 0:                
            bindersData = saveBindersData(payload=payload)
        else:
            for rec in records:
                session.query(BindersData).filter(BindersData.binders_id == rec.get('binders_id')).update(payload)
                session.commit()

    # Iterate through the childs. process for one entry items
    children_t = {}
    for artNumbr, obj in children_t.items():
        relation_map = []
        child_track = []
        final_p = None
        final_cs = []
        pattern = r'\b\d+~\d+\.\d{1,3}\b'
        # Find all matches in the string
        matches = re.findall(pattern, obj.get('Related_Records'))
        if len(matches) == 1:
            prefix = artNumbr[:artNumbr.index('.')]
            childs = fetchChildren(artNumbr, obj.get('Related_Records'), children_t, relation_map, child_track, parents_t_Article_Numbers)

            # Verify which one is the parent
            childs_dict = {}
            suffixes = []
            titles = []
            for item in childs:
                childs_dict[item.get('Article_Number')] = item
                suffixes.append(int(item.get('Article_Number')[item.get('Article_Number').index('.')+1:]))
                titles.append(item.get('HPE_Title'))
            if artNumbr not in childs_dict:
                childs_dict[artNumbr] = obj
                suffixes.append(int(obj.get('Article_Number')[obj.get('Article_Number').index('.')+1:]))
                titles.append(obj.get('HPE_Title'))
            
            #Find Titles has 'Add Prod Agree' word
            substring = 'Add Prod Agree'.upper()
            # Using filter() to get elements containing the substring            
            filtered_strings = list(filter(lambda s: substring in s.upper(), titles))
            if len(filtered_strings) > 0:
                # Using list comprehension and enumerate() to get indices of matching elements
                indices = [index for index, string in enumerate(titles) if substring in string.upper()]
                if len(indices) > 1:
                    suffixes = []
                    for indice in indices:
                        suffixes.append(int(childs_dict[list(childs_dict.keys())[indice]]['Article_Number'][childs_dict[list(childs_dict.keys())[indice]]['Article_Number'].index('.')+1:]))
                    # Find min number to get parent
                    low = 0 if len(suffixes) == 0 else min(suffixes)
                    if low > 0:
                        at = suffixes.index(low)
                        at = indices[at]
                        final_p = childs_dict[list(childs_dict.keys())[indices[suffixes.index(low)]]]
                        del childs_dict[list(childs_dict.keys())[indices[suffixes.index(low)]]]
                        final_cs = list(childs_dict.values())
                else:
                    at = indices[0]
                    final_p = childs_dict[list(childs_dict.keys())[at]]
                    del childs_dict[list(childs_dict.keys())[at]]
                    final_cs = list(childs_dict.values())
            else:
                # Find min number to get parent
                low = 0 if len(suffixes) == 0 else min(suffixes)
                if low > 0:
                    at = suffixes.index(low)
                    final_p, final_cs = separateParentChildren(childs_dict, list(childs_dict.keys())[at])


            # Now store the binder
            contract_type = obj.get('Contract_Type')
            trim_number = prefix
            binder_identifier = prefix[prefix.index('~')+1:]
            ucn = None if final_p is None or (final_p.get('UCN') is None and len(final_cs) == 0) else final_p.get('UCN') if final_p is not None else final_cs[0].get('UCN') if len(final_cs) > 0 else None
            parent_content_id = None if final_p is None else final_p.get('ContentID')
            parent_record_number = None if final_p is None else final_p.get('Article_Number')
            child_content_ids = None if final_cs is None or len(final_cs) == 0 else ','.join([str(item.get('ContentID')) for item in final_cs])
            child_record_numbers = None if final_cs is None or len(final_cs) == 0 else ','.join([str(item.get('Article_Number')) for item in final_cs])
            ucn_type = findUCNType(ucn)
            if ucn_type == 'PARENT':
                parent_ucn = ucn
            elif ucn_type == 'INDIVIDUAL':
                rows = session.scalars(select(ExplosionReportData).where(ExplosionReportData.INDIV_UCN == ucn)).fetchall()
                pucns = [it.M_SUPER_PARNT_UNI_CUST_NO for it in rows]
                ns = [it.IDN_NAME for it in rows]
                try:
                    indx = -1 if final_p is None else ns.index(final_p.get('Customer_Name'))
                except ValueError as e:
                    print(f"***** EXCEPTION --- {e}")
                    indx = -1
                parent_ucn = pucns[indx] if indx != -1 else None if pucns is None or len(pucns) == 0 else pucns[0]
            elif ucn_type == 'SHIPTO':
                rows = session.scalars(select(ExplosionReportData).where(ExplosionReportData.MEMBER_SHIPTO_UCN == ucn)).fetchall()
                pucns = [it.M_SUPER_PARNT_UNI_CUST_NO for it in rows]
                ns = [it.IDN_NAME for it in rows]
                try:
                    indx = -1 if final_p is None else ns.index(final_p.get('Customer_Name'))
                except ValueError as e:
                    print(f"***** EXCEPTION --- {e}")
                    indx = -1
                parent_ucn = pucns[indx] if indx != -1 else None if pucns is None or len(pucns) == 0 else pucns[0]

            conditions = [BindersData.ucn == ucn, BindersData.contract_type == contract_type, BindersData.trim_number == trim_number, BindersData.binder_identifier == binder_identifier, BindersData.parent_content_id == parent_content_id]
            payload = {'parent_ucn': parent_ucn, 'ucn': ucn, 'contract_type': contract_type, 'trim_number': trim_number, 'binder_identifier': binder_identifier, 
                        'parent_content_id': parent_content_id, 'child_content_ids': child_content_ids, 'summary': None, 'comments': None, 'status': 1,
                        'parent_record_number': parent_record_number, 'child_record_numbers': child_record_numbers}
            if payload['parent_content_id'] is None:
                    payload['comments'] = 'Unable find the Parent/Master Contract'
                    payload['status'] = 0
            records = findBindersDataByParams(conditions)
            if len(records) == 0:                
                bindersData = saveBindersData(payload=payload)
            else:
                for rec in records:
                    rec.update(**payload)
                    session.commit()

def createGoldenRecords():
    inv_records = findInventoryDataAll()
    inv_records = convertRowToDict(InventoryData, inv_records)
    inv_dict = {a['ContentID']: a for a in inv_records}

    binders_records = findBindersDataAll()
    binders_records = convertRowToDict(BindersData, binders_records)
    # Using defaultdict with list comprehension
    binders_dict = defaultdict(list)
    [binders_dict[a["binder_identifier"]].append(a) for a in binders_records]
    # Convert defaultdict to a normal dictionary if needed
    binders_dict = dict(binders_dict)
    for inv_record in inv_records:
        if inv_record['Article_Type'] == 'Template':
            continue
        inv_record['Product_Agreement_Or_Amendment'] = 'N/A'
        if inv_record['Contract_Type'] == 'PRODUCT AGREEMENT':
            if inv_record['End_Date'] is not None:
                inv_record['Product_Agreement_Or_Amendment'] = 'AGREEMENT'
            else:
                inv_record['Product_Agreement_Or_Amendment'] = 'AMENDMENT'
                prefix = inv_record.get('Article_Number')[:inv_record.get('Article_Number').index('.')]
                suffix = int(inv_record.get('Article_Number')[inv_record.get('Article_Number').index('.')+1:])
                binder_identifier = prefix[prefix.index('~')+1:]
                for rec in binders_records:
                    if inv_record['Parent_UCN'] == rec['parent_ucn']:
                        if rec['child_content_ids']is not None and len(rec['child_content_ids']) > 0:
                            if inv_record['ContentID'] in rec['child_content_ids'].split(','):
                                inv_record['End_Date'] = None if rec['parent_content_id'] is None else inv_dict[rec['parent_content_id']]['End_Date']
                                break


                        # if rec['contract_type'] in ['MASTER AGREEMENT', 'MASTER W/ CSC', 'MASTER BUSINESS ASSOCIATE']:
                        #     inv_record['End_Date'] = None if rec['parent_content_id'] is None else inv_dict[rec['parent_content_id']]['End_Date']
                            

    pass
    df = pd.DataFrame(inv_records)
    if 'Article_Number' in df.columns:
        df['Article_Number'] = df['Article_Number'].astype(str)
    if 'UCN' in df.columns:
        df['UCN'] = df['UCN'].astype(str)
    if 'Policy_Number' in df.columns:
        df['Policy_Number'] = df['Policy_Number'].astype(str)
    if 'Parent_UCN' in df.columns:
        df['Parent_UCN'] = df['Parent_UCN'].astype(str)
    # Write DataFrame to Excel file
    df.to_excel('3Customers.xlsx', index=False, sheet_name='3 Customers')
    #Filter Banner health
    banner_df = df[df['Parent_UCN'] == '01018471']

    banner_df.reset_index(drop=True, inplace=True)
    banner_df = banner_df.copy()
    # Convert End_Date column to datetime, handling None values
    banner_df['End_Date'] = pd.to_datetime(banner_df['End_Date'], errors='coerce')

    # Get today's date
    # today = datetime.today()
    # Define the target check date (1st March 2025)
    check_date = datetime(2025, 3, 1)

    # Define the date range (last 30 days from today)
    # last_30_days = today - timedelta(days=30)

    # Filter data: Include rows where End_Date is within the last 30 days OR is NaT (None)
    # filtered_df = banner_df[(banner_df['End_Date'].isna()) | (banner_df['End_Date'] >= last_30_days)]
    filtered_df = banner_df[(banner_df['End_Date'].isna()) | (banner_df['End_Date'] >= check_date)]
    filtered_df.reset_index(drop=True, inplace=True)

    filtered_df.to_excel('Banner_Health_1st_March.xlsx', index=False, sheet_name='BANNER HEALTH')

def updateGoldenMD():
    excel_file = r"metadata/Golden Record Lexora-01-08-2026_Processed 1.xlsx"
    sheet_name = 'Content Matrix'
    df = pd.read_excel(excel_file, sheet_name, dtype=str, keep_default_na=False, na_values=[""])
    df = df.replace({pd.NA: None})
    # Replace None with NaN
    df = df.replace(np.nan, None)
    df.reset_index(drop=True, inplace=True)
    df.columns = df.columns.str.strip()
    excel_metadata_json = df.to_dict(orient='records')
    for index, item in enumerate(excel_metadata_json):
        item['Ingestion_Date'] = None if item.get('Ingestion_Date') is None else datetime.strptime(item['Ingestion_Date'], "%Y-%m-%d %H:%M:%S")
        item['Effective_Date'] = None if item.get('Effective_Date') is None else datetime.strptime(item['Effective_Date'], "%Y-%m-%d %H:%M:%S")
        item['End_Date'] = None if item.get('End_Date') is None else datetime.strptime(item['End_Date'], "%Y-%m-%d %H:%M:%S")
        item['Created_Date'] = None if item.get('Created_Date') is None else datetime.strptime(item['Created_Date'], "%Y-%m-%d %H:%M:%S")
        item['Last_Modified_Date'] = None if item.get('Last_Modified_Date') is None else datetime.strptime(item['Last_Modified_Date'], "%Y-%m-%d %H:%M:%S")
        item['First_Published_Date'] = None if item.get('First_Published_Date') is None else datetime.strptime(item['First_Published_Date'], "%Y-%m-%d %H:%M:%S")
        item['Date_of_Last_Review'] = None if item.get('Date_of_Last_Review') is None else datetime.strptime(item['Date_of_Last_Review'], "%Y-%m-%d %H:%M:%S")
        item['Expiration_Date'] = None if item.get('Expiration_Date') is None else datetime.strptime(item['Expiration_Date'], "%Y-%m-%d %H:%M:%S")
        item['Demo'] = 'YES'
        

        inventoryData = findInventoryDataByContentID(ContentID=item.get('ContentID'))        
        if inventoryData is not None:
            if item.get('FileName') == inventoryData.FileName:
                inventoryData.update(**item)
                session.commit()
            else:
                logger.error(f"************ ContentID mismatch for  - {item.get('FileName')}. Input - {item.get('ContentID')}  DB = {inventoryData.ContentID}")

# =============================================================================

if __name__ == '__main__':
    # processExplosionData()
    # processInventoryData()
    # # BANNER HEALTH => 01018471 INTERMOUNTAIN HEALTH => 01533908 PROVIDENCE  ST. JOSEPH HEALTH => 01018864
    # Only working one at a time, need to optimize later
    processBindersDataCommercialContract(parent_UCN= '01030242')
    processBindersDataProdcutContract(parent_UCN= '01030242')
    # '01018471, 01018845, 01030242'
    createGoldenRecords()
    updateGoldenMD()
    pass
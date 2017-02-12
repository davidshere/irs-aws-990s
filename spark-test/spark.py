from collections import defaultdict

from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, Row
sql_context = SQLContext(sc)

df = sql_context.read.format('com.databricks.spark.xml').options(rowTag='Return').load('../get_schema_from_data/3*.xml.gz')

return_versions = df.select('ObjectID', '_returnVersion')
preparer_info = df.select('ObjectID', 'ReturnHeader.PreparerFirm.PreparerFirmBusinessName.BusinessNameLine1',  'ReturnHeader.PreparerFirm.PreparerFirmUSAddress.AddressLine1', 'ReturnHeader.PreparerFirm.PreparerFirmUSAddress.City', 'ReturnHeader.PreparerFirm.PreparerFirmUSAddress.State', 'ReturnHeader.PreparerFirm.PreparerFirmUSAddress.ZipCode')
filer_info = df.select(['ObjectID', 'ReturnHeader.Filer.USAddress.AddressLine1', 'ReturnHeader.Filer.USAddress.AddressLine2', 'ReturnHeader.Filer.USAddress.City','ReturnHeader.Filer.USAddress.State','ReturnHeader.Filer.USAddress.ZipCode','ReturnHeader.Officer.Name','ReturnHeader.Officer.Title'])
mission = df.select('ObjectID', 'ReturnData.IRS990.ActivityOrMissionDescription', 'ReturnData.IRS990.MissionDescription', 'ReturnData.IRS990.Description')
people = df.select('ObjectID', 'ReturnData.IRS990.Form990PartVIISectionA.NamePerson', 'ReturnData.IRS990.Form990PartVIISectionA.Title', 'ReturnData.IRS990.Form990PartVIISectionA.AverageHoursPerWeek', 'ReturnData.IRS990.Form990PartVIISectionA.IndividualTrusteeOrDirector', 'ReturnData.IRS990.Form990PartVIISectionA.Officer', 'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromOrganization', 'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromRelatedOrgs', 'ReturnData.IRS990.Form990PartVIISectionA.OtherCompensation')

organization

mission = df.select('ObjectID', 'ReturnData.IRS990.ActivityOrMissionDescription', 'ReturnData.IRS990.MissionDescription', 'ReturnData.IRS990.Description')
people = df.select('ObjectID', 'ReturnData.IRS990.Form990PartVIISectionA.NamePerson', 'ReturnData.IRS990.Form990PartVIISectionA.Title', 'ReturnData.IRS990.Form990PartVIISectionA.AverageHoursPerWeek', 'ReturnData.IRS990.Form990PartVIISectionA.KeyEmployee', 'ReturnData.IRS990.Form990PartVIISectionA.IndividualTrusteeOrDirector', 'ReturnData.IRS990.Form990PartVIISectionA.Officer', 'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromOrganization', 'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromRelatedOrgs', 'ReturnData.IRS990.Form990PartVIISectionA.OtherCompensation')

people_paths = [
'ObjectID',
'ReturnData.IRS990.Form990PartVIISectionA.NamePerson', 
'ReturnData.IRS990.Form990PartVIISectionA.Title', 
'ReturnData.IRS990.Form990PartVIISectionA.AverageHoursPerWeek', 
'ReturnData.IRS990.Form990PartVIISectionA.KeyEmployee', 
'ReturnData.IRS990.Form990PartVIISectionA.IndividualTrusteeOrDirector', 
'ReturnData.IRS990.Form990PartVIISectionA.Officer', 
'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromOrganization', 
'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromRelatedOrgs', 
'ReturnData.IRS990.Form990PartVIISectionA.OtherCompensation',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.PersonName',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.AddressUS.AddressLine1',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.AddressUS.City',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.AddressUS.State',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.AddressUS.ZIPCode',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.Title',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.AvgHoursPerWkDevotedToPosition',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.Compensation',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.ContriToEmplBenefitPlansEtc',
'ReturnData.IRS990EZ.OfficerDirectorTrusteeKeyEmpl.ExpenseAccountOtherAllowances',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.PersonNm',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.USAddress',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.USAddress.AddressLine1',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.USAddress.City',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.USAddress.State',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.USAddress.ZIPCode',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.TitleTxt',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.AverageHrsPerWkDevotedToPosRt',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.CompensationAmt',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.EmployeeBenefitProgramAmt',
'ReturnData.IRS990PF.OfficerDirTrstKeyEmplGrp.ExpenseAccountOtherAllwncAmt',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.PersonName',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.USAddress.AddressLine1',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.USAddress.City',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.USAddress.State',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.USAddress.ZIPCode',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.Title',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.AvgHoursPerWkDevotedToPosition',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.Compensation',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.ContriToEmplBenefitPlansEtc',
'ReturnData.IRS990PF.OfcrDirTrusteesKeyEmployeeInfo.OfcrDirTrusteesOrKeyEmployee.ExpenseAccountOtherAllowances']

people columns ='''
ObjectID
NamePerson
Title
AverageHoursPerWeek
IndividualTrusteeOrDirector
Officer
ReportableCompFromOrganization
ReportableCompFromRelatedOrgs
OtherCompensation
KeyEmployee
'''

def map_paths(schema, recursion_level=0, parents=[], results=[]):
    for element in schema:
        dt = element.dataType
        if isinstance(dt, StructType):
            new_parent = parents + [element.name]
            map_paths(dt, recursion_level + 1, new_parent, results)
        else:
            result = [element.name, '.'.join(parents + [element.name])]
            results.append(result)
    return results

def map_row(row, recursion_level=0, parents=[], results=[]):
    for element in row:
        dt = element.dataType
        if isinstance(dt, StructType):
            new_parent = parents + [element.name]
            map_paths(dt, recursion_level + 1, new_parent, results)
        else:
            result = [element.name, '.'.join(parents + [element.name])]
            results.append(result)
    return results




def map_row(row, results=[], parents=[]):
    if isinstance(row, Row):
        row = row.asDict(recursive=True)
    for element in row:
        if isinstance(row[element], dict):
            new_parents = parents + [element]
            map_row(row[element], results=results, parents=new_parents)
        else:
            result = '.'.join(parents + [element])
            results.append(result)
    return results


df1 = df.select('_returnVersion', 'ReturnHeader', 'ReturnData')
rows = df1.collect()

schema_versions = defaultdict(set)
start = time.time()
for row in df1.toLocalIterator():
    return_version = row._returnVersion
    schema_map = map_row(row, results=[])
    schema_set = set(schema_map)
    schema_versions[return_version].update(schema_set)

l = time.time() - start
print l * 1350, 'seconds'






df.select('ObjectID', '_returnVersion', 'ReturnHeader.ReturnTypeCd', 'ReturnHeader', 'ReturnData')

df.select('ObjectID', '_returnVersion', 'ReturnHeader')

paths = map_paths(rh.schema)





f = df.select('ReturnHeader.Filer')
f.printSchema()


# EMR
df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='Return').load('s3://irs-form-990-hadoop/xml-files/*')
return_versions.write.csv('s3://irs-form-990-hadoop/return_versions')





## Lists of people into pandas
### start ###
df = pd.read_csv('ppl_df.csv') # or people.toPandas()
df = df.iloc[:, 1:]
df.ObjectID = df.ObjectID.astype('str')

def parse_list_string(string):
    x = string.replace('u\'', '').replace('[', '').replace(']', '').replace('\'', '').split(",")
    return x

person_df = df[df.Title.notnull()]

input_data = person_df.values.tolist()

rows = []
for input_row in input_data:
    object_id = input_row.pop(0)

    person_fact_arrays = [parse_list_string(j) for j in input_row]
    zipped = [a for a in zip(*person_fact_arrays)]

    for row in zipped:
        data = [object_id]
        data.extend(row)
        rows.append(data)
        
columns_headers = person_df.columns.values.tolist()
rows.insert(0, columns_headers)

### end ###

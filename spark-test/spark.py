from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, ArrayType
sqlContext = SQLContext(sc)

df = sqlContext.read
		.format('com.databricks.spark.xml')
		.options(rowTag='Return')
		.load('../spark-test/xml/*.xml')

df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='Return').load('xml/*.xml')

return_versions = df.select('ObjectID', '_returnVersion')
preparer_info = df.select('ObjectID', 'ReturnHeader.PreparerFirm.PreparerFirmBusinessName.BusinessNameLine1',  'ReturnHeader.PreparerFirm.PreparerFirmUSAddress.AddressLine1', 'ReturnHeader.PreparerFirm.PreparerFirmUSAddress.City', 'ReturnHeader.PreparerFirm.PreparerFirmUSAddress.State', 'ReturnHeader.PreparerFirm.PreparerFirmUSAddress.ZipCode')
filer_info = df.select(['ObjectID', 'ReturnHeader.Filer.USAddress.AddressLine1', 'ReturnHeader.Filer.USAddress.AddressLine2', 'ReturnHeader.Filer.USAddress.City','ReturnHeader.Filer.USAddress.State','ReturnHeader.Filer.USAddress.ZipCode','ReturnHeader.Officer.Name','ReturnHeader.Officer.Title'])
mission = df.select('ObjectID', 'ReturnData.IRS990.ActivityOrMissionDescription', 'ReturnData.IRS990.MissionDescription', 'ReturnData.IRS990.Description')
people = df.select('ObjectID', 'ReturnData.IRS990.Form990PartVIISectionA.NamePerson', 'ReturnData.IRS990.Form990PartVIISectionA.Title', 'ReturnData.IRS990.Form990PartVIISectionA.AverageHoursPerWeek', 'ReturnData.IRS990.Form990PartVIISectionA.IndividualTrusteeOrDirector', 'ReturnData.IRS990.Form990PartVIISectionA.Officer', 'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromOrganization', 'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromRelatedOrgs', 'ReturnData.IRS990.Form990PartVIISectionA.OtherCompensation')



mission = df.select('ObjectID', 'ReturnData.IRS990.ActivityOrMissionDescription', 'ReturnData.IRS990.MissionDescription', 'ReturnData.IRS990.Description')
people = df.select('ObjectID', 'ReturnData.IRS990.Form990PartVIISectionA.NamePerson', 'ReturnData.IRS990.Form990PartVIISectionA.Title', 'ReturnData.IRS990.Form990PartVIISectionA.AverageHoursPerWeek', 'ReturnData.IRS990.Form990PartVIISectionA.IndividualTrusteeOrDirector', 'ReturnData.IRS990.Form990PartVIISectionA.Officer', 'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromOrganization', 'ReturnData.IRS990.Form990PartVIISectionA.ReportableCompFromRelatedOrgs', 'ReturnData.IRS990.Form990PartVIISectionA.OtherCompensation')



def get_all_schema_types(schema, recursion_level=0):
	for element in schema:
		dt = element.dataType
		if isinstance(dt, StructType):
			print '-', element.name, recursion_level
			get_all_schema_types(dt, recursion_level + 1)
		else:
			print '*', element.name, recursion_level

f = df.select('ReturnHeader.Filer')
get_all_schema_types(f.schema)

def get_all_schema_types(schema, parents=[], siblings=[]):
	for element in schema:
		dt = element.dataType
		parents.append(element.name)
		if isinstance(dt, StructType):
			get_all_schema_types(dt, parents, siblings)
		else:
			print '*', '.'.join(parents)
			parents.pop()

get_all_schema_types(rh.schema)


x=get_all_schema_types(schema)

# EMR
df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='Return').load('s3://irs-form-990-hadoop/xml/*')
return_versions.write.csv('s3://irs-form-990-hadoop/return_versions.csv')




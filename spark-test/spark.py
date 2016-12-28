from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
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



def map_paths(schema, recursion_level=0, parents=[]):
	for element in schema:
		dt = element.dataType
		if isinstance(dt, StructType):
			new_parent = parents + [element.name]
			map_paths(dt, recursion_level + 1, new_parent)
		else:
			print element.name, '.'.join(parents + [element.name])

map_paths(f.schema)

f = df.select('ReturnHeader.Filer')
f.printSchema()


# EMR
df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='Return').load('s3://irs-form-990-hadoop/xml/*')
return_versions.write.csv('s3://irs-form-990-hadoop/return_versions.csv')




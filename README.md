## Tools for sorting through IRS 990 Filings on AWS

This is a work in progress. Ultimately I'd like to build out a process to automate updating 501cdata.org, which will involve sprucing up these scripts. So far I've got:
 - refresh_index.py
   - a script to download the datasets indices, which contain information about which organizations tax returns are available
 - refresh_schemas.py
   - a script to download the schema information detailing the contents of the XML tax returns themselves. Now the script captures schema versions, form types, tags, and their descriptions, and line numbers.
 - schema.py
   - A class to model the schema documents retrieved by refresh_schemas.py. This class mostly focuses on parsing the xsd files.

In `mr/`, there's code to analyze the tax returns using the python MapReduce library MRJob. If you want to use them, you'll unfortunately have to transform the returns from individual XML files to row-delimited files with many returns, like I did. That said:
 - map_xml_schema.py
   - Parses a tax return, finds its paticular version, and maps it to unique XML paths found within that document. That provides a universe of possible queries for when you want to actually pull out information.
 - mr_select.py
   - Takes a set of paths and searches the returns for data matching one of those paths. Returns a mapping of objects where data was found to the data that was found.

**Amazon's [description](https://aws.amazon.com/public-datasets/irs-990/) of the data:**

Machine-readable data from certain electronic 990 forms filed with the IRS from 2011 to present are available for anyone to use via Amazon S3.

Form 990 is the form used by the United States Internal Revenue Service to gather financial information about nonprofit organizations. Data for each 990 filing is provided in an XML file that contains structured information that represents the main 990 form, any filed forms and schedules, and other control information describing how the document was filed. Some non-disclosable information is not included in the files.

This data set includes Forms 990, 990-EZ and 990-PF which have been electronically filed with the IRS and is updated regularly in an XML format. The data can be used to perform research and analysis of organizations that have electronically filed Forms 990, 990-EZ and 990-PF. Forms 990-N (e-Postcard) are not available withing this data set. Forms 990-N can be viewed and downloaded from the IRS website.


from datetime import timedelta
import time
from datetime import datetime
from pprint import pprint
import csv 
import sys
import AggregatedReading as rR

import os.path
import ConfigParser

def mainWork():

	# read the config file - so that we don't hardcode in the names of the attributes
	# the name of the config file is "setup.cfg"
	config = ConfigParser.RawConfigParser()
	config.read('setup.cfg')
	
	__INPUT_FILE_WITH_DATA__ = ""

	try:
		# get the name of the input file from the config file.  
		__INPUT_FILE_WITH_DATA__ = config.get('IPRXML', 'DATA_FILE_INPUT')
	except ConfigParser.NoOptionError:
		print ("ERROR: You have no specified a file for import of the data for the IPR XML ")
		sys.exit(1)
		
	# before we proceed we must check then if the INPUT FILE is actually valid. It could well be 
	# marked in the config file but not actually exist. 
	# The most pythonic way to do this is to use the os.path
	
	if not os.path.isfile(__INPUT_FILE_WITH_DATA__):
		print ("ERROR: The file that you specified in the config file [" + __INPUT_FILE_WITH_DATA__ + "] does not exist and therefore cannot be processed ")
		sys.exit(1)
		
	# at this point we should have got past all of the checks for the input data file. 

	# create a python dictionary of all pollutants at all of the stations. 
	dictionaryStationPollGas = {}

	## iterate over all of the rows from the database
	# this will involve the creation of a dictionary so that we 
	# can easily separate out stations and their pollutants. 
	# so the dictionary will have the KEY
	# station$pollutant 
	# and it will have a list of items attached ... mean value and then dateTime
	
	dateRangeCounter = 0
	
	# read in the CSV file. 
	# remember the input csv file is specified in the __INPUT_FILE_WITH_DATA__ variable
	# from the config file above. 
	
	rows = csv.reader(open(__INPUT_FILE_WITH_DATA__, 'r'), delimiter=',', quotechar='"')
	
	datesInInputFile = [] 
	# an overly cautious approach to finding the maximum and minimum dates in the 
	# input data file - in case it is not sorted itself. 
	
	# process each line of the CSV file individually. 
	#
	# we currently have a very simple structure to this file - which can 
	# grow more complex as we expand this script. 
	#Bray,32.225000,O3,21:00 02 04 2012
	for row in rows:
		siteName = row[0]
		meanValue = row[1]
		
		if (meanValue == 'None'):
			meanValue = -999.00
			
		measureCode = row[2]
		dateTime = row[3]
		date_object = datetime.strptime(dateTime, '%H:00 %d %m %Y')
	
		# put this date into the array of all of the dates encountered in the input data. 
		# this will help us figure the date range - without requiring people to specify this 
		# in a configuration file or parameter. 
		
		datesInInputFile.append(date_object)
		
		# let us make a key for the dictionary out of STATION$Pollutant
		
		dictKey = siteName + "$" + measureCode
		
		reading = rR.AggregatedReading()
		# create a reading object with these variable names
		reading.createReading(siteName,meanValue,measureCode,date_object)
		
		# maintain the dictionary - using this dictionary key
		# create a new list for each unique dictionary key. 
		
		if not (dictKey in dictionaryStationPollGas):
			dictionaryStationPollGas[dictKey] = [] # create a new list for this dictionary entry
			# for example Bray$O3 - so Ozone (O3) and Bray station. 
				
		# add this new key to the dictionary
		dictionaryStationPollGas[dictKey].append(reading)

	# dictionary entries have been created for all possible keys. 

	# sort the array of dates that we have collected
	datesInInputFile.sort()
	
	minimumDate = datesInInputFile[0] # the first element is the oldest (minimum date)
	maximumDate = datesInInputFile[-1] # the last element in the array is the most recent (maximum date)


	gmlEndTimeString = maximumDate.strftime("%Y-%m-%dT%H:%M:00")
	gmlStartTimeString = minimumDate.strftime("%Y-%m-%dT%H:%M:00")
	
	dateTimesToProcess = generateDateRange(minimumDate,maximumDate)
	

	Station_Sequence_Number = 1

	print (getTopXML("./support/topOfXML.txt"))

	for k,v in sorted(dictionaryStationPollGas.items()):
		#print (k,len(v))
		print ("<!-- Processing " + k + " with " + str(len(v)) + " readings -->")
		
		readingsList = v
		#pprint(readingsList)
		gapFill(k,readingsList,dateTimesToProcess,Station_Sequence_Number)
		
		# advance the sequence number for the observation gml_id. 
		Station_Sequence_Number = Station_Sequence_Number + 1
		
	print ("</gml:FeatureCollection>")


################################################################		
		
def gapFill(k,stationReadingList,dateRangeList,sequenceNum):
	# date range list should be the same length as the stationReadingList
	
	stationName = ""
	measureCode = ""

	
	keySplit = k.split("$")
	# remember the dictionary key is split by the $ symbol. 
	
	stationName = keySplit[0]
	measureCode = keySplit[1]
	print ("<!-- Processing " + stationName + " for " + measureCode + "-->")
	
	if (measureCode == "NOX"):
		measureCode = "NOXasNO2"
	
	_pollutant_or_gas_ = "#" + measureCode
	_pollutant_or_gas_codelist = measureCode
	
	gapFillArray = []
	
	## fill an array with NULL values and the proper dates
	## and the proper measure codes
	## so this will then be used as a place to put the proper data
	for i in dateRangeList:
		temp = rR.AggregatedReading()
		temp.createReading(stationName,-999.00,measureCode,i)
		gapFillArray.append(temp)
		
	# enumerate over this gapFillArray
	# match the dates - if there is no date found in the StationReadingList
	# then it is missing for that date time. 
	# then the gapFillArray does not have a new value assigned to it. 
	# it uses the null values it has been assigned previously
	
	readingsNotRequiringGapFilling = 0
	
	for index,item in enumerate(gapFillArray):
		
		counter = 0
		found = 0
		while ((found == 0) and (counter < len(stationReadingList))):
			 
			 #print (gapFillArray[j].getDateTimeReading(),stationReadingList[counter].getDateTimeReading())
			 if (gapFillArray[index].getDateTimeReading() == stationReadingList[counter].getDateTimeReading()):
				 # WE HAVE THIS READING
				 gapFillArray[index] = stationReadingList[counter]
				 readingsNotRequiringGapFilling = readingsNotRequiringGapFilling + 1
				 
				 found = 1
			 counter = counter + 1
		
	print ("<!-- There were " + str(readingsNotRequiringGapFilling) + " values that did not need gap filling -->")
	print ("<!-- There were " + str(len(gapFillArray) - readingsNotRequiringGapFilling) + " gap filled values -->")
	#pprint(gapFillArray)
	
	_element_count_ = len(gapFillArray)
	
	_time_start_ = dateRangeList[0].strftime("%Y-%m-%dT%H:%M:00")
	_time_end_ = dateRangeList[-1].strftime("%Y-%m-%dT%H:%M:00")
	
	_inlet_ = namedValueSampleInputPointFile("./support/namedvalue-samplepoint-inlet.csv",_pollutant_or_gas_codelist)
	EOIStationSequence = searchObservationGML_ID_File("./support/observation-gml-id.csv",stationName) + "_seq" + str(sequenceNum)
	
	print ("<gml:featureMember>")
	print ("\t<om:OM_Observation gml:id=\"Observation_" + EOIStationSequence + "\">")
	print ("\t\t<om:phenomenonTime>")
	print ("\t\t<gml:TimePeriod gml:id=\"ObservationTimePeriodLOCAL_ID_" + str(sequenceNum) + "\">")
	print ("\t\t\t<gml:beginPosition>" + _time_start_ + "</gml:beginPosition>")
	print ("\t\t\t<gml:endPosition>" + _time_end_ + "</gml:endPosition>")
	print ("\t\t</gml:TimePeriod>")
	print ("\t\t</om:phenomenonTime>")
	
	print ("<om:resultTime>")
	print ("<gml:TimeInstant gml:id=\"ObservationResultInstance_seq" + str(sequenceNum) + "\">")
	print ("\t<gml:timePosition>" + _time_end_ + "</gml:timePosition>")
	print ("</gml:TimeInstant>")
	print ("</om:resultTime>")
	
	# now for the process or the measuring method. . . 
	process_str = omprocedurexlinkFile("./support/om-procedure-xlink.csv",_pollutant_or_gas_codelist)
	
	print ("\t<om:procedure xlink:href= \"" + process_str + "\"/>")
	
	
	print ("\t<om:parameter>")
	print ("\t\t<om:NamedValue>")
	print ("\t\t\t\t<om:name xlink:href=\"samplingPoint\"/>")
	print ("\t\t\t\t<om:value>SamplingPoint_" + EOIStationSequence +"_" + _inlet_ + "</om:value>")
	print ("\t\t</om:NamedValue>")	
	print ("\t</om:parameter>")
	print ("<om:observedProperty xlink:href=\"" + _pollutant_or_gas_ + "\"/>")
	
	print ("<om:featureOfInterest xlink:href=\"SampleFeature_"  + EOIStationSequence +"_" +  _inlet_ + "\"/>")
	
	
	## start printing out the actual data values...
	## xsi:type="swe:DataArrayType"
	print ("<om:result xsi:type=\"swe:DataArrayType\">")
	
	# give the number of elements...
	
	print ("\t<swe:elementCount>")
	print ("\t\t<swe:Count>")
	print ("\t\t\t\t<swe:value>" + str(_element_count_) + "</swe:value>")	
	print ("\t\t</swe:Count>")
	print ("\t</swe:elementCount>")
	
	## now begin to write out the DataRecord - which will have the components or the column headers. 
	
	print ("\t<swe:elementType name = \"Components\">")
	
	print ("\t\t<swe:DataRecord>")

	print (tab(5) + "<swe:field name=\"time\">")
	print (tab(6) + "<swe:Time definition=\"urn:ogc:property:time:iso8601\">")
	print (tab(7) + "<swe:uom code=\"hour\"/>")
	print (tab(6) + "</swe:Time>")
	print (tab(5) + "</swe:field>")
	print (tab(5) + "<swe:field name=\"Validity\">")
	print (tab(6) + "<swe:Quantity definition=\"Validity\">")
	print (tab(7) + "<swe:uom code=\"Codelist\"/>")
	print (tab(6) + "</swe:Quantity>")
	print (tab(5) + "</swe:field>")
	
	print (tab(5) + "<swe:field name=\"Verification\">")
	print (tab(6) + "<swe:Quantity definition=\"Verification\">")
	print (tab(7) + "<swe:uom code=\"Codelist\"/>")
	print (tab(6) + "</swe:Quantity>")
	print (tab(5) + "</swe:field>")
	
	print (tab(5) + "<swe:field name=\"" + _pollutant_or_gas_codelist + "\">")
	print (tab(6) + "<swe:Quantity definition=\"" + _pollutant_or_gas_codelist + "\">")
	print (tab(7) + "<swe:uom code=\"microg/m3\"/>")
	print (tab(6) + "</swe:Quantity>")
	print (tab(5) + "</swe:field>")
	print ("\t\t</swe:DataRecord>")	
	print (tab(1) + "</swe:elementType>")	
	#Validity
	"""
	1 = valid, 2 = valid but replaced by 0.5*detection limit
	3 not valid due to an issue, 4 not valid, missing
	"""
	
	#Verification
	"""
	1 = verified, 2 = preliminary verification, 3 = no verification
	"""
	print (tab(2) + "<swe:values>")

	# we need to iterate over the gapFillArray ... and print out over each element
	
	sweValueStr = "" # this will be printed out in the <swe:values> tag
	
	for sweValue in gapFillArray:
		sweValueReading = sweValue
		# this is a rR.AggregatedReading
		
		# need to time format... .strftime("%Y-%m-%dT%H:%M:00")
		
		sweValueStr = sweValueStr + sweValueReading.getDateTimeReading().strftime("%Y-%m-%dT%H:%M:00")
		
		
		if sweValueReading.getAggregatedReading() == None:
			sweValueStr = sweValueStr + ",X,X," + str(-999.00) + "@@"
			# not valid because it is missing... then it is verified
			print ("\n\n NONE VALUE DETECTED \n\n")		
		
		if sweValueReading.getAggregatedReading() == -999.00:
			sweValueStr = sweValueStr + ",4,1," + str(round(float(sweValueReading.getAggregatedReading()),4)) + "@@"
			# not valid because it is missing... then it is verified
		if sweValueReading.getAggregatedReading() > -999.00:
			sweValueStr = sweValueStr + ",1,1,"   + str(round(float(sweValueReading.getAggregatedReading()),4)) + "@@"  
			#then it is verified
		
		
	print (tab(3) + sweValueStr)
	print (tab(2) + "</swe:values>")
	
	
	#updated with specifications of the encoding used for the results presented as a DataArray 
	print (tab(2) + "<swe:encoding>") 
	print (tab(3) + "<swe:TextEncoding blockSeparator=\"@@\" decimalSeparator=\".\" tokenSeparator=\",\"/>") 
	print (tab(2) + "</swe:encoding>") 
		
	print ("</om:result>")
	# finished with the printing out of the actual data values. 
	print ("\t</om:OM_Observation>")
	print ("</gml:featureMember>")
	
#
#########################################################
#	
# this method simply creates an hourly date range between 
# the oldest value (s) and the most recent value (f)
# a list is created and then populated with these values. 

def generateDateRange(s,f):
	dateRange = []
	
	d = s
	delta = timedelta(hours=1)
	# <= used so we go inclusive from start to finish date... 
	while d <= f:
		#dateRange.append(d.strftime("%Y-%m-%d %H:%M:00"))
		dateRange.append(d)

		d += delta

	return dateRange;


"""
repeat the tab character n times. 
"""
def tab(n):
	return "\t"*n




"""
search the file to look up the EOI codes for a given station. 
"""
def searchObservationGML_ID_File(filename,station):
	
	EOICode = "NOT-FOUND"
	
	for line in open(filename):
		if station in line:
			lineList = line.split(",")
			EOICode = lineList[1].rstrip('\r\n') # second element of the list is the EOI Code
			break
	return EOICode

def omprocedurexlinkFile(filename,pollOrGas):
	
	omprocedure = "Process_XYZ?"
	
	for line in open(filename):
		if pollOrGas in line:
			lineList = line.split(",")
			omprocedure = lineList[1].rstrip('\r\n') # second element of the list is the PROCESS
			break
	return omprocedure


def namedValueSampleInputPointFile(filename,pollOrGas):
	
	theInlet = "Inlet_XYZ?"
	
	for line in open(filename):
		if pollOrGas in line:
			lineList = line.split(",")
			theInlet = lineList[1].rstrip('\r\n')  # second element of the list is theInlet
			
			
			
			break
	return theInlet

def getTopXML(filename):
	
	XML = ""
	
	for line in open(filename):
		XML = XML + str(line)
	return XML

	
####
####
####
#### call the main function to run the code..

mainWork()

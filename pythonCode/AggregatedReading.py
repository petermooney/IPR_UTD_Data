class AggregatedReading(object):

	def __init__(self):
		site = "Unknown"
		aggregatedReading = -999.00
		code = "UnknownCode"
		dateTimeReading = None
		
		
	def createReading(self,s,agg,co,dtR):
		self.site = s
		self.aggregatedReading = agg
		self.code = co
		self.dateTimeReading = dtR
		
	def getSite(self):
		return self.site
	
	def getAggregatedReading(self):
		return self.aggregatedReading
		
	def getCode(self):
		return self.code
	
	def getDateTimeReading(self):
		return self.dateTimeReading
		

	def __repr__(self):
		
		textToWrite = self.site + "," + self.code + "," + str(self.aggregatedReading) + "," + str(self.dateTimeReading.strftime("%Y-%m-%d %H:%M:00"))
		return textToWrite	

	def __str__(self):
		reading = "%.4f" % self.aggregatedReading		
		textToWrite = self.site + "," + self.code + "," + reading + "," + str(self.dateTimeReading)
		return textToWrite	

'''
Author: Ivan Nguyen
Last Updated: 08-09-2023
Description: Remove Infiniti
Version: 1.4
'''
import java
import time,calendar,datetime
from time				            		import mktime
from hec.dssgui		                		import ListSelection
from time				            		import mktime
import inspect
import DBAPI
import os
import urllib
from hec.heclib.util	            		import HecTime
from hec.hecmath		            		import TimeSeriesMath
from hec.io			                		import TimeSeriesContainer
from rma.services		            		import ServiceLookup
from java.util			            		import TimeZone
from hec.data.tx                    		import QualityTx
from java.text 								import SimpleDateFormat
from java.util 								import Date

try:
	StartTw  = datetime.datetime.now()
	print '='
	print '='
	print '='
	print '=================================================================================='
	print '======================================== START REMOVE INFINITI  LOG RUN AT ' + str(StartTw)
	print '================================================================================== '
	print '='
	print '='
	print '='
	

	# hard code start and end time window
	mysdate = '01Jan2015 0000'
	myedate = '31Dec2016 2400'

	print 'mysdate = ' + mysdate
	print 'myedate = ' + myedate

	# connect to db
	CwmsDb = DBAPI.open()
	if not CwmsDb : raise Exception
	CwmsDb.setTimeWindow(mysdate,myedate)
	CwmsDb.setOfficeId('MVS')
	CwmsDb.setTimeZone('GMT')
	CwmsDb.setStoreRule('Replace All')

	# set tsc with start and end time
	Tsc = CwmsDb.get('Sullivan-Meramec.Stage.Inst.15Minutes.0.lrgsShef-rev', mysdate, myedate)


	counter = 0
	maxval = len(Tsc.values)

	# loop through data set and print out the values
	while counter < maxval:
	
		if Tsc.values[counter] > 1000000:

			print 'counter = ' + str(counter)

			# set override protection data True
			CwmsDb.setOverrideProtection(True)

			# Remove protection flag in data
			Tsc.quality[counter] = QualityTx.clearProtected_int(Tsc.quality[counter])
			
			print 'value BEFORE = ' + str(Tsc.values[counter])
			print 'quality BEFORE = ' + str(Tsc.quality[counter])

			
			Tsc.values[counter] =  -3.40282346639e+38
			Tsc.quality[counter] = 5

			
			print 'value AFTER = ' + str(Tsc.values[counter])
			print 'quality AFTER = ' + str(Tsc.quality[counter])

			print '==================================================='
			
			counter += 1
			
		print '=== LOOP DONE ==='	
		#print 'Before DBPUT '
	CwmsDb.put(Tsc)
	#print 'After DBPUT '


	print '='
	print '='
	print '='
	print '=================================================================================='
	print '======================================== END REMOVE INFINITI  LOG RUN AT ' + str(StartTw)
	print '================================================================================== '
	print '='
	print '='
	print '='
	
finally :
	# close the connection in the finally block
	if CwmsDb:
		CwmsDb.close()
		print("Oracle database connection closed")

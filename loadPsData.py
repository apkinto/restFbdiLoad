import sys, os
import json
import urllib.parse
from oraRESTTools import *
import datetime
import time


def postData ( objectList, url ):
	for object in objectList:
		output, t, status, statusText = postRest( url, session, object, requestHeader, authorization, log )
		log.info('\t\t\tStatusCode: %s\t TotalTime: \t%s sec \t%s' % ( status, t, t ) )
		
def createPlans ( psPlanUrl, restCount ):
	plans = getExcelData( excelFile, 'Plans' )
	
	''' Get Existing Plan Information '''
	psPlanOutput, t, status, statusText, restCount = getRest( psPlanUrl, session, payload, None, requestHeader, authorization, recordLimit, log, restCount)
	psPlanIdList, psPlanXref = getPsPlanId( psPlanOutput, log )	
	print (psPlanIdList)
		
	'''	For each plan that does not already exist, insert OrgId	'''
	for plan in plans:
		if plan['PlanName'] in ( [dict['PlanName'] for dict in psPlanIdList] ):
			log.info('\t\t**Plan %s already exists, skipping' % ( plan['PlanName'] ) )
		else:
			#plan.pop('PlanId')
			plan['OrganizationId'] = orgXref[ plan['OrganizationCode'] ]
			log.info('\tCreating Plan %s' % ( plan['PlanName'] ) )
			#print (type(plan), plan)
			output, t, status, statusText, restCount = postRest( psPlanUrl, session, plan, requestHeader, authorization, log, restCount )
			
def segmentXreference ( orgs, segCodes, restCount ):
	segmentCodeIdXref ={}	
	for org in orgs:
		log.info('\t\t-->\tAttributeCode AttributeID Cross reference for %s ' % (segCodes))
		attrUrl = getUrl( psOrgUrl, str(orgXref[org]), 'child/attributes' )
		psOrgAttr, t, status, statusText, restCount = getRest( attrUrl, session, payload, None, requestHeader, authorization, recordLimit, log, restCount)
		for orgAttr in psOrgAttr['items']:
			attrKeys = orgAttr['@context']['key']
			segmentCodeIdXref[orgAttr['SegmentCode']] = [orgAttr['AttributeId'], attrKeys, orgAttr['AttributeCode']]
	
	return segmentCodeIdXref
	
def attributeValXreference ( segCodes, segXref, restCount ):
	attributeValueXref = {}
	for segment in segCodes:	
		log.info('\t\t-->AttributeValueCode AttributeValueID Cross reference for %s' % (str(segment)))
		attrValUrl = getUrl( psOrgUrl, str(orgXref[segment[0]]), 'child/attributes', segXref[segment[1]][1], 'child/attributeValues' )
		psOrgAttrVal, t, status, statusText, restCount = getRest( attrValUrl, session, payload, None, requestHeader, authorization, recordLimit, log, restCount)
		for attrVal in psOrgAttrVal['items']:
			#attrValKeys = getKey( attrVal['links'] )
			attrValKeys = attrVal['@context']['key']
			attributeValueXref[ (segment[0], segment[1], attrVal['AttributeValueCode'] ) ] = [ attrVal['AttributeValueId'], attrValKeys ] 
	
	return attributeValueXref
	
def updateAttributeBatch( restCount ):
	log.info('\tUpdating AttributeValue Colors')
	start = getTime()
	attributeValues = getExcelData( excelFile, 'AttributeValue' )
	segmentCodes = set( (dict['OrganizationCode'], dict['SegmentCode']) for dict in attributeValues )
	uniqueOrgs = set( dict['OrganizationCode'] for dict in attributeValues )
	
	segmentXref = segmentXreference( uniqueOrgs, segmentCodes, restCount )
	attrValXref = attributeValXreference( segmentCodes, segmentXref, restCount )  
	
	partsList = []
	for av in attributeValues:
		log.info('\t\t-->Getting Attribute Colors for %s: %s' % (av['SegmentCode'], av['AttributeValueCode']))
		attrColor={}
		attrColor['OrganizationId'] = orgXref[ av['OrganizationCode'] ]
		attrColor['AttributeId'] = segmentXref[ av['SegmentCode'] ][0]
		attrColor['AttributeValueId'] = attrValXref [ (av['OrganizationCode'], av['SegmentCode'], av['AttributeValueCode']) ][0]
		attrColor['Color'] = av['Color']
		attrValKey = attrValXref [ (av['OrganizationCode'], av['SegmentCode'], av['AttributeValueCode']) ][1]
		postAttrValueUrl = getUrl( '','/productionSchedulingOrganizations', str(orgXref[ av['OrganizationCode']]), 'child/attributes', segmentXref[av['SegmentCode']][1], 'child/attributeValues', attrValKey)
		parts = getParts(str(attrValXref[(av['OrganizationCode'], av['SegmentCode'], av['AttributeValueCode'])][0]), getUrl( '','/productionSchedulingOrganizations', str(orgXref[ av['OrganizationCode']]), 'child/attributes', segmentXref[av['SegmentCode']][1], 'child/attributeValues', attrValKey), 'update', attrColor)
		partsList.append(parts)
		

	log.info('\t\tUpdating %s Attribute Color Records in batches of %s' % (len(partsList), batchChunks))
	t, status, statusText, restCount = postBatchRest(url, session, partsList, int(batchChunks), authorization, log, restCount)
	#print('===', url, statusText)
	TotalTime = getTime() - start
	log.info('\t\tUpdated Attribute Colors %s REST calls in %s\tsec' % (restCount, TotalTime))

def createChangeoversBatch( restCount ):
	log.info('\tCreating Changeovers')
	start = getTime()
	changeOvers = getExcelData( excelFile, 'Changeovers' )
	workCenters = set( dict['WorkCenterCode'] for dict in changeOvers)	
	wcXref, restCount = getWc(url, workCenters, restCount)
	segmentCodes = set( (dict['OrganizationCode'], dict['SegmentCode']) for dict in changeOvers )
	uniqueOrgs = set( dict['OrganizationCode'] for dict in changeOvers )	
	segmentXref = segmentXreference( uniqueOrgs, segmentCodes, restCount )
	attrValXref = attributeValXreference( segmentCodes, segmentXref, restCount ) 
	#changeoverSeq=int(time.mktime(datetime.datetime.now().timetuple())*100000)
	
	partsList = []
	for co in changeOvers:
		if wcXref.get((orgXref[co['OrganizationCode']], co['WorkCenterCode'], co['ResourceCode'])):
			resourceKey=(orgXref[co['OrganizationCode']], co['WorkCenterCode'], co['ResourceCode'])
			
			parts = {}
			coPayload ={}
			coPayload['OrganizationId'] = orgXref[ co['OrganizationCode' ] ]
			#coPayload['ChangeoverId'] = changeoverId
			coPayload['ChangeoverSequenceNumber'] = co['ChangeoverSequenceNumber']
			coPayload['WorkCenterId'] = wcXref[orgXref[co['OrganizationCode']], co['WorkCenterCode'], co['ResourceCode']][1]
			coPayload['WorkCenterCode'] = co['WorkCenterCode']
			coPayload['ResourceId'] = wcXref[orgXref[co['OrganizationCode']], co['WorkCenterCode'], co['ResourceCode']][2]
			coPayload['ResourceCode'] = co['ResourceCode']
			coPayload['AttributeId'] = segmentXref[ co['SegmentCode'] ][0]
			coPayload['AttributeCode'] = segmentXref[ co['SegmentCode'] ][2]
			coPayload['FromAttributeValueId'] = attrValXref[ (co['OrganizationCode' ], co['SegmentCode'], co['FromAttributeValueCode']) ][0]
			coPayload['FromAttributeValueCode'] = co['FromAttributeValueCode']
			coPayload['ToAttributeValueId'] = attrValXref[ (co['OrganizationCode' ], co['SegmentCode'],co['ToAttributeValueCode']) ][0]
			coPayload['ToAttributeValueCode'] = co['ToAttributeValueCode']
			coPayload['Duration'] = co['Duration']
			coPayload['DurationUnit'] = co['DurationUnit']
			coPayload['Cost'] = co['Cost']
			parts = getParts(co['ChangeoverSequenceNumber'], getUrl('/productionSchedulingOrganizations', str(orgXref[co['OrganizationCode']]), 'child/changeoverRules'), 'create', coPayload)
			partsList.append(parts)

		else:
			log.info('\t\t!! Resource doesn\'t exist in WC for %s %s %s' % (g['OrganizationCode'],g['WorkCenterCode'], g['ResourceCode']) )
		
	log.info('\t\tCreating %s Changeover Records in batches of %s' % (len(partsList), batchChunks))
	t, status, statusText, restCount = postBatchRest( url, session, partsList, int(batchChunks), authorization, log, restCount )
	TotalTime = getTime() - start
	#print (statusText)
	log.info('\t\tChangeovers:: %s REST calls in %s\tsec' % (restCount, TotalTime))

	
def updateResourceGroups( psOrgUrl, restCount ):
	log.info('\tUpdating ResourceGroups')
	start = getTime()
	resourceGroups = getExcelData( excelFile, 'ResourceGroups' )
	uniqueOrgs = set( dict['OrganizationCode'] for dict in resourceGroups )
	#print('***', orgXref['M1'])

	log.info('\t\t-->Getting Resource Groups' )
	for rg in resourceGroups:
		rgPayload = {}
		rgPayload['OrganizationId'] = orgXref[rg['OrganizationCode']]
		rgPayload['GroupCode'] = rg['GroupCode' ]
		rgPayload['Description'] = rg['Description']
		rgUrl = getUrl(psOrgUrl, str(orgXref[rg['OrganizationCode']]), 'child/resourceGroups')
		
		output,t, status, statusText, restCount = postRest(rgUrl, session, rgPayload, requestHeader, authorization, log, restCount)
		
		log.info('\t\t%s Creating Resource Group %s %s' % (status,rg['OrganizationCode'], rg['GroupCode']))
	
	TotalTime = getTime() - start
	log.info('\tCreate ResourceGroups %s REST calls in %s\tsec' % (restCount, TotalTime))
	
	return groupXreference(uniqueOrgs, restCount)
	
def groupXreference ( orgs, restCount ):
	groupIdXref ={}   #{ResourceGroupCode: [orgId, groupId]}	
	for org in orgs:
		log.info('\t\t-->Generate ResourceGroup Cross reference for %s ' % (org))
		rgUrl = getUrl( psOrgUrl, str(orgXref[org]), 'child/resourceGroups' )
		psResGroups, t, status, statusText, restCount = getRest( rgUrl, session, payload, None, requestHeader, authorization, recordLimit, log, restCount)

		for resGroup in psResGroups['items']:
			groupIdXref[resGroup['GroupCode']] = [resGroup['OrganizationId'], resGroup['GroupId']]

	return groupIdXref

def updateGroupMembers(psOrgUrl, groupIdXref, restCount ):
	log.info('\tUpdating ResourceGroup Members...')
	start = getTime()
	groupMembers = getExcelData( excelFile, 'ResourceGroupMembers' )
	uniqueOrgs = set(dict['OrganizationCode'] for dict in groupMembers)
	uniqueWc = set(dict['WorkCenterCode'] for dict in groupMembers)
	
	wcXref, restCount = getWc(url, uniqueWc, restCount)
	log.info('\t\t-->Adding Resources to Resource Groups' )	

	for g in groupMembers:
		if wcXref.get((orgXref[g['OrganizationCode']], g['WorkCenterCode'], g['ResourceCode'])):
			resourceKey=(orgXref[g['OrganizationCode']], g['WorkCenterCode'], g['ResourceCode'])

			gPayload = {}
			gPayload['OrganizationId'] = resourceGroups[g['GroupCode']][0]
			gPayload['GroupId'] = resourceGroups[g['GroupCode']][1]
			gPayload['WorkCenterCode'] = g['WorkCenterCode']
			gPayload['WorkCenterId'] = wcXref[orgXref[g['OrganizationCode']], g['WorkCenterCode'], g['ResourceCode']][1]
			gPayload['ResourceId'] = wcXref[orgXref[g['OrganizationCode']], g['WorkCenterCode'], g['ResourceCode']][2]
			gPayload['ResourceCode'] = g['ResourceCode']
			gPayload['MemberSequenceNumber'] = int(g['MemberSequenceNumber'])

			gUrl = getUrl(psOrgUrl, str(orgXref[g['OrganizationCode']]), 'child/resourceGroups',str(resourceGroups[g['GroupCode']][1]), 'child/groupMembers')
			output,t, status, statusText, restCount = postRest(gUrl, session, gPayload, requestHeader, authorization, log, restCount)
			log.info('\t\t%s Adding Resource Group Members %s %s %s' % (status,g['OrganizationCode'], g['GroupCode'], g['ResourceCode']))
			#print(gPayload, statusText)
		else:
			log.info('\t\t!! Resource doesn\'t exist in WC for %s %s %s' % (g['OrganizationCode'],g['WorkCenterCode'], g['ResourceCode']) )
	
	TotalTime = getTime() - start
	log.info('\tAdded ResourceGroups Members %s REST calls in %s\tsec' % (restCount, TotalTime))

	
def dxt ( restCount ):
	plans = getExcelData( excelFile, 'Plans' )
	dxtUrl ='https://fuscdrmsmc141-fa-ext.us.oracle.com/fscmRestApi/resources/11.13.18.05/productionSchedulingPlans/300100185100807/enclosure/EngineStateFile'
	''' Get Existing Plan Information '''
	psPlanOutput, t, status, statusText, restCount = getRest( dxtUrl, session, payload, None, requestHeader, authorization, None, log, restCount)
		
	print (psPlanOutput)
	
def getWc(url, data, restCount):
	log.info('\t\tGetting Work Center Id\'s...')
	
	'''
	# Get a list of unique Work Centers
	wcList=[]
	for d in data:
		if (d['WorkCenterCode'] in wcList):
			pass
		else:
			wcList.append(d['WorkCenterCode'])
	'''
	
	wcUrl = getUrl( url, 'workCenters')
	wcResXref = {} # Key = (orgId, wcCode, resCode)

	for w in data:
		query='WorkCenterCode=\'' + w +'\'' 
		wcOutput, t, status, statusText, restCount = getRest( wcUrl, session, payload, query, requestHeader, authorization, recordLimit, log, restCount)
		#print('\n', 'Made it here',wcOutput)
		if wcOutput['items']:
			print('\n', '******YES')
			wcId = wcOutput['items'][0]['WorkCenterId']
			orgId = wcOutput['items'][0]['OrganizationId']
			wcResUrl = getUrl(wcUrl, str(wcId), 'child/WorkCenterResource')
			wcResOutput, t, status, statusText, restCount = getRest( wcResUrl, session, payload, None, requestHeader, authorization, recordLimit, log, restCount)
		
			for wcRes in wcResOutput['items']:
				wcResXref[orgId, w,wcRes['ResourceCode']] = orgId, wcRes['WorkCenterId'], wcRes['ResourceId']
		else:
			print('NO')

		
		
	log.info('\t\tRetrieved Work Center in %s seconds...' % (t))

	return wcResXref, restCount
	
def resourceParameters(url, orgs, restCount):

	log.info('\tLoading Resource Parameters...')
	start = getTime()
	resourceParameters = getExcelData(excelFile, 'ResourceParameters')
	uniqueWc = set(dict['WorkCenterCode'] for dict in resourceParameters)

	Id = 1

	wcXref, restCount = getWc(url, uniqueWc, restCount)
	
	# Create Resource Parameters Cross Reference for existing entries
	resParamXref={}
	for o in orgs:
		resParamUrl = getUrl(url, 'productionSchedulingOrganizations',str(o), 'child/resourceParameters')
		resParameters, t, status, statusText, restCount = getRest( resParamUrl, session, payload, None, requestHeader, authorization, recordLimit, log, restCount)
		
		for r in resParameters['items']:
			resParamXref[r['OrganizationId'],r['WorkCenterCode'], r['ResourceCode']]=[r['OrganizationId'],r['WorkCenterId'], r['ResourceId']]
	
	for wcR in resourceParameters:
		
		if wcXref.get((orgXref[wcR['OrganizationCode']], wcR['WorkCenterCode'], wcR['ResourceCode'])):
			resourceKey=(orgXref[wcR['OrganizationCode']], wcR['WorkCenterCode'], wcR['ResourceCode'])
			if resParamXref.get(resourceKey):
				log.info('\t\t!! Resource parameters already exist for %s' % str(resourceKey))
			else:
				resParameter = {}
				resParamUrl = getUrl(url,'productionSchedulingOrganizations', str(resourceKey[0]), 'child/resourceParameters')
				resParameter['OrganizationId']= str(wcXref[resourceKey][0])
				resParameter['WorkCenterId']= str(wcXref[resourceKey][1])
				resParameter['ResourceId']= str(wcXref[resourceKey][2])
				resParameter['WorkCenterCode']= wcR['WorkCenterCode']
				resParameter['ResourceCode']= wcR['ResourceCode']
				resParameter['ConstraintMode']=wcR['ConstraintMode']
				resParameter['EnforceHorizonStartFlag']=wcR['EnforceHorizonStartFlag']
				resParameter['ChangeoverCalculation']=wcR['ChangeoverCalculation']
				resParameter['ChangeoverPosition']=wcR['ChangeoverPosition']
				resParameter['ApplyIdealSequenceFlag']=wcR['ApplyIdealSequenceFlag']
				
				output, t, status, statusText, restCount = postRest(resParamUrl, session, resParameter, requestHeader, authorization, log, restCount)
				log.info('\t\t%s Updating Resource Parameter for %s' % (status, wcR['ResourceCode']))
				Id += 1
		
		else:
			log.info('\t\t!! Resource doesn\'t exist in WC for %s %s %s' % (wcR['OrganizationCode'],wcR['WorkCenterCode'], wcR['ResourceCode']) )
			continue
	
	TotalTime = getTime() - start
	log.info('\t%s %s Resource Parameters : %s REST calls in %s\tsec' % (status, Id, restCount, TotalTime))

if __name__ == "__main__":
	
	'''	Set Variables, logging, and establish Session 	'''
	log = setLogging()
	variables = setVariables('psr.xml')
	for key,val in variables.items():
		exec(key + '=val')	

	session, authorization, requestHeader, payload = scmAuth ( user, password )
	mfgsession, mfgauthorization, mfgrequestHeader, mfgpayload = scmAuth ( mfgUser, password )
	
	log.info('REST Server: %s' % ( url ))
	psOrgUrl = getUrl ( url, 'productionSchedulingOrganizations' ) 
	psPlanUrl = getUrl( url, 'productionSchedulingPlans')
	restCount = 0
	
	'''	get Schedule Organizations and create code/id xref	'''
	psOrganizations, t, status, statusText, restCount = getRest ( psOrgUrl, session, payload, None, requestHeader, authorization, recordLimit, log, restCount )
	orgXref = idCode (psOrganizations, 'OrganizationCode', 'OrganizationId', log)
	organizations = list(orgXref.values())

	#createPlans( psPlanUrl, restCount)
	updateAttributeBatch(restCount)
	createChangeoversBatch(restCount)
	resourceGroups = updateResourceGroups(psOrgUrl, restCount)
	updateGroupMembers(psOrgUrl, resourceGroups, restCount)
	resourceParameters(url, organizations, restCount)
	

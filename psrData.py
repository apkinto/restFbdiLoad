import sys, os
import json
import urllib.parse
from oraRESTTools import *
import datetime
import time
import base64

def getPlan(url, restCount):
	log.info('\tGetting Plans...')
	planOutput, t, status, statusText, restCount = getRest( url, session, payload, None, requestHeader, authorization, recordLimit, log, restCount)
	log.info('\tRetrieved Plans in %s seconds...' % (t))
	planIdList, planXref = getPsPlanId( planOutput, log )

	return planIdList, planXref, restCount

def pollEss(essJobId, interval, restCount, essSession, essAuth, essHeader):
	postUrl = getUrl( url, 'erpintegrations')

	essBody = {}
	essBody['OperationName'] = 'getESSJobStatus'
	essBody['ReqstId'] = essJobId
	essStatus=None

	requestHeader['Content-Type'] = 'application/json'
	while essStatus not in ["ERROR", "SUCCEEDED", "WARNING"]:
		essOutput, t, status, statusText, restCount = postRest( postUrl, essSession, essBody, essHeader, essAuth, log, restCount )
		essStatus = essOutput['RequestStatus']
		log.info('\t\t...ESS JobId: %s --> %s' % (essJobId, essStatus))
		time.sleep(interval)
	else:
		log.info('\tFinished with status %s' % (essStatus))

	return essStatus

def runSteps(steps, plansXref, statusField, statusCodes, scpAction, restCount, interval, log):
	for s in steps:
		start = getTime()
		log.info('\tProcessing Step: %s' % (s['Step']))

		# If the Excel worksheet contains this field, it is used to run SCP processes else PS processes which uses the Action field
		if s['Body']:
			body = json.loads(s['Body'])
		else:
			body = getPsBody(s['Action'], s['Parameters'])

		if s['Type'] == 'collections':
			postUrl = getUrl( url, 'dataCollections')
			essJobField = 'ESSCollectionJobId'
		else:
			postUrl = getUrl( url, s['Type'], str(plansXref[s['PlanName']]), scpAction)
			#essJobField = 'JobId'
			essJobField = 'result'

		requestHeader['Content-Type'] = 'application/vnd.oracle.adf.action+json'
		output, t, status, statusText, restCount = postRest( postUrl, session, body, requestHeader, authorization, log, restCount )
		essJobId = getEssJobId(output, essJobField)

		if status == 200 or status == 201:
			pollEss(essJobId, interval, restCount)
		else:
			log.info('\tERROR:\t\n%s' % (statusText))

def createCategories(restCount):
	log.info('\tCreating Categories')
	start = getTime()
	catalogs = getExcelData(excelFile, 'catalog')
	categories = getExcelData(excelFile, 'categories')
	Id = 1
	catalogUrl = getUrl(url, 'itemCatalogs')
	categoryUrl = getUrl(url, 'itemCategories')

	for w in catalogs:
		log.info('\t\t-->Getting Catalogs...')

		#catalog = {'CatalogCode': 'APK_CATALOG', 'CatalogName': 'APK_CATALOG', 'Description': 'APK_CATALOG'}
		catalog = {}
		catalog['CatalogCode'] = w['CatalogCode']
		catalog['CatalogName'] = w['CatalogCode']
		catalog['Description'] = w['CatalogCode']
		catalog['MasterControlledFlag'] = True
		catalog['StartDate'] = datetime.datetime.now(datetime.timezone.utc).strftime(("%Y-%m-%d"))
		catalog['EndDate'] = None
		catalog['AssignItemsToLeafOnlyFlag'] = True
		catalog['AllowItemToMultipleCategoriesFlag'] = True
		catalog['PublicCatalogFlag'] = True
		catalog['DefaultCategoryCode'] = None
		#print(catalog)

		output, t, status, statusText, restCount = postRest(catalogUrl, session, catalog, requestHeader, authorization, log, restCount)
		Id += 1
		#print (status, statusText)

	for w in categories:
		log.info('\t\t-->Getting Categories...')
		category = {}

		category['CategoryCode'] = w['CategoryCode']
		category['CategoryName'] = w['CategoryCode']
		category['CatalogCode'] = catalog['CatalogCode']
		category['Description'] = catalog['CatalogCode']
		category['StartDate'] = str(datetime.datetime.now(datetime.timezone.utc).strftime(("%Y-%m-%d")))
		category['ItemsOnlyFlag'] = False

		output, t, status, statusText, restCount = postRest(categoryUrl, session, category, requestHeader, authorization, log, restCount)
		Id += 1

	TotalTime = getTime() - start
	log.info('\t%s %s Categories : %s REST calls in %s\tsec' % (status, Id, restCount, TotalTime))
	if status != 201:
		log.info('\t\t-->%s' % (statusText))

def createWc(restCount):
	log.info('\tCreating WorkCenters')
	start = getTime()
	workCenter = getExcelData(excelFile, 'workCenter')
	Id = 1
	postWaUrl = getUrl(url, 'workAreas')
	postWcUrl = getUrl(url, 'workCenters')

	for w in workCenter:
		log.info('\t\t-->Getting WorkCenters...')
		workArea = {}
		workCenter = {}

		workArea['OrganizationCode'] = w['OrganizationCode']
		workArea['WorkAreaName'] = w['WorkAreaName']
		workArea['WorkAreaDescription'] = w['WorkAreaDescription']
		workArea['WorkAreaCode'] = w['WorkAreaCode']
		output, t, status, statusText, restCount = postRest(postWaUrl, session, workArea, requestHeader, authorization, log, restCount)
		Id += 1

		workCenter['OrganizationCode'] = w['OrganizationCode']
		workCenter['WorkCenterCode'] = w['WorkCenterCode']
		workCenter['WorkCenterName'] = w['WorkCenterName']
		workCenter['WorkCenterDescription'] = w['WorkCenterDescription']
		workCenter['WorkAreaName'] = w['WorkAreaName']
		output, t, status, statusText, restCount = postRest(postWcUrl, session, workCenter, requestHeader, authorization, log, restCount)
		Id += 1
		workCenterId = getEssJobId(output, 'WorkCenterId')
		#print (workCenterId)

	TotalTime = getTime() - start
	log.info('\t%s WorkCenters : %s REST calls in %s\tsec' % (Id, restCount, TotalTime))
	if status != 201:
		log.info('\t\t-->%s' % (statusText))

	return workCenterId

def createResources(restCount, batchChunks):
	log.info('\tCreating Resources')
	start = getTime()
	resources = getExcelData(excelFile, 'resources')
	Id = 1

	partsList = []
	log.info('\t\t-->Getting Resources...')
	for r in resources:
		resources = {}
		resources['OrganizationCode'] = r['OrganizationCode']
		resources['ResourceName'] = r['ResourceName']
		resources['ResourceDescription'] = r['ResourceDescription']
		resources['ResourceCode'] = r['ResourceCode']
		resources['ResourceType'] = r['ResourceType']
		resources['UOMCode'] = r['UOMCode']
		resources['CostedFlag'] = 'true'
		parts = getParts(Id, getUrl('', '/productionResources'), 'create', resources)
		partsList.append(parts)
		Id += 1

	chunks = [int(batchChunks)]

	for c in chunks:
		log.info('\t\tUpdating %s Resource Records in batches of %s' % (len(partsList), c))
		t, status, statusText, restCount = postBatchRest(url, session, partsList, c, authorization, log, restCount)
		TotalTime = getTime() - start
		log.info('\t\tCreated Resource %s REST calls in %s\tsec' % (restCount, TotalTime))

def createWcResourceSingle(wc, restCount, batchChunks):
	log.info('\tCreating WorkCenter Resources')
	start = getTime()
	resources = getExcelData(excelFile, 'resources')
	Id = 1

	postUrl = getUrl(url, 'workCenters', str(wc), 'child/WorkCenterResource')

	log.info('\t\t-->Getting WorkCenter Resources...')
	for r in resources:
		wcResources = {}
		wcResources['ResourceName'] = r['ResourceName']
		wcResources['ResourceQuantity'] = int(r['ResourceQuantity'])
		wcResources['Available24HoursFlag'] = 'false'
		wcResources['CheckCtpFlag'] = 'false'
		wcResources['UtilizationPercentage'] = 100
		wcResources['EfficiencyPercentage'] = 100
		output, t, status, statusText, restCount = postRest(postUrl, session, wcResources, requestHeader, authorization, log, restCount)
		log.info('\t\t\tResource:: %s %s' % (r['ResourceName'], status))
		Id += 1

	TotalTime = getTime() - start
	log.info('\t%s Work Center Resource : %s REST calls in %s\tsec' % (Id, restCount, TotalTime))

def uploadUcm(ucmurl, ucmFile, ucmFilename, ucmAccount, restCount):
	log.info('\t\t-->Uploading %s to UCM...' %(ucmFilename))

	ucmBody = {}
	ucmBody['OperationName'] = "uploadFileToUCM"
	ucmBody['DocumentContent'] = ucmFile
	ucmBody['DocumentAccount'] = ucmAccount
	ucmBody['ContentType'] = "zip"
	ucmBody['FileName'] = ucmFilename
	ucmBody['DocumentId'] = None
	#print ("***",ucmurl)

	output, t, status, statusText, restCount = postRest(ucmurl, session, ucmBody, requestHeader, authorization, log, restCount)
	docId = getEssJobId(output, 'DocumentId')
	log.info('\t\t\t--DocId: %s Status: %s...' %(docId, status))

	return docId

def submitEssJob(essUrl, jobPackName, jobDefName, essParam, inter, restCount, mySession, myAuth, myHeader):
	log.info('\t\t-->Launching ESS %s...' %(jobDefName))

	essBody = {}
	essBody['OperationName'] = "submitESSJobRequest"
	essBody['JobPackageName'] = jobPackName
	essBody['JobDefName'] = jobDefName
	essBody['ESSParameters'] = essParam
	#print(essBody)

	output, t, status, statusText, restCount = postRest(essUrl, mySession, essBody, myHeader, myAuth, log, restCount)
	log.info('\t\t--REST Status: %s...' % (status))
	essJobId = getEssJobId(output, 'ReqstId')

	pollEss(essJobId, inter, restCount, mySession, myAuth, myHeader)
	getLogs(essUrl, essJobId, jobDefName, inputDir, log, mySession, myAuth, myHeader)
	essDetails(erpUrl, essJobId, log, mySession, myAuth, myHeader)

	log.info('\t\t\t--essProcessID: %s Status: %s...' % (essJobId, status))

def createItems(batchId):
	#UCM
	itemFile = toBase64(inputDir, itemZip)
	itemFileName = itemZip
	itemAccount = "scm$/item$/import$"

	itemUcmDocId = uploadUcm(erpUrl, itemFile, itemFileName, itemAccount, restCount)
	time.sleep(3)

	#LoadInterface
	loadItemParams = ','.join(('29', itemUcmDocId, 'N', 'N'))
	submitEssJob(erpUrl, interfacePckName, interfaceJobDefName, loadItemParams, int(interval), restCount, pimSession, pimAuthorization, pimRequestHeader)

	#LoadTables
	itemJobPackName = "/oracle/apps/ess/scm/productModel/items/"
	itemJobDefName = "ItemImportJobDef"
	itemParameters = ','.join((batchId, '#NULL', 'CREATE', 'Y', 'ORA_AR', 'Y', 'Y'))
	submitEssJob(erpUrl, itemJobPackName, itemJobDefName, itemParameters, int(interval)*8, restCount, pimSession, pimAuthorization, pimRequestHeader)

def createStructure(batchId):
	#UCM
	structFile = toBase64(inputDir, structureZip)
	structFileName = structureZip
	structAccount = "scm$/item$/import$"

	structUcmDocId = uploadUcm(erpUrl, structFile, structFileName, structAccount, restCount)
	time.sleep(3)

	#LoadInterface
	loadstructParams = ','.join(('29', structUcmDocId, 'N', 'N'))
	submitEssJob(erpUrl, interfacePckName, interfaceJobDefName, loadstructParams, int(interval), restCount, pimSession, pimAuthorization, pimRequestHeader)

	#LoadTables
	structJobPackName = "/oracle/apps/ess/scm/productModel/items/"
	structJobDefName = "ItemImportJobDef"
	structParameters = ','.join((batchId, '#NULL', 'CREATE', 'Y', 'ORA_AR', 'Y', 'Y'))
	submitEssJob(erpUrl, structJobPackName, structJobDefName, structParameters, int(interval)*2, restCount, pimSession, pimAuthorization, pimRequestHeader)

def createWd(batchId):
	# UCM
	wdFile = toBase64(inputDir, wdZip)
	wdFileName = wdZip
	wdAccount = "scm$/wis$/workdefinition$"

	wdUcmDocId = uploadUcm(erpUrl, wdFile, wdFileName, wdAccount, restCount)
	time.sleep(3)

	# LoadInterface
	loadWdParams = ','.join(('133', wdUcmDocId, 'N', 'N'))
	submitEssJob(erpUrl, interfacePckName, interfaceJobDefName, loadWdParams, int(interval) * 2, restCount, session,
				 authorization, requestHeader)

	# LoadTables
	wdJobPackName = "/oracle/apps/ess/scm/commonWorkSetup/workDefinitions/massImport/"
	wdJobDefName = "ImportWorkDefinitionJob"
	wdParameters = batchId
	submitEssJob(erpUrl, wdJobPackName, wdJobDefName, wdParameters, int(interval) * 2, restCount, session, authorization, requestHeader)

def createWo(batchId):
	# UCM
	woFile = toBase64(inputDir, woZip)
	woFileName = woZip
	woAccount = "scm$/wis$/workorder$"

	woUcmDocId = uploadUcm(erpUrl, woFile, woFileName, woAccount, restCount)

	# LoadInterface
	loadWoParams = ','.join(('63', woUcmDocId, 'N', 'N'))
	submitEssJob(erpUrl, interfacePckName, interfaceJobDefName, loadWoParams, int(interval) * 2, restCount, session,
				 authorization, requestHeader)

	# LoadTables
	woJobPackName = "/oracle/apps/ess/scm/commonWorkExecution/massImport/workOrders/"
	woJobDefName = "ImportWorkOrdersJob"
	woParameters = batchId
	submitEssJob(erpUrl, woJobPackName, woJobDefName, woParameters, int(interval) * 2, restCount, session, authorization, requestHeader)


if __name__ == "__main__":
	'''	Set Variables from XML, logging, and establish Session 	'''
	log = setLogging()
	variables = setVariables('psr.xml')
	for key,val in variables.items():
		exec(key + '=val')

	''' Variables	'''
	interfacePckName = "/oracle/apps/ess/financials/commonModules/shared/common/interfaceLoader/"
	interfaceJobDefName = "InterfaceLoaderController"
	session, authorization, requestHeader, payload = scmAuth (user, password)
	pimSession, pimAuthorization, pimRequestHeader, pimPayload = scmAuth (pimUser, password)
	erpUrl = getUrl(url, 'erpintegrations')
	restCount = 0

	log.info('REST Server: %s' % ( url ))

	''' Categories START'''
	createCategories(restCount)

	''' WC and Resources START'''
	wcId = createWc(restCount)
	createResources(restCount, batchChunks)
	createWcResourceSingle(wcId, restCount, batchChunks)

	createItems((int(itemBatch))
	createStructure(int(structureBatch))
	createWd(int(wdBatch))
	createWo(int(woBatch))
	#createWo('4411')

	#essDetails(erpUrl, 57127, log, session, authorization, requestHeader)

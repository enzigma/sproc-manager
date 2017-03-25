import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors
import sys, getopt
from config import Config

class IDisposable:

	def __init__( self, obj ):
		self.obj = obj

	def __enter__( self ):
		return self.obj

	def __exit__( self, exception_type, exception_val, trace ):
		self = None

class SProceManagement:
	@staticmethod
	def delete_storedProcedure( client, sproclink ):
		try:
			client.DeleteStoredProcedure( sproclink, None )
			print( "\nScript deletion successful" )
		except Exception as ex:
			print( "\nException: {0}".format( ex.args) )

	@staticmethod
	def create_storedProcedure( client, collection_link, sproc ):
		try: 
			client.CreateStoredProcedure( collection_link, sproc )
			print( "\nScript upload successful" )
		except Exception as ex:
			print( "\nException: {0}".format( ex.args) )

def deploySProc( config, scriptFile, sProcId ):
	with IDisposable( document_client.DocumentClient( config.settings["host"], { "masterKey" : config.settings["master_key"] } ) ) as client:
		try:
			database_link = "dbs/" + config.settings["database_id"]
			collection_link = database_link + "/colls/" + config.settings["collection_id"]
			sproc_link = database_link + "/colls/" + config.settings["collection_id"] + "/sprocs/" + sProcId;
			SProceManagement.delete_storedProcedure( client, sproc_link )
			SProceManagement.create_storedProcedure( client, collection_link, scriptFile )
		except Exception as ex:
			print( "\nException: {0}".format( ex.args) )

def main( argv ):
	try:
		opts, args = getopt.getopt( argv, "c:s:n:", [ "configFilePath", "scriptFilePath", "sProcName" ])
		configFilePath = None
		scriptFilePath = None
		sProcId = None
		for option, argument in opts:
			if option == "-c":
				configFilePath = argument
			elif option == "-s":
				scriptFilePath = argument
			elif option == "-n":
				sProcId = argument
		
		if configFilePath != None and scriptFilePath != None and sProcId:
			configFile = open( configFilePath )
			cfg = Config( configFile )
			scriptFile = { "id" : sProcId, "body" : "" + open( scriptFilePath ).read() + "" }
			deploySProc( cfg, scriptFile, sProcId );
		else:
			print( "\nArguments -c, -s or -n are missing" )
			print( "\nSpecify configuration file path followed by argument: -c" )
			print( "Specify script file path to upload followed by argument: -s" )
			print( "Specify script name (case-sensitive) followed by argument: -n" )
	except Exception as ex:
		print( "\nException: {0}".format( ex.args) )

if __name__ == "__main__":
	try:
		main( sys.argv[1:] )
	except Exception as ex:
		print( "\nException: {0}".format( ex.args) )
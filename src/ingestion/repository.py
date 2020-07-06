import os
import copy
import asyncio
import tempfile

from threading import Thread
from progressbar import ProgressBar

from server import Server
from product import Product

from src.utility import fs
from src.utility import parser
from src.utility.gsclient import GsClient


class Repository:


    def __init__( self, obj, threads=1 ):

        """
        constructor
        """

        self._obj = obj
        self._threads = threads

        # create list of product objects
        self._products = []
        for item in self._obj[ 'products' ]:
            self._products.append( Product ( item ) )

        # create list of servers
        self._servers = []        
        for item in self._obj[ 'servers' ]:
            self._servers.append( Server ( item ) )

        # create list of templates
        self._templates = {}       
        for key, value in self._obj[ 'templates' ].items():    

            # read sql template file
            with open ( str( value ) ) as fp:
                command = fp.read()

            # append buffer and id 
            self._templates[ key ] = command

        return
        

    def getName( self ):

        """
        get repository name
        """

        return self._obj[ 'name' ]


    def getPath( self ):

        """
        get repository root path
        """

        return self._obj[ 'path' ]


    def getKeywords( self ):

        """
        get keywords
        """

        return self._obj[ 'keywords' ]


    def getDescription( self ):

        """
        get description
        """

        return self._obj[ 'description' ]


    def getProductNameList( self ):

        """
        get product name list
        """

        names = []

        # create list of product names
        for item in self._products:
            names.append( item.getName() )
    
        return names


    def getProduct( self, name ):

        """
        get product 
        """

        product = None

        # search repository list for name match
        for item in self._products:
            if item.getName() == name:
                product = item
                break
    
        return product


    def getProductImageList( self, product, path=None ):

        """
        get image list
        """

        images = None

        # use default path
        if path is None:
            path = self._obj[ 'path' ] 

        # attempt to parse path as gcs uri
        bucket, prefix = GsClient.parseUri( path )
        if bucket is not None:

            # add credential variables to environment
            if self._obj[ 'credentials' ] is not None:
                GsClient.updateCredentials( self._obj[ 'credentials' ] )

            # create client and get image list                
            client = GsClient( bucket )
            images = client.getImageUriList( prefix, pattern=product.getPattern() )

        else:

            # get local file list matched with regexp
            images = fs.getFileList( path, product.getPattern() )

        return images


    def ingestImages( self, product ):

        """
        ingest image into database as postgis raster objects
        """

        def getCoreParameterList( server ):

            """
            compile parameter values for transaction into dict 
            """

            parameters = {}

            # get schema and product table names
            parameters[ 'DATABASE' ] = server.getDatabase()
            parameters[ 'SCHEMA' ] = self.getName()
            parameters[ 'PRODUCT' ] = product.getName()

            # get records for product and measurement ancillary tables
            parameters[ 'PRODUCT_DATA' ] = product.getSqlRecord()
            parameters[ 'MEASUREMENT_DATA' ] = product.getMeasurementSqlRecords()
            parameters[ 'TILE_SIZE' ] = product.getTileSize()

            return parameters


        # 1 or more servers
        for server in self._servers:

            # compile list of product-specific core values to specialise sql scripts
            parameters = getCoreParameterList( server )

            # execute preprocess operation
            out, error, code = self.executeTemplateOperation( server, 'preprocess', parameters )
            if 'ERROR' not in str( error ):

                # get ingestion task list - split across multiple threads
                tasks, task_count = self.getTaskList( server, product )
                with ProgressBar( max_value=task_count ) as bar:
        
                    # execute tasks        
                    threads = []; 
                    for task in tasks:

                        # start thread
                        process = Thread(target=self.executeTask, args=[ task, parameters.copy(), bar ] )
                        process.start()
                        threads.append(process)

                    # pause main thread until all child threads complete
                    for process in threads:
                        process.join()

        return


    def getTaskList( self, server, product ):

        """
        get task list
        """

        tasks = []
        images = self.getProductImageList( product )

        # initialise loop variables
        step = int ( round ( len ( images ) / self._threads ) )
        start = 0
        
        # equally allocate tile ids between threads
        for thread in range( 0, self._threads ):
          
            #  update subset of records to process
            end = start + step
            if thread == self._threads - 1:
                end = len( images )

            #  append a new task
            tasks.append ( {    'index' : thread, 
                                'server' : server,
                                'product' : product,
                                'images' : images[ start : end ] 
                            } )
            start = end

        return tasks, len ( images )


    def executeTask( self, task, parameters, bar ):

        """
        entry point into ingestion thread function
        """

        async def threadFunc ():

            """
            iterate through image list
            """

            # iterate through images
            task [ 'parameters' ] = parameters
            for image in task[ 'images' ]:
    
                try:
                    # ingest image into database
                    self.ingestImage( task, image )
                    bar.update()
                
                # report error
                except Exception as e:
                    print ( 'Ingestion Error: {}'.format( e.args ) )
                    break

            return
                               

        # create new asynch event loop and execute
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # wait until sub function complete - close loop
        loop.run_until_complete( threadFunc () )
        loop.close()

        return 


    def ingestImage( self, task, pathname ):

        """
        ingest image into database as postgis raster objects
        """

        def getProductParameterList():

            """
            compile parameter values for transaction into dict 
            """

            parameters = {}

            # get parameters from pathname
            parameters[ 'PATHNAME' ] = pathname
            parameters[ 'PATH' ] = os.path.dirname( pathname )

            dt = parser.getDateTime( pathname )
            parameters[ 'TIMESTAMP' ] = dt.strftime("%Y-%m-%d %H:%M:%S")

            return parameters


        # raster already in database                
        code = 0
        print ( pathname )
        if not self.isLoaded( task[ 'server' ], pathname ):

            # create temp path            
            with tempfile.TemporaryDirectory() as tmp_path:

                # compile list of product-specific parameter values to specialise sql scripts
                task[ 'parameters' ] = { **task[ 'parameters' ], **getProductParameterList() }
                task[ 'parameters' ][ 'TEMP_TABLE' ] = os.path.basename( tmp_path )

                # load raster as tiles into database table
                out, error, code = task[ 'server' ].loadRaster( task[ 'parameters' ] )
                if 'ERROR' not in str( error ):

                    # execute post-process
                    self.executeTemplateOperation( task[ 'server' ], 'postprocess', task[ 'parameters' ] )

                # raise exception on error
                if 'ERROR' in str( error ):
                    raise ValueError ( pathname, out, error, code )
                    
        return


    def executeTemplateOperation( self, server, operation, parameters ):

        """
        execute operation
        """

        def transposeTokens( command, parameters  ):

            """
            transpose parameter values into template sql
            """

            for key, value in parameters.items():

                # map demarked key with value
                label = '!' + key.upper() + '!'
                command = command.replace( label, value )

            return command

        # transpose template with actual parameter values
        buffer = transposeTokens( self._templates[ operation ], parameters )

        # execute transposed template from file 
        with tempfile.TemporaryDirectory() as tmp_path:
            pathname = os.path.join( tmp_path, '{}.sql'.format( operation ) )
            out, error, code = server.executeTransactionFromFile( fs.writeFile( pathname, buffer ) )

        return out, error, code


    def isLoaded(self, server, pathname ):

        """
        check image already loaded in database
        """

        # query pathname in catalog table
        records = server.executeQuery( "SELECT pathname FROM {repository}.cat WHERE pathname = '{pathname}'".format(    repository=self.getName(), 
                                                                                                                        pathname=pathname ) )
        if len( records ) == 1:
            return True

        return False


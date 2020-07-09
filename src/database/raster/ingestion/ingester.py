import os
import copy
import asyncio
import tempfile

from threading import Thread
from progressbar import ProgressBar

from src.utility import parser


class Ingester:


    def __init__( self, repo, threads=6 ):

        """
        constructor
        """

        # members
        self._threads = threads
        self._repo = repo
        return


    def process( self, product ):

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
            parameters[ 'SCHEMA' ] = self._repo.getName()
            parameters[ 'PRODUCT' ] = product.getName()

            # get records for product and measurement ancillary tables
            parameters[ 'PRODUCT_DATA' ] = product.getSqlRecord()
            parameters[ 'MEASUREMENT_DATA' ] = product.getMeasurementSqlRecords()
            parameters[ 'TILE_SIZE' ] = product.getTileSize()

            return parameters


        # 1 or more servers
        for server in self._repo.getServerList():

            # compile list of product-specific core values to specialise sql scripts
            parameters = getCoreParameterList( server )

            # execute preprocess operation
            out, error, code = server.executeTemplateOperation( self._repo.getTemplate( 'ingest-preprocess' ), parameters )
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
        images = self._repo.getProductImageList( product )

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
                    task[ 'server' ].executeTemplateOperation( self._repo.getTemplate( 'ingest-postprocess' ), 
                                                                task[ 'parameters' ] )

                # raise exception on error
                if 'ERROR' in str( error ):
                    raise ValueError ( pathname, out, error, code )
                    
        return


    def isLoaded(self, server, pathname ):

        """
        check image already loaded in database
        """

        # query pathname in catalog table
        records = server.getRecords( "SELECT pathname FROM {repository}.cat WHERE pathname = '{pathname}'".format(  repository=self._repo.getName(), 
                                                                                                                    pathname=pathname ) )
        if len( records ) == 1:
            return True

        return False

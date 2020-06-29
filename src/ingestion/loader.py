import os
import gdal
import asyncio
import asyncpg
 
from threading import Thread

from src.utility import parser
from repository import Repository

class Loader:


    def __init__( self, repository, root_path, threads=4 ):

        """
        constructor
        """

        # copy members
        self._repository = repository
        self._root_path = root_path
        self._threads = threads
        
        return


    def process( self, pathname, product, chunk_size=4096 ):

        """
        process        
        """

        # construct path for vrts
        out_path = os.path.join( self._root_path, parser.getTle( pathname ) ) 
        out_path = os.path.join( out_path, parser.getDateTimeString( pathname ) )
        
        # slice dataset into virtual raster files
        slices = self.sliceDataset( pathname, out_path, chunk_size=chunk_size )
        tasks = self.getTaskList ( slices )

        # execute tasks        
        threads = []; 
        for task in tasks:

            # start thread
            process = Thread( target=self.executeTask, args=[ task, product ] )
            process.start()
            threads.append(process)

        # pause main thread until all child threads complete
        for process in threads:
            process.join()

        return


    def sliceDataset( self, pathname, out_path, chunk_size=4096 ):

        """
        get list of repository names
        """

        # open dataset
        src_ds = gdal.Open( pathname, gdal.GA_ReadOnly )
        if src_ds is not None:

            # create out path
            if not os.path.exists( out_path ):
                os.makedirs( out_path )

            # step through tile locations
            slices = []
            for y1 in range ( 0, src_ds.RasterYSize, chunk_size ):
                
                for x1 in range ( 0, src_ds.RasterXSize, chunk_size ):

                    # compute rightside coordinates
                    x2 = min( ( x1 + chunk_size ), src_ds.RasterXSize )
                    y2 = min( ( y1 + chunk_size ), src_ds.RasterYSize )

                    # create vrt pathname
                    slices.append( os.path.join( out_path, 'slice_{}_{}_{}_{}.vrt'.format( x1, y1, x2-x1, y2-y1 ) ) )
                    gdal.Translate( slices[ -1 ], src_ds, srcWin=[ x1, y1, (x2-x1), (y2-y1) ] )

            # close dataset
            src_ds = None

        return slices


    def getTaskList( self, slices ):

        """
        get task list
        """

        tasks = []

        # initialise loop variables
        step = int ( round ( len ( slices ) / self._threads ) )
        start = 0
        
        # equally allocate tile ids between threads
        for thread in range( 0, self._threads ):
          
            #  update subset of records to process
            end = start + step
            if thread == self._threads - 1:
                end = len( slices )

            #  append a new task
            tasks.append ( { 'index' : thread, 'slices' : slices[ start : end ] } )
            start = end

        return tasks


    def executeTask( self, task, product ):

        """
        execute task
        """

        async def threadFunc ():

            """
            thread function
            """

            # for each tile 
            for pathname in task[ 'slices' ]:
                self._repository.ingestImage( pathname, product )

            return
                               
        # create new asynch event loop and execute
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # wait until sub function complete - close loop
        loop.run_until_complete( threadFunc () )
        loop.close()

        return 

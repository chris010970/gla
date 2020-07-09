import os
import glob
import tempfile
import argparse
import src.utility.ogr2ogr as ogr2ogr

from datetime import datetime
from src.database.objects.manager import Manager


def getTaskList( server, args ):

    """
    get task list to populate tiled geometry with raster info
    """

    tasks = []

    # get total records into tiled geometry
    records = server.getRecordCount( args.schema, args.tile_table )

    # initialise loop variables
    step = int ( round ( records / args.threads ) )
    start = 0
    
    # equally allocate tile ids between threads
    for thread in range( 0, args.threads ):
        
        #  update subset of records to process
        end = start + step
        if thread == args.threads - 1:
            end = records




        #  append a new task
        tasks.append ( {    'index' : thread, 
                            'start_row' : start,
                            'end_row' : end
                        } )
        start = end

    return tasks


def importGeometry( server, args ):

    """
    parse custom argparse *date* type 
    """

    # get schema name
    cmd_args = [    'ogr2ogr',
                    '-f', 
                    'PostgreSQL',
                    server.getGdalConnectionString(),
                    args.geometry_file,
                    '-lco',
                    'SCHEMA={schema}'.format( schema=args.schema ),
                    '-lco',
                    'OVERWRITE=YES',
                    '-lco', 
                    'GEOMETRY_NAME=geom',
                    '-nln',
                    args.table ]

    # execute ogr2ogr
    return ogr2ogr.main( cmd_args )


def validDateTimeArgument ( arg ):

    """
    parse custom argparse *date* type 
    """
    
    try:
        # parse datetime string
        return datetime.strptime( arg, "%d/%m/%Y %H:%M:%S" )
    except ValueError:
        msg = "Given Date ({0}) not valid! Expected format, YYYY-MM-DD!".format(arg)
        raise argparse.ArgumentTypeError(msg)


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-aoi')
    parser.add_argument('geometry_file', action="store")
    parser.add_argument('config_file', action="store")
    parser.add_argument('repository', action="store")
    parser.add_argument('product', action="store")

    # naming options
    parser.add_argument('-schema', default=None, action="store" )
    parser.add_argument('-table', default=None, action="store" )

    # filter options
    parser.add_argument('-s','--start_dt', type=validDateTimeArgument, help='start datetime', default=None )
    parser.add_argument('-e','--end_dt', type=validDateTimeArgument, help='end datetime', default=None )

    # tiler options
    parser.add_argument('-size', default=0.001, type=int, action="store" )
    parser.add_argument('-threads', default=6, type=int, action="store" )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()
    manager = Manager( args.config_file )

    # setup defaults
    if args.schema is None:
        args.schema = args.repository

    if args.table is None:
        args.table = os.path.splitext( os.path.basename( args.geometry_file ) )[ 0 ]

    # check valid args
    if os.path.exists( args.geometry_file ):
    
        # get repository
        repo = manager.getRepository( args.repository )
        if repo is not None:

            # get product
            product = repo.getProduct( args.product )
            if product is not None:

                # 1 or more servers
                for server in repo.getServerList():

                    # get schema / table names
                    if importGeometry( server, args ):

                        # tile geometry  
                        args.tile_table = os.path.basename(tempfile.mkstemp('','tile','')[1])
                        server.executeCommand( 'CREATE TABLE {schema}.{tile_table} AS '\
                                    'SELECT Tiler( {size}, geom ) AS geom FROM {schema}.{table}'.format( schema=args.schema,
                                                                                                        tile_table=args.tile_table,
                                                                                                        size=args.size,
                                                                                                        table=args.table ) )

                        # get task list to populate tiled geometry with raster info
                        tasks = getTaskList( server, args )

                        # execute tasks        
                        threads = []; 
                        for task in tasks:

                            # start thread
                            process = Thread(target=executeTask, args=[ task ] )
                            process.start()
                            threads.append(process)

                        # pause main thread until all child threads complete
                        for process in threads:
                            process.join()

                        

    return
    

# execute main
if __name__ == '__main__':
    main()


import os
import argparse
import shutil
from datetime import datetime

# local imports
from spot import Spot
from pleiades import Pleiades
from dataset import Dataset

from src.utility import log
from src.utility import parser
from src.utility.gsclient import GsClient


def checkOutputExists( blobs, client ):

    """
    remove blobs whose output directory + files already exist
    """

    # for each blob name
    results = []
    for blob in blobs:

        # get blobs in output directory
        path = os.path.dirname( blob ).replace( 'raw', 'ard' )
        out_blobs = client.getBlobNameList( path, '.*TIF' )

        # no blobs - no output
        if len ( out_blobs ) == 0:
            results.append( blob )
        else:
            print ( 'output exists: {}', path )

    return results


def applyFilters( blobs, args ):

    """
    parse bucket name and prefix from uri string
    """

    def validTile( blob ):

        """
        parse bucket name and prefix from uri string
        """

        valid = True

        # check blob name includes specified tile
        if args.tiles is not None:
            valid = any( tile in blob for tile in args.tiles )

        return valid 


    def validDateTime( blob ):

        """
        parse bucket name and prefix from uri string
        """

        valid = False

        # parse datetime from directory structure
        dt = parser.getDateTime( blob )
        if dt is not None:

            # check datetime satisfies temporal constraints
            if args.start_dt is None or dt >= args.start_dt:

                if args.end_dt is None or dt <= args.end_dt:
                    valid = True

        return valid 


    # for each blob name
    results = []
    for blob in blobs:

        # apply tile filter
        if validTile( blob ):

            # apply datetime filter
            if validDateTime( blob ):
                results.append( blob )

    return results


def getRoiTiles( roi ):

    """
    parse custom argparse *date* type 
    """

    # lat range - assume northern hemisphere
    tiles = []
    for lat in range( roi[ 1 ], roi[ 3 ] + 1 ):

        # west of meridian
        if roi[ 0 ] < 0:

            for lon in range ( roi[ 0 ], min( 0, roi[ 2 ] ) ):
                tiles.append ( 'W{:03d}N{:02d}'.format ( abs( lon ), lat ) )

        # east of meridian
        if roi[ 2 ] >= 0:

            for lon in range ( max( roi[ 0 ], 0 ), roi[ 2 ] + 1 ):
                tiles.append ( 'E{:03d}N{:02d}'.format ( lon, lat ) )

    # confirm filter
    return tiles


def validDateTimeArgument ( arg ):

    """
    parse custom argparse *date* type 
    """
    
    try:
        return datetime.strptime( arg, "%d/%m/%Y %H:%M:%S" )
    except ValueError:
        msg = "Given Date ({0}) not valid! Expected format, YYYY-MM-DD!".format(arg)
        raise argparse.ArgumentTypeError(msg)

                    
def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-ard')
    parser.add_argument( 'uri', action="store" )
    parser.add_argument( 'key_pathname', action="store" )
    parser.add_argument( 'download_path', action="store" )
    parser.add_argument('-t','--tles', nargs='+', help='tles', type=int, required=True )

    # filter options
    parser.add_argument('-s','--start_dt', type=validDateTimeArgument, help='start datetime', default=None )
    parser.add_argument('-e','--end_dt', type=validDateTimeArgument, help='end datetime', default=None )
    
    parser.add_argument('-roi', nargs=4,default=None, action="store", type=int )
    parser.add_argument('-tiles', nargs='+', default=None, action="store" )

    # runtime performance
    parser.add_argument('-log_path', default='.', action="store" )
    parser.add_argument('-chunk_size', default=None, action="store", type=int )
    parser.add_argument('-ram', default=4096, action="store", type=int )

    # elevation data
    parser.add_argument('-dem_path', default=None, action="store" )
    parser.add_argument('-geoid_pathname', default=None, action="store" )
    
    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()
    
    # construct logger
    logger = log.getFileLogger( 'run-cloud', os.path.join( args.log_path, 'run-cloud.log' ) )

    # writeback args to log file
    logger.info( '******************** process start ********************')
    logger.info( 'bucket uri: {}'.format( args.key_pathname ) )
    logger.info( 'authentication key: {}'.format( args.key_pathname ) )
    logger.info( 'download path: {}'.format( args.download_path ) )
    logger.info( 'dem path: {}'.format( args.dem_path ) )
    logger.info( 'geoid pathname: {}'.format( args.geoid_pathname ) )

    logger.info( 'otb ram: {ram} (MB)'.format( ram='default' if args.ram is None else int( args.ram ) ) )
    logger.info( 'gcs chunk size: {chunk} (MB)'.format( chunk='default' if args.chunk_size is None else int( args.chunk_size ) ) )

    # convert roi into corresponding tile strings
    if args.roi is not None:
        args.tiles = getRoiTiles( args.roi )

    # filters
    logger.info( 'tles: {}'.format( ','.join( [ str(i) for i in args.tles ] ) ) )
    logger.info( 'tiles: {tiles}'.format( tiles='n/a' if args.tiles is None else ','.join( args.tiles ) ) )
    logger.info( 'start datetime: {dt}'.format( dt='n/a' if args.start_dt is None else args.start_dt ) )
    logger.info( 'end datetime: {dt}'.format( dt='n/a' if args.end_dt is None else args.end_dt ) )

    # parse uri
    bucket, prefix = GsClient.parseUri( args.uri )
    if bucket is not None:

        # update credentials
        if os.path.exists( args.key_pathname ):
            GsClient.updateCredentials( args.key_pathname )

        # open client
        client = GsClient( bucket, chunk_size=args.chunk_size )
        for tle in args.tles:

            # retrieve list of blobs in prefix + tle directory            
            bucket_path = '{}/{}'.format( prefix, str( tle ) ).lstrip('/')

            blobs = client.getBlobNameList( bucket_path, '.*zip' )
            logger.info( 'blobs found: {}'.format( str( len( blobs ) ) ) )

            # apply filters to blob list
            blobs = applyFilters( blobs, args )
            logger.info( 'blobs after filtering: {}'.format( str( len( blobs ) ) ) )

            # check output files already exist
            blobs = checkOutputExists( blobs, client )
            logger.info( 'blobs after output check: {}'.format( str( len( blobs ) ) ) )

            for blob in blobs:

                # download blob to local file system
                logger.info( 'downloading {} -> {}'.format( blob, args.download_path ) )
                scene = client.downloadBlob( blob, args.download_path )
                logger.info( '... OK' )

                # get dataset id
                _name = Dataset.getClassName( scene )
                if _name is not None:

                    logger.info( 'processing scene: {}'.format( scene ) )

                    # create object
                    _class = globals()[ _name ]            
                    obj = _class (  scene,
                                    dem_path=args.dem_path,
                                    geoid_pathname=args.geoid_pathname,
                                    pan_method='bayes',
                                    roi=args.roi,
                                    log_path=args.log_path,
                                    ram=args.ram )
                    try:

                        # execute process to ard
                        out_files = obj.processToArd()
                        logger.info( '... OK' )

                    except Exception as e:

                        # handle exception
                        logger.info( '... ERROR: {} {} {}'.format( scene, e.message, e.args ) )
                        out_files = None

                    finally:

                        # mark object for deletion
                        obj = None

                    # upload files
                    if out_files is not None:

                        for f in out_files:

                            # construct paths                        
                            upload_path = '{}/{}'.format( bucket_path, parser.getDateTimeString( f ) )
                            upload_path = upload_path.replace( 'raw', 'ard' )

                            logger.info( 'uploading {} -> {}'.format( f, upload_path ) )
                            client.uploadFile( f, prefix=upload_path, flatten=True )
                            logger.info( '... OK' )


                # remove directory
                shutil.rmtree( os.path.dirname ( scene ) )

    return


# execute main
if __name__ == '__main__':
    main()


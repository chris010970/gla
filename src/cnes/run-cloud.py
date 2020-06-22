import os
import argparse
import shutil
from datetime import datetime

# local imports
from spot import Spot
from pleiades import Pleiades
from dataset import Dataset

from src.utility import parser
from src.utility.gsclient import GsClient


def applyFilters( blobs, args ):


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


    """
    parse bucket name and prefix from uri string
    """

    # for each blob name
    results = []
    for blob in blobs:

        # apply tile filter
        if validTile( blob ):

            # apply datetime filter
            if validDateTime( blob ):
                results.append( blob )

    return results


def parseUri( uri ):

    """
    parse bucket name and prefix from uri string
    """

    bucket = None

    # check gcs compliant
    drive = 'gs://'
    if drive in uri:
        
        # look for prefix
        tail = uri[ len( drive )  : ]
        tokens = tail.split( '/' )

        bucket = tokens[ 0 ]
        prefix = ''

        # retrieve prefix
        if len( tokens ) > 1:
            prefix = '/'.join( tokens[ 1 : ] )

    return bucket, prefix        


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
    print ( 'Filtering on tiles: {}'.format( tiles ) )
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

    # options
    parser.add_argument('-s','--start_dt', type=validDateTimeArgument, help='start datetime', default=None )
    parser.add_argument('-e','--end_dt', type=validDateTimeArgument, help='end datetime', default=None )
    
    parser.add_argument('-roi', nargs=4,default=None, action="store", type=int )
    parser.add_argument('-tiles', nargs='+', default=None, action="store" )

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

    if args.roi is not None:
        args.tiles = getRoiTiles( args.roi )

    # parse uri
    bucket, prefix = parseUri( args.uri )
    if bucket is not None:

        if os.path.exists( args.key_pathname ):
            GsClient.updateCredentials( args.key_pathname )

        # open client
        client = GsClient( bucket, chunk_size=2097152 )  # 1024 * 1024 B * 2 = 2 MB
        for tle in args.tles:

            # retrieve list of blobs in prefix + tle directory            
            bucket_path = '{}/{}'.format( prefix, str( tle ) ).lstrip('/')

            blobs = client.getBlobNameList( bucket_path, [ '.zip' ] )
            blobs = applyFilters( blobs, args )

            for blob in blobs:

                # download blob to local file system
                scene = client.downloadBlob( blob, args.download_path )

                # get dataset id
                _name = Dataset.getClassName( scene )
                if _name is not None:

                    # create object and process ard
                    _class = globals()[ _name ]            
                    obj = _class (  scene,
                                    dem_path=args.dem_path,
                                    geoid_pathname=args.geoid_pathname,
                                    pan_method='bayes',
                                    roi=args.roi )

                    out_files = obj.processToArd()

                    # upload files
                    for f in out_files:

                        # construct paths                        
                        upload_path = '{}/{}'.format( bucket_path, parser.getDateTimeString( f ) )
                        upload_path = upload_path.replace( 'raw', 'ard' )

                        client.uploadFile( f, prefix=upload_path, flatten=True )

    return


# execute main
if __name__ == '__main__':
    main()


import os
import pdb
import gdal
import shutil
import argparse
import numpy as np

from src.utility import parser
from src.utility.gsclient import GsClient


def setNoData( pathname, nodata=0 ):

    """
    set no data
    """

    # open the file for editing
    ds = gdal.Open( pathname, gdal.GA_Update)
    if ds is not None:

        for i in range(1, ds.RasterCount + 1):
            ds.GetRasterBand(i).SetNoDataValue(nodata)

    # save to file
    ds.FlushCache()
    ds = None

    return


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
    parser.add_argument('-chunk_size', default=None, action="store", type=int )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

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
            blobs = client.getBlobNameList( bucket_path, '.*_MS_.*TIF' )
            print( 'blobs found: {}'.format( str( len( blobs ) ) ) )

            for blob in blobs:

                # download blob to local file system
                print ( 'downloading: {}'.format ( blob ) )
                pathname = client.downloadBlob( blob, args.download_path )
                setNoData( pathname )

                # upload cog to bucket                       
                upload_path = '{}/{}'.format( bucket_path, parser.getDateTimeString( pathname ) )

                print( 'uploading: {}'.format( pathname ) )
                client.uploadFile( pathname, prefix=upload_path, flatten=True )
                
                # remove download directory
                shutil.rmtree( args.download_path )
                
    return


# execute main
if __name__ == '__main__':
    main()


import os
import gdal
import shutil
import argparse

from src.utility import parser
from src.utility.gsclient import GsClient


def convertToCog( pathname, out_pathname, options ):

    """
    convert image to COG with gdal translate functionality
    """

    # open existing image
    src_ds = gdal.Open( pathname, gdal.GA_ReadOnly )
    if src_ds is not None:

        # create out path if required
        out_path = os.path.dirname( out_pathname )
        if not os.path.exists( out_path ):
            os.makedirs( out_path )

        # execute translation - report error to log
        gdal.Translate( out_pathname, src_ds, format='COG', options=gdal.TranslateOptions(creationOptions=options ) )
                
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
            blobs = client.getBlobNameList( bucket_path, [ '.TIF' ] )

            for blob in blobs:

                # download blob to local file system
                pathname = client.downloadBlob( blob, args.download_path )
                out_pathname = pathname.replace( 'ard', 'cog' )  

                # convert to cog
                convertToCog(   pathname,  
                                out_pathname,
                                ['BIGTIFF=YES', 'COMPRESS=DEFLATE' ] )

                # upload cog to bucket                       
                upload_path = '{}/{}'.format( bucket_path, parser.getDateTimeString( out_pathname ) )
                upload_path = upload_path.replace( 'ard', 'cog' )

                client.uploadFile( out_pathname, prefix=upload_path, flatten=True )
                
                # remove download directory
                shutil.rmtree( args.download_path )
                
    return


# execute main
if __name__ == '__main__':
    main()


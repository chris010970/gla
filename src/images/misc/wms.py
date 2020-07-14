import os
import pdb
import gdal
import shutil
import argparse
import numpy as np

from src.utility import parser
from src.utility.gsclient import GsClient


def convertToCog( pathname, out_pathname, creationOptions ):

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
        gdal.Translate( out_pathname, src_ds, format='COG', creationOptions=creationOptions )
                
    return

             
def getPercentiles( pathname, band_idxs=[1,2,3], nbuckets=1000, percentiles=[2.0, 98.0] ):

    """
    compute percentile from image histogram - use to rescale 16bit to 8bit
    """

    results = []

    # open image dataset and band 
    src = gdal.Open( pathname )
    for idx in band_idxs: 
        
        # Use GDAL to find the min and max
        band = src.GetRasterBand( idx )
        (lo, hi, avg, std) = band.GetStatistics(True, True)

        # Use GDAL to calculate a big histogram
        rawhist = band.GetHistogram(min=lo, max=hi, buckets=nbuckets)
        binEdges = np.linspace(lo, hi, nbuckets+1)

        # Probability mass function. Trapezoidal-integration of this should yield 1.0.
        pmf = rawhist / (np.sum(rawhist) * np.diff(binEdges[:2]))

        # Cumulative probability distribution. Starts at 0, ends at 1.0.
        distribution = np.cumsum(pmf) * np.diff(binEdges[:2])

        # Which histogram buckets are close to the percentiles requested?
        bucket_idxs = [np.sum(distribution < p / 100.0) for p in percentiles]

        # record result in dict
        results.append (  [binEdges[i] for i in bucket_idxs] )

    return results


def rescaleTo8Bit( pathname, out_pathname, bands=[ 1, 2, 3 ], no_data=0 ):

    """
    convert image to COG with gdal translate functionality
    """

    # create out path if required
    out_path = os.path.dirname( out_pathname )
    if not os.path.exists( out_path ):
        os.makedirs( out_path )

    # get percentiles to computing 16bit to 8bit scaling  
    step = 1.0
    while step < 10.0:

        # iteratively reduce percentile range until 256 values accommodated
        results = getPercentiles( pathname, percentiles=[ step, 100.0 - step ] )
        for result in results:        
            
            if result[ 1 ] - result[ 0 ] > 255:
                step += 1.0
                continue

        break

    # open existing image
    src_ds = gdal.Open( pathname, gdal.GA_ReadOnly )
    if src_ds is not None:

        # get dimensions
        nCols = src_ds.GetRasterBand(1).XSize
        nRows = src_ds.GetRasterBand(1).YSize

        # create internal mask
        gdal.SetConfigOption('GDAL_TIFF_INTERNAL_MASK', 'YES')
        driver = gdal.GetDriverByName("GTiff")

        out_ds = driver.Create( out_pathname, nCols, nRows, len( bands ), gdal.GDT_Byte )
        if out_ds is not None:
            
            for out_idx, src_idx in enumerate( bands ): 

                # get bands
                src_band = src_ds.GetRasterBand( src_idx )
                out_band = out_ds.GetRasterBand( out_idx + 1 )

                # create mask band if first iteration
                if out_idx == 0:
                    out_band.CreateMaskBand( gdal.GMF_PER_DATASET ) 

                # set geotransform / projection as input
                out_ds.SetGeoTransform( src_ds.GetGeoTransform() )  
                out_ds.SetProjection( src_ds.GetProjection() )

                # get min and max
                min_value = round( results[ out_idx ] [ 0 ] )
                max_value = round( results[ out_idx ] [ 1 ] )

                # get scale and offset
                out_band.SetScale ( ( max_value - min_value ) / 255.0 )
                out_band.SetOffset ( min_value )

                # for each row
                rowRange = range( nRows )
                for row in rowRange:

                    data = src_band.ReadAsArray( 0, row, nCols, 1 )
                    result = np.zeros( (1, nCols) )

                    # rescale data to 8-bit
                    idx = ( data != src_band.GetNoDataValue() )
                    result[ idx ] = ( data[ idx ] - out_band.GetOffset() ) / out_band.GetScale()

                    # clip and write data to band 
                    result[ idx ] = np.clip( np.around( result[ idx ] ), 0.0, 255.0 )
                    out_band.WriteArray( np.uint8( result ), 0, row )

                    # write internal mask if 1st iteration
                    if out_idx == 0:
                        out_band.GetMaskBand().WriteArray( np.uint8( idx ), 0, row ) 

            # saves to disk!!
            out_ds.FlushCache() 
            out_ds = None    

    return


def checkOutputExists( blobs, client ):

    """
    remove blobs whose output directory + files already exist
    """

    # for each blob name
    results = []
    for blob in blobs:

        # get blobs in output directory
        path = os.path.dirname( blob ).replace( 'ard', 'wms' )
        out_blobs = client.getBlobNameList( path, '.*_MS_.*TIF' )

        # no blobs - no output
        if len ( out_blobs ) == 0:
            results.append( blob )
        else:
            print ( 'output exists: {}', path )

    return results


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

            # check output files already exist
            blobs = checkOutputExists( blobs, client )
            print( 'blobs after output check: {}'.format( str( len( blobs ) ) ) )

            for blob in blobs:

                # download blob to local file system
                print ( 'downloading: {}'.format ( blob ) )

                pathname = client.downloadBlob( blob, args.download_path )
                tmp_pathname = pathname.replace( 'ard', 'tmp' )  

                # rescale to 8bit
                print ( 'generating: {}'.format( tmp_pathname ) )
                rescaleTo8Bit(  pathname, tmp_pathname )

                # convert to cog with jpeg compression
                out_pathname = tmp_pathname.replace( 'tmp', 'wms' )
                convertToCog(   tmp_pathname, 
                                out_pathname,
                                ['BIGTIFF=YES', 'COMPRESS=JPEG', 'NUM_THREADS=ALL_CPUS' ] )

                # upload cog to bucket                       
                upload_path = '{}/{}'.format( bucket_path.replace( 'ard', 'wms' ), parser.getDateTimeString( out_pathname ) )

                print( 'uploading: {}'.format( out_pathname ) )
                client.uploadFile( out_pathname, prefix=upload_path, flatten=True )
                
                # remove download directory
                shutil.rmtree( args.download_path )
                
    return


# execute main
if __name__ == '__main__':
    main()


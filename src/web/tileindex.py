import os 
import pdb
import operator 
import argparse
import numpy as np
import geopandas as gpd

from osgeo import gdal, osr
from datetime import datetime
from shapely.geometry import box
from collections import defaultdict

from src.utility import parser
from src.utility.gsclient import GsClient


def getRasterBounds( path ):

    """
    get extent of raster in crs coordinates
    """

    # compute the bounding box of a gdal raster file
    in_ds = gdal.Open(path) 
    ulx, xres, xskew, uly, yskew, yres  = in_ds.GetGeoTransform()
    lrx = ulx + ( in_ds.RasterXSize * xres ) 
    lry = uly + ( in_ds.RasterYSize * yres )

    return box( lrx, lry, ulx, uly ) 


def getTimeIndexList( images ):

    """
    generate list of dates mapped to pathnames
    """

    # get index list
    indexList = defaultdict(list)
    for image in images:

        # create date time key
        dt = parser.getDateTime( image )
        key = dt.strftime( '%Y-%m-%dT00:00:00Z' )

        indexList[ key ].append( image )

    return indexList


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-ard')
    parser.add_argument( 'uri', action="store" )
    parser.add_argument( 'key_pathname', action="store" )
    parser.add_argument( 'out_pathname', action="store" )
    parser.add_argument('-t','--tles', nargs='+', help='tles', type=int, required=True )
    parser.add_argument('-chunk_size', default=None, action="store", type=int )
    parser.add_argument('-pattern', default='.*TIF', action="store" )

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

        images =  []
        for tle in args.tles:

            # retrieve list of blobs in prefix + tle directory            
            bucket_path = '{}/{}'.format( prefix, str( tle ) ).lstrip('/')
            images.extend( client.getImageUriList( bucket_path, pattern=args.pattern ) )

        print( 'images found: {}'.format( len( images )  ) )

        # sort into time ascending order
        indexList = getTimeIndexList( images )
        sortList = sorted( indexList.items(), key=operator.itemgetter(0) )

        # shape file attributes - time default
        gdf = gpd.GeoDataFrame(columns=['location','geometry','time'])
        gdf.crs = {'init' :'epsg:27700'}

        # for each item in sort list
        for entry in sortList:
            datetime = entry[ 0 ]

            # insert datetime and associated raster pathnames into index file
            for location in entry[ 1 ]:
                gdf = gdf.append({  'location': location, 
                                    'geometry': getRasterBounds( location ), 
                                    'time': datetime }, 
                                    ignore_index=True )

        # save to shape file
        if not os.path.exists( os.path.dirname( args.out_pathname ) ):
            os.makedirs( os.path.dirname( args.out_pathname ) )

        print( 'created: {}'.format( args.out_pathname ) )
        gdf.to_file( args.out_pathname )

    return


# execute main
if __name__ == '__main__':
    main()


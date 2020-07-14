import os 
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
    Placeholder
    """

    # compute the bounding box of a gdal raster file
    in_ds = gdal.Open(path) 
    ulx, xres, xskew, uly, yskew, yres  = in_ds.GetGeoTransform()
    lrx = ulx + ( in_ds.RasterXSize * xres ) 
    lry = uly + ( in_ds.RasterYSize * yres )

    return box( lrx, lry, ulx, uly ) 


def getReprojectedRasterBounds( path, epsg ):

    """
    Placeholder
    """

    # compute the bounding box of a gdal raster file
    in_ds = gdal.Open(path) 
    ext = None

    # get spatial reference systems    
    s_prj = in_ds.GetProjection()
    s_srs = osr.SpatialReference( wkt = s_prj )

    t_srs = osr.SpatialReference ()
    t_srs.ImportFromEPSG ( epsg )

    # compute bounding box coordinates in target srs
    s_geo = in_ds.GetGeoTransform ()
    tx = osr.CoordinateTransformation ( s_srs, t_srs )

    x_size = in_ds.RasterXSize
    y_size = in_ds.RasterYSize

    (ulx, uly, ulz ) = tx.TransformPoint( s_geo[0],  s_geo[3])
    (lrx, lry, lrz ) = tx.TransformPoint( s_geo[0] + s_geo[1] * x_size, \
                                          s_geo[3] + s_geo[5] * y_size )

    # validate reprojected extent
    if ( ~np.isinf( ulx ) and ~np.isinf( uly ) and ~np.isinf( lrx ) and ~np.isinf( lry ) ):
        ext = box(lrx,lry,ulx,uly)

    return ext, s_srs 


def getTimeIndexList( images ):

    """
    Placeholder
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
        df = gpd.GeoDataFrame(columns=['location','geometry','time'])

        # for each item in sort list
        for entry in sortList:
            datetime = entry[ 0 ]

            # insert datetime and associated raster pathnames into index file
            for location in entry[ 1 ]:
                df = df.append({    'location': location, 
                                    'geometry': getRasterBounds( location ), 
                                    'time': datetime }, 
                                    ignore_index=True )

        # save to shape file
        if not os.path.exists( os.path.dirname( args.out_pathname ) ):
            os.makedirs( os.path.dirname( args.out_pathname ) )

        print( 'created: {}'.format( args.out_pathname ) )
        df.to_file( args.out_pathname )

    return


# execute main
if __name__ == '__main__':
    main()


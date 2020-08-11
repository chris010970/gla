import os 
import glob
import json
import argparse
import numpy as np

import fiona
from fiona import crs

from osgeo import ogr
from osgeo import gdal, ogr
from shapely.geometry import box
from shapely.ops import transform
from shapely.wkt import dumps, loads
from shapely.geometry import shape
from shapely.geometry import mapping

from src.utility import parser 
from src.utility.gsclient import GsClient


def saveAoI( aoi, out_path ):

    """
    get footprint polygon of raster
    """

    # get crs
    bng = crs.from_epsg(27700)

    # define polygon feature geometry with one attribute
    schema = {
        'geometry' : 'Polygon',
        'properties': { 'id': 'int' },
    }

    # create new shapefile
    with fiona.open( os.path.join( out_path, 'aoi.shp' ), 'w', 'ESRI Shapefile', schema, crs=bng ) as c:
        c.write({
            'geometry': mapping( aoi ),
            'properties': { 'id': 1 },
        })

    return


def saveFootprints( intersects, out_path ):

    """
    get footprint polygon of raster
    """

    # get crs
    bng = crs.from_epsg(27700)

    # define polygon feature geometry with one attribute
    schema = {
        'geometry' : 'Polygon',
        'properties': { 'id': 'int', 'pathname' : 'str' },
    }

    # create new shapefile
    with fiona.open( os.path.join( out_path, 'footprints.shp' ), 'w', 'ESRI Shapefile', schema, crs=bng ) as c:

        for idx, intersect in enumerate( intersects ):
            c.write({
                'geometry': mapping( intersect[ 'roi' ] ),
                'properties': {     'id': idx, 
                                    'pathname': intersect[ 'pathname' ],
                            },
            })

    return


def getRoiMask( pathname, client ):

    """
    get extent of raster in crs coordinates
    """

    mask = None

    # get path to mask gml
    path = os.path.dirname( pathname )
    path = path [ path.find( 'ssgp' ) : ].replace( 'wms', 'anc' )  

    # locate file
    files = client.getImageUriList( path, pattern='ROI.*_P_.*MSK.GML' )
    if len ( files ) > 0:

        # load roi polygon from gml file on gcs
        gml = ogr.Open( files[ 0 ] )
        
        layer = gml.GetLayer()
        feature = layer.GetNextFeature()
        
        # create shapely obj from wkt
        wkt = feature.GetGeometryRef().ExportToWkt()
        mask = loads( wkt )


    return mask


def getRasterBounds( pathname ):

    """
    get extent of raster in crs coordinates
    """

    # compute the bounding box of a gdal raster file
    in_ds = gdal.Open( pathname ) 
    ulx, xres, xskew, uly, yskew, yres  = in_ds.GetGeoTransform()
    lrx = ulx + ( in_ds.RasterXSize * xres ) 
    lry = uly + ( in_ds.RasterYSize * yres )

    return box( lrx, lry, ulx, uly ) 


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-ard')
    parser.add_argument( 'uri', action="store" )
    parser.add_argument( 'key_pathname', action="store" )
    parser.add_argument( 'geometry_pathname', action="store" )
    parser.add_argument( 'out_path', action="store" )
    parser.add_argument('-t','--tles', nargs='+', help='tles', type=int, required=True )
    parser.add_argument('-pattern', default='.*_MS_.*TIF', action="store" )
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

        images =  []
        for tle in args.tles:

            # retrieve list of blobs in prefix + tle directory            
            bucket_path = '{}/{}'.format( prefix, str( tle ) ).lstrip('/')
            images.extend( client.getImageUriList( bucket_path, pattern=args.pattern ) )

        print( 'images found: {}'.format( len( images )  ) )

        # compute bboxes for images
        footprints = []
        for image in images:            
            footprints.append( {    'pathname' : image,
                                    'bbox' : getRasterBounds( image ),
                                    'roi' : getRoiMask( image, client ) } )

        # open geometries pathname
        ds = ogr.Open( args.geometry_pathname )
        if ds is not None:

            layer = ds.GetLayer( 0 )
            for feature in layer:

                # convert ogr feature to shapely object
                json_obj = json.loads( feature.ExportToJson() )
                aoi = shape( json_obj[ 'geometry' ] )

                # get list of scenes intersecting 
                intersects = []
                for footprint in footprints:
                    if footprint[ 'roi' ].intersects( aoi ):

                        overlap = ( aoi.intersection( footprint[ 'roi' ] ).area / aoi.area ) * 100
                        if overlap > 50.0:

                            intersects.append( {    'pathname' : footprint[ 'pathname' ],
                                                    'roi' : footprint[ 'roi' ], 
                                                    'overlap' : overlap } )

                # save everything to shape file
                if len( intersects ) > 0:

                    name = json_obj[ 'properties'][ 'NAME' ].replace( ' ', '_' )
                    name = name.lower()

                    print ( name, len( intersects ) )
                    out_path = os.path.join( args.out_path, name )

                    # create out path if not exists
                    if not os.path.exists( out_path ):
                        os.makedirs( out_path )

                    # save borough aoi and footprints
                    saveFootprints( intersects, out_path )
                    saveAoI( aoi, out_path )
                
    return


# execute main
if __name__ == '__main__':
    main()

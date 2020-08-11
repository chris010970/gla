import os
import copy
import glob
import json
import argparse
import gdal, ogr

from shapely.geometry import shape
from src.utility import parser
from src.utility.bbox import BBox

# constant
root = '/web/mapserver/mapfile/gla'

def saveFile( buffer, pathname ):

    """
    save buffer to file
    """

    # make directory
    if not os.path.exists ( os.path.dirname( pathname ) ):
        os.makedirs( os.path.dirname( pathname ) )

    # read template map file
    with open ( pathname, 'w+' ) as fp:
        fp.write( buffer )

    return


def updateTemplateBuffer( template, parameter ):

    """
    update template with parameter values
    """

    # update template with scene specific values
    buffer = copy.deepcopy( template )
    for k, v in parameter.items():
        buffer = buffer.replace( '!{}!'.format( k ), v )

    return buffer


def getIndexParameters( pathname, borough, s_srs=27700, t_srs=3857 ):

    """
    get parameters to replace template placeholders
    """

    parameter = {}

    # open geometries pathname
    ds = ogr.Open( pathname )
    if ds is not None:

        layer = ds.GetLayer( 0 )
        for feature in layer:

            # convert ogr feature to shapely object
            json_obj = json.loads( feature.ExportToJson() )
            aoi = shape( json_obj[ 'geometry' ] )

            #bbox = BBox( aoi.bounds, s_srs )
            parameter =   { 'NAME' : borough,
                            'EXTENT' : ' '.join( [ str(x) for x in aoi.bounds ] ),
                            'MAP_PATHNAME' : '{root}/{borough}/index.map'.format( root=root, borough=borough ) }
                            #'EXTENT' : ' '.join( [ str(x) for x in bbox.transform( t_srs ).bounds ] ) }
            
    return parameter


def getLayerParameters( pathname, args ):

    """
    get parameters to replace template placeholders
    """

    parameters = []

    # open geometries pathname
    ds = ogr.Open( pathname )
    if ds is not None:

        layer = ds.GetLayer( 0 )
        for feature in layer:

            # convert ogr feature to shapely object
            json_obj = json.loads( feature.ExportToJson() )
            footprint = shape( json_obj[ 'geometry' ] )

            # get pathname + wms name
            pathname = json_obj[ 'properties' ][ 'pathname' ]
            name = '{group}_{datetime}'.format(     group=args.group, 
                                                    datetime=parser.getDateTimeString( pathname ) )

            parameters.append(  {   'GROUP' : args.group,
                                    'NAME' : name,
                                    'EXTENT' : ' '.join( [ str(x) for x in footprint.bounds ] ),
                                    'IMAGE_PATHNAME' : json_obj[ 'properties' ][ 'pathname' ],
                                    'WMS_TITLE' : name } )
            
    return parameters


def readFile( pathname ):

    """
    read template file into buffer
    """

    # read template map file
    with open ( pathname ) as fp:
        buffer = fp.read()

    return buffer


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-ard')
    parser.add_argument( 'group', action="store" )
    parser.add_argument( 'data_path', action="store" )
    parser.add_argument( 'template_path', action="store" )
    parser.add_argument( 'out_path', action="store" )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

    # read templates
    template = {}
    for item in [ 'index', 'layer' ]:
        template[ item ] = readFile( os.path.join( args.template_path, '{}.map'.format( item ) ) )

    # find aoi shapefiles
    files = glob.glob( os.path.join( args.data_path, '**/footprints.shp' ) )
    for f in files:

        # get path and borough
        layers = getLayerParameters( f, args )
        borough = os.path.basename ( os.path.dirname ( f ) )

        # initialise map pathname
        for layer in layers:
            layer.update( { 'MAP_PATHNAME' : '{root}/{borough}/{group}/{name}.map'.format(  root=root,
                                                                                            borough=borough,
                                                                                            group=args.group,
                                                                                            name=layer[ 'NAME'] ) } )
            # save updated buffer to file
            out_pathname = '{out_path}/{borough}/{group}/{name}.map'.format(    out_path=args.out_path,
                                                                                borough=borough,
                                                                                group=args.group,
                                                                                name=layer[ 'NAME'] )
            
            saveFile(   updateTemplateBuffer( template[ 'layer' ], layer ), 
                        out_pathname )


        # get index pathname
        out_pathname = '{out_path}/{borough}/index.map'.format( out_path=args.out_path,
                                                                borough=borough )

        if not os.path.exists ( out_pathname ):

            # create index file            
            saveFile(   updateTemplateBuffer(   template[ 'index' ], 
                                                getIndexParameters( os.path.join( os.path.dirname( f ), 'aoi.shp' ), borough ) ), 
                        out_pathname )
                
        # create includes                    
        includes = '\t'.join( [ 'INCLUDE\t\t\t"' + x[ 'MAP_PATHNAME'] + '"\n' for x in layers ] )
        includes += '\n\nEND\n'

        # append to foot of index file buffer
        buffer = readFile ( out_pathname )
        buffer = buffer[ 0 : buffer.rfind( 'END' ) ] + includes

        # read template map file
        saveFile( buffer, out_pathname )

    return

# execute main
if __name__ == '__main__':
    main()

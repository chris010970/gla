import os
import re
import yaml
import argparse
from datetime import datetime

from qgis.core import QgsApplication
from qgis.core import QgsProject
from qgis.core import QgsDataSourceUri
from qgis.core import QgsRasterLayer
from qgis.core import QgsLayerTree
from qgis.core import QgsContrastEnhancement

from src.database.objects.server import Server

import pdb


def addRasterLayers( project, server, schema ):
 
    """
    add postgis raster layers to qgis project
    """

    groups = []

    # get postgis raster table
    records = server.getTableNames ( schema, match='^rast_[0-9]{8}_.*')
    print ( 'processing {count} tables in schema {schema}'.format( count=len( records ), schema=schema ) )
    for record in records:

        # get gdal postgis raster connection string
        table = record[ 0 ]
        conn = '{gdal} mode=2 schema={schema} table={table} column=rast'.format (   gdal=server.getGdalConnectionString(),
                                                                                    schema=schema,
                                                                                    table=table )
        # get layer object
        layer = QgsRasterLayer( conn, table, 'gdal' )
        if layer.isValid():

            try:
            
                # add to project
                layer.setContrastEnhancement( QgsContrastEnhancement.StretchToMinimumMaximum )
                QgsProject.instance().addMapLayer( layer )

                # extract date from table name
                dt = datetime.strptime( re.search( '[0-9]{8}', table )[ 0 ], '%Y%m%d' )

                sub_group = next(( item for item in groups if str( dt.year ) == item[ 'year' ] ), None )
                if sub_group is None:
                    groups.append ( { 'year' : str( dt.year ), 'layers' : [] } )

                # add layer id to schema / year indexed lists
                sub_group = next((item for item in groups if str( dt.year ) == item[ 'year' ] ), None )
                sub_group[ 'layers' ].append( { 'id' : layer.id(), 'dt' : dt, 'name' : table } )
            
            except Exception as e:
                print ( 'ERROR: {conn} {msg}'.format( conn=conn, msg=e) )

    # return layers in yearly slices in descending order
    return sorted( groups, key=lambda k: k['year'], reverse=True )


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-aoi')
    parser.add_argument('project_file', action="store")
    parser.add_argument('server_file', action="store")
    parser.add_argument('out_path', action="store")

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

    # initialise application
    qgs = QgsApplication([], False)
    QgsApplication.setPrefixPath( 'C:\\Program Files\\QGIS 3.14\\apps\\qgis', True )
    QgsApplication.initQgis()

    # load config parameters from file
    with open( args.server_file, 'r' ) as f:
        server = Server( yaml.safe_load( f ) )

    # get ldd specific schemas
    records = server.getSchemaNames( match='^ldd_.*' )
    for record in records:

        # initialise project instance
        project = QgsProject.instance() #
        project.read( args.project_file )

        groups = addRasterLayers( project, server, record[ 0 ] )
        root = project.layerTreeRoot()

        for group in groups:

            # sort layers into descending order
            group[ 'layers' ] = sorted( group[ 'layers' ], key=lambda k: k ['name'], reverse=True )

            # add layers to parent node
            parent = root.addGroup( group[ 'year' ] )
            for item in group[ 'layers' ]:

                layer = root.findLayer( item[ 'id' ] )
                clone = layer.clone()

                parent.insertChildNode( 0, clone )
                root.removeChildNode(layer)

            # set checked and expand
            parent.setItemVisibilityCheckedRecursive(False)
            parent.setExpanded( False )

        # write project file to disc
        if not os.path.exists( args.out_path ):
            os.makedirs( args.out_path )

        project.write( os.path.join( args.out_path, '{schema}.qgs'.format( schema=record[ 0 ] ) ) )


    return
    

# execute main
if __name__ == '__main__':
    main()

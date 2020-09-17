import os
import re
import yaml
import argparse

from Server import server
from datetime import datetime

from qgis.core import QgsProject
from qgis.core import QgsDataSourceUri
from qgis.core import QgsRasterLayer
from qgis.core import QgsLayerTree
from qgis.core import QgsContrastEnhancement

import pdb


def getRasterTableNames( server ):

    """
    get data tables
    """

    data = {}

    # get ldd specific schemas
    schemas = server.getSchemaNames( match='^ldd_.*' )
    for schema in schemas:

        # get raster tables - no ovrviews !
        data[ record[ 0 ] ] = server.getTableNames ( schema[ 0 ], match='^rast_[0-9]{8}_.*')

    return data


def addRasterLayers( project, server ):

    """
    add postgis raster layers to qgis project
    """

    groups = {}

    # get postgis raster table
    data = getRasterTableNames( server )
    for schema, tables in data.items():

        groups[ schema ] = []
        for table in tables:

            # extract date
            table = table[ 0 ]

            # get gdal postgis raster connection string
            conn = '{gdal} mode=2 schema={schema} table={table} column=rast'.format (   gdal=server.getGdalConnectionString(),
                                                                                        schema=schema,
                                                                                        table=table )
            # get layer object
            layer = QgsRasterLayer( conn, table, 'gdal' )
            if layer.isValid():

                # extract date from table name
                m = re.search( 'rast_[0-9]{8}_.*', table )
                if m:

                    # add to project
                    layer.setContrastEnhancement( QgsContrastEnhancement.StretchToMinimumMaximum )
                    QgsProject.instance().addMapLayer( layer )

                    # record layer info
                    date = datetime.strptime( m[ 0 ], '%Y%m%d' )
                    if str( date.year ) not in groups[ schema ]:
                        groups[ schema ].append ( { str( date.year ) : [] } )

                    # add layer id to schema / year indexed lists
                    groups[ schema ][ str( date.year ) ].append( layer.layerId() )

    return groups


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-aoi')
    parser.add_argument('config_pathname', action="store")

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

    # load config parameters from file
    with open( args.server_file, 'r' ) as f:
        server = Server( yaml.safe_load( f ) )

    # initialise project instance
    project = QgsProject.instance() #'C:\\Users\\Chris.Williams\\Desktop\\template-config.qgs'
    project.read( args.config_file )

    # add postgis raster layers to project
    groups = addRasterLayers( project, server )

    # get root node
    root = project.layerTreeRoot()
    for permission_id, year in groups.items():

        # add group nodes
        parent = root.addGroup( permission_id )
        child = parent.addGroup( year )

        for _id in groups[ permission_id ][ year ]:

            layer = root.findLayer( _id )
            clone = layer.clone()

            child.insertChildNode( 0, clone )
            root.removeChildNode(layer)

        # set checked and expand
        parent.setItemVisibilityCheckedRecursive(False)
        parent.setExpanded( False )


    return
    

# execute main
if __name__ == '__main__':
    main()

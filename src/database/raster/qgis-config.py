import os
import re
import yaml
import argparse
from datetime import datetime

from qgis.core import QgsApplication
from qgis.core import QgsProject
from qgis.core import QgsDataSourceUri
from qgis.core import QgsProviderRegistry
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

        uri_config = {
            # database parameters
            'dbname': server.getDatabase(),      # The PostgreSQL database to connect to.
            'host': server.getHost(),     # The host IP address or localhost.
            'port': server.getPort,          # The port to connect on.
            'sslmode':QgsDataSourceUri.SslDisable, # SslAllow, SslPrefer, SslRequire, SslVerifyCa, SslVerifyFull
            
            # user and password are not needed if stored in the authcfg or service
            'authcfg': None,  # The QGIS athentication database ID holding connection details.
            'service': None,         # The PostgreSQL service to be used for connection to the database.
            'username':'sac',        # The PostgreSQL user name.
            'password':'sac',        # The PostgreSQL password for the user.
            
            # table and raster column details
            'schema': schema,      # The database schema that the table is located in.
            'table': table,   # The database table to be loaded.
            'geometrycolumn':'rast',# raster column in PostGIS table
            'sql':None,             # An SQL WHERE clause. It should be placed at the end of the string.
            'key':None,             # A key column from the table.
            'srid':'epsg:27700',            # A string designating the SRID of the coordinate reference system.
            'estimatedmetadata':'False', # A boolean value telling if the metadata is estimated.
            'type':None,            # A WKT string designating the WKB Type.
            'selectatid':None,      # Set to True to disable selection by feature ID.
            'options':None,         # other PostgreSQL connection options not in this list.
            'enableTime': None,
            'temporalDefaultTime': None,
            'temporalFieldIndex': None,
            'mode':'2',             # GDAL 'mode' parameter, 2 unions raster tiles, 1 adds tiles separately (may require user input)
        }
        # remove any NULL parameters
        uri_config = {key:val for key, val in uri_config.items() if val is not None}
        
        # get the metadata for the raster provider and configure the URI
        md = QgsProviderRegistry.instance().providerMetadata('postgresraster')
        uri = QgsDataSourceUri(md.encodeUri(uri_config))

        layer = QgsRasterLayer(  uri.uri(False), table, "postgresraster")

        """
        conn = '{gdal} mode=2 schema={schema} table={table} column=rast'.format (   gdal=server.getGdalConnectionString(),
                                                                                    schema=schema,
                                                                                    table=table )
                                                                        
        # get layer object
        layer = QgsRasterLayer( conn, table,         postgresraster")

        """


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
                print ( 'ERROR: {conn} {msg}'.format( conn=uri.uri(False), msg=e) )

            break

        

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

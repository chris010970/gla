import os
import yaml
import tempfile
import argparse

import src.utility.ogr2ogr as ogr2ogr
from src.database.objects.server import Server


class Ingester:


    def __init__( self, config_file ):

        """
        constructor
        """

        # load config parameters from file
        with open( config_file, 'r' ) as f:
            self._server = Server( yaml.safe_load( f ) )

        return


    def process( self, args ):

        """
        execute processing
        """

        # import geometry to temp table
        temp_table = 'ingest_{}'.format( next(tempfile._get_candidate_names()) )
        if self.importToTempTable( args, temp_table ):

            # check name column exists
            if self._server.checkColumnExists( args.schema, temp_table, 'name' ):

                # get names + geometries from temp table
                records = self._server.getRecords( "SELECT name, geom, ST_SRID( geom ) FROM {schema}.{temp_table}".format(  schema=args.schema, 
                                                                                                                            temp_table=temp_table ) )
                if len( records ) > 0:

                    # create geometry table if not exists
                    query = """
                            CREATE TABLE IF NOT EXISTS {schema}.{table} ( id SERIAL PRIMARY KEY, name text NOT NULL UNIQUE, geom geometry );
                            """.format ( schema=args.schema, table=args.table )

                    if self._server.executeCommand( query ) is None:

                        # compile values from temp table
                        query = "INSERT INTO {schema}.{table} ( name, geom ) VALUES".format( schema=args.schema, table=args.table )
                        for idx, record in enumerate( records ):

                            query +=    """ 
                                        (  '{name}', 
                                            ST_SetSRID( ST_AsBinary( '{geom}'::geometry )::geometry, {srid}::integer ) )
                                        """.format(   name=record[ 0 ], geom=record[ 1 ], srid=record[ 2 ] )

                            if idx < len( records ) - 1:
                                query += ", "

                        # adapt behaviour on conflict
                        query +=  " ON CONFLICT (name) DO NOTHING" 
                        if self._server.executeCommand( query ) is None:
                            print ( 'OK!')


        # drop temp table
        query =  "DROP TABLE IF EXISTS {schema}.{temp_table}".format( schema=args.schema, temp_table=temp_table ) 
        self._server.executeCommand( query )

        return


    def importToTempTable( self, args, temp_table ):

        """
        import kml / geojson file into temp table as pg geometry object
        """

        # get schema name
        cmd_args = [    'ogr2ogr',
                        '-f', 
                        'PostgreSQL',
                        self._server.getGdalConnectionString(),
                        args.geometry_file,
                        '-lco',
                        'SCHEMA={schema}'.format( schema=args.schema ),
                        '-lco',
                        'OVERWRITE=YES',
                        '-lco', 
                        'GEOMETRY_NAME=geom',
                        '-nln',
                        temp_table,
                        '-nlt',
                        'PROMOTE_TO_MULTI' ]

        # execute ogr2ogr
        return ogr2ogr.main( cmd_args )


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-aoi')
    parser.add_argument('geometry_file', action="store")
    parser.add_argument('config_file', action="store")

    # naming options
    parser.add_argument('-schema', default='aoi', action="store" )
    parser.add_argument('-table', default=None, action="store" )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments and validate
    args = parseArguments()
    if os.path.exists( args.geometry_file ) and os.path.exists( args.config_file ):

        # setup defaults
        if args.table is None:
            args.table = os.path.splitext( os.path.basename( args.geometry_file ) )[ 0 ]

        # create instance and execute ingestion    
        obj = Ingester( args.config_file )
        obj.process( args )

    return
    
# execute main
if __name__ == '__main__':
    main()

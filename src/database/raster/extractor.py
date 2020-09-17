import os
import asyncio
import tempfile
import argparse

from threading import Thread
from datetime import datetime
from src.database.objects.manager import Manager


class Extractor:


    def __init__( self, repo ):

        """
        constructor
        """

        # members
        self._repo = repo
        return


    def process( self, product, args ):

        """
        control loop to handle product generation
        """

        # 1 or more servers
        for server in self._repo.getServerList():

            """
            records = server.getSchemaNames()
            for record in records:
                server.vacuumTables( record[ 0 ] )
            break
            """

            # default out schema to repo
            geom_names = [ args.geometry[ 'name' ] ] if 'name' in args.geometry else self.getGeometryNameList( server, args )
            for geom_name in geom_names:

                # create schema
                args.out_schema = geom_name
                if server.createSchema( args.out_schema ) is None:

                    # get dates of coincident rasters
                    dates = self.getCoverageList( server, product, args, geom_name )
                    for date in dates:

                        # create or replace output table
                        args.out_table = 'rast_{date}_{repository}'.format (    repository=self._repo.getName(),
                                                                                product=product.getName(),
                                                                                date=date.strftime('%Y%m%d') )

                        # bypass if table already exists
                        if server.checkTableExists( args.out_schema, args.out_table ):
                            print ( 'Raster table exists: {}.{}'.format( args.out_schema, args.out_table ) )
                            continue

                        # create table
                        if server.createOrReplaceTable(     args.out_schema, 
                                                            args.out_table, 
                                                            { 'rast': 'raster' } ) is None:       

                            # optional no clip to geometry - keeps image data outside database (very fast)
                            raster_value = 'ST_SetBandNoDataValue( ST_Clip( rast, geom ), 0.0 )'
                            if args.no_clip:
                                raster_value = 'ST_SetBandNoDataValue( rast, 0.0 )'

                            # format sql query 
                            query =     """
                                        INSERT INTO {out_schema}.{out_table} (rast) 
                                        WITH    p AS ( SELECT id FROM {repository}.product WHERE name = '{product}' ),
                                                cat AS ( SELECT fid, fdate FROM {repository}.cat, p WHERE DATE( fdate ) = '{date}' AND pid = p.id ),
                                                aoi AS ( SELECT ST_Envelope( ST_Buffer( ST_Transform( geom, {epsg} ), {buffer} ) ) AS geom FROM {geom_schema}.{geom_table} WHERE name = '{geom_name}' )
                                                SELECT {raster_value} AS rast FROM {repository}.{product} p, aoi, cat 
                                                    WHERE p.fid = cat.fid AND ST_Intersects( rast, geom );
                                        """.format (    out_schema=args.out_schema,
                                                        out_table=args.out_table,                                  
                                                        repository=self._repo.getName(),
                                                        product=product.getName(),
                                                        date=date.strftime('%Y%m%d'),
                                                        geom_schema=args.geometry[ 'schema' ],
                                                        geom_table=args.geometry[ 'table' ],
                                                        geom_name=geom_name,
                                                        epsg=args.epsg,
                                                        buffer=args.buffer,
                                                        raster_value=raster_value )

                            # create table with tiles clipped to geometry
                            print ( 'Creating raster table: {}.{}'.format( args.out_schema, args.out_table ) )
                            if server.executeCommand( query ) is None:
                                
                                # postprocess - add spatial index + constraints
                                print ( '... OK !' )
                                if server.postProcessRasterTable( args.out_schema, args.out_table ) is None:

                                    # add optional overviews
                                    if args.overviews is not None:

                                        # create overviews (constraints added automatically)
                                        print ( 'Creating raster table overviews: {}.{}'.format( args.out_schema, args.out_table ) )
                                        for overview_factor in args.overviews:
                                            server.addRasterTableOverview( args.out_schema, args.out_table, overview_factor )
                                        print ( '... OK !' )

                                # vacuum raster tables and overviews
                                if server.vacuumTables( args.out_schema, match=args.out_table ) is not None:
                                    exit( 'terminate on vacuum error' )

        return


    def getGeometryNameList( self, server, args ):

        """
        construct query to retrieve unique date list of geometry feature names
        """

        names = []

        # format sql query
        query =     """
                    SELECT DISTINCT( lower( name ) ) FROM {geom_schema}.{geom_table}                    
                    """.format (    epsg=args.epsg,
                                    geom_schema=args.geometry[ 'schema' ],
                                    geom_table=args.geometry[ 'table' ] )

        # convert to list
        records = server.getRecords( query )
        for record in records:
            names.append( record[ 0 ] )

        return names


    def getCoverageList( self, server, product, args, geom_name ):

        """
        construct query to retrieve unique date list of raster tiles coincident with geometry
        """

        dates = []

        # format sql query
        query =     """
                    WITH aoi AS ( SELECT ST_Transform( geom, {epsg} ) AS geom FROM {geom_schema}.{geom_table} WHERE LOWER( name ) = '{geom_name}' ),
                    int AS ( SELECT fid, rast FROM {repository}.{product}, aoi WHERE ST_Intersects( rast, geom ) ) 
                    SELECT DISTINCT ( DATE( fdate ) ) FROM int INNER JOIN {repository}.cat ON int.fid = cat.fid;
                    """.format (    epsg=args.epsg,
                                    geom_schema=args.geometry[ 'schema' ],
                                    geom_table=args.geometry[ 'table' ],
                                    geom_name=geom_name,
                                    repository=self._repo.getName(),
                                    product=product.getName() )

        # get unique date records
        records = server.getRecords( query )

        # apply start and end date filter
        for record in records:
            if args.start_date is None or record[ 0 ] >= args.start_date.date():
                if args.end_date is None or record[ 0 ] <= args.end_date.date():
                    dates.append( record[ 0 ] )

        return dates


def validGeometryArgument ( arg ):

    """
    parse custom argparse *date* type 
    """
    
    try:

        # split period separated strings into attributes
        tokens = arg.split('.')
        if len( tokens ) == 2:
            return { 'schema' : tokens[ 0 ], 'table' : tokens[ 1 ] }
        else:
            return { 'schema' : tokens[ 0 ], 'table' : tokens[ 1 ], 'name' : tokens[ 2 ].lower() }
    except ValueError:
        msg = "geometry path argument ({0}) not valid! Expected format, schema.table or schema.table.name!".format(arg)
        raise argparse.ArgumentTypeError(msg)


def validDateArgument ( arg ):

    """
    parse custom argparse *date* type 
    """
    
    try:
        # parse datetime string
        return datetime.strptime( arg, "%d/%m/%Y" )
    except ValueError:
        msg = "date argument ({0}) not valid! Expected format, DD/MM/YYYY!".format(arg)
        raise argparse.ArgumentTypeError(msg)


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-aoi')
    parser.add_argument('config_path', action="store")
    parser.add_argument('geometry', type=validGeometryArgument, action="store" )

    # filter options
    parser.add_argument('-s','--start_date', type=validDateArgument, help='start date', default=None )
    parser.add_argument('-e','--end_date', type=validDateArgument, help='end date', default=None )

    # options
    parser.add_argument('--repository', nargs='+', help='repository list', default=[ 'geoeye1', 'worldview2', 'worldview3', 'worldview4' ] )
    parser.add_argument('--product', default='pan', help='product name' )
    parser.add_argument('--no_clip', help='no clip', action="store_true" )
    parser.add_argument('--overviews', default=None, nargs='+', type=int, help='overviews' )
    parser.add_argument('--epsg', default=27700, type=int, help='epsg' )
    parser.add_argument('--buffer', default=400, type=int, help='buffer distance' )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

    for platform in args.repository:

        config_file = os.path.join( args.config_path, '{platform}-gcp.yml'.format( platform=platform ) )
        manager = Manager( config_file )

        # get repository
        repo = manager.getRepository( platform )
        if repo is not None:

            # get product
            product = repo.getProduct( 'pan' )
            if product is not None:

                # create and execute extraction
                extractor = Extractor( repo )
                extractor.process ( product, args )

    return
    

# execute main
if __name__ == '__main__':
    main()

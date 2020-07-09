import os
import asyncio
import tempfile

from threading import Thread


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
        ingest image into database as postgis raster objects
        """

        # default out schema to repo
        if args.out_schema is None:
            args.out_schema = self._repo.getName()

        # 1 or more servers
        for server in self._repo.getServerList():

            # get dates of coincident rasters
            dates = self.getCoverageList( server, product, args )
            for date in dates:

                # create or replace output table
                args.out_table = '{geom_name}_{date}'.format (  geom_name=args.geometry[ 'name' ],
                                                                date=date.strftime('%Y%m%d') )

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
                                        aoi AS ( SELECT ST_Transform( geom, {epsg} ) AS geom FROM {geom_schema}.{geom_table} WHERE name = '{geom_name}' )
                                        SELECT {raster_value} AS rast FROM {repository}.{product} p, aoi, cat 
                                            WHERE p.fid = cat.fid AND ST_Intersects( rast, geom );
                                """.format (    out_schema=args.out_schema,
                                                out_table=args.out_table,                                  
                                                repository=self._repo.getName(),
                                                product=product.getName(),
                                                date=date.strftime('%Y%m%d'),
                                                geom_schema=args.geometry[ 'schema' ],
                                                geom_table=args.geometry[ 'table' ],
                                                geom_name=args.geometry[ 'name' ],
                                                epsg=args.epsg,
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

        return


    def getCoverageList( self, server, product, args ):

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
                                    geom_name=args.geometry[ 'name' ],
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

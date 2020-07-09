import os
import tempfile
import psycopg2 

from src.utility import ps
from src.utility import fs


class Server:


    def __init__( self, obj ):

        """
        constructor
        """

        self._obj = obj
        self._bin_path = 'C:\\Program Files\\PostgreSQL\\12\\bin'
        self._default_port = 5432
        return


    def getHost( self ):

        """
        get host
        """

        return self._obj[ 'host' ]


    def getDatabase( self ):

        """
        get database
        """

        return self._obj[ 'database' ]


    def getPort( self ):

        """
        get port
        """

        return self._obj[ 'port' ] if 'port' in self._obj else self._default_port


    def getUser( self ):

        """
        get user
        """

        return self._obj[ 'user' ] if 'user' in self._obj else None


    def getPassword( self ):

        """
        get password
        """

        return self._obj[ 'password' ] if 'password' in self._obj else None


    def getGdalConnectionString( self ):

        """
        get password
        """

        # construct gdal connection string
        return "PG:host={host} port={port} dbname=\'{dbname}\' user=\'{user}\' password=\'{password}\'".format(   host=self.getHost(), 
                                                                                                                    port=self.getPort(), 
                                                                                                                    dbname=self.getDatabase(),
                                                                                                                    user=self.getUser(),
                                                                                                                    password=self.getPassword() )

    def executeTransactionFromFile( self, command ):

        """
        execute transaction from file
        """

        # get psql argument list
        args = [ '-h', self.getHost(),
                 '-d', self.getDatabase() ]

        # optional user argument
        if self.getUser() is not None:
            args.append( '-U' ); args.append( self.getUser() )

        # optional password argument
        if self.getPassword() is not None:
            os.environ['PGPASSWORD'] = self.getPassword()

        # append file 
        args.append( '-f' )
        args.append( command )

        # execute sql from file using psql client
        return ps.execute( os.path.join( self._bin_path, 'psql.exe' ), args )


    def getConnection( self ):

        """
        get psycopg arguments
        """

        # create connection string for psycopg
        cfg = "dbname='{}' host='{}'".format( self.getDatabase(), self.getHost() )

        # optional user
        if self.getUser() is not None:
            cfg += " user='{}'".format( self.getUser() )

        # optional password
        if self.getPassword() is not None:
            cfg += " password='{}'".format( self.getPassword() )

        # get connection
        return psycopg2.connect( cfg )


    def getRecords( self, query ):

        """
        execute query
        """

        records = []

        # get connection
        conn = self.getConnection()
        cur = conn.cursor()
        
        try:
            # execute query
            cur.execute( query )        
            records = cur.fetchall()

        # handle exception
        except psycopg2.Error as e:

            print ( e.pgerror )

        # close connection
        conn.close()
        return records


    def executeCommand( self, command ):

        """
        execute command
        """

        error = None

        # get connection
        conn = self.getConnection()
        cur = conn.cursor()
        
        try:
            # execute query
            cur.execute( command )
            conn.commit()

        # handle exception
        except psycopg2.Error as e:

            print ( e.pgerror )
            error = e.pgerror

        # close connection
        conn.close()
        return error


    def getRecordCount( self, schema, table ):

        """
        execute query
        """

        count = None

        # get connection
        conn = self.getConnection()
        cur = conn.cursor()
        
        try:
            # execute query
            cur.execute( 'SELECT COUNT(*) FROM {schema}.{table}'.format( schema=schema, table=table ) )
            count = int ( cur.fetchone()[0] )

        # handle exception
        except psycopg2.Error as e:

            print ( e.pgerror )

        # close connection
        conn.close()
        return count


    def loadRaster( self, parameters  ):

        """
        load raster into database using raster2pgsql
        """

        # standard argument list - tile size configurable
        # args = [ '-R', '-C', '-d', '-F' ]
        args = [ '-R', '-d', '-F', '-Y' ]
        # args = [ '-d', '-F', '-Y' ]

        args.extend( [ '-t', parameters[ 'TILE_SIZE' ] ] )
        args.append( parameters[ 'PATHNAME' ] )
        args.append( '{}.{}'.format( parameters[ 'SCHEMA' ], parameters[ 'TEMP_TABLE' ] ) )

        #  execute raster2pgsql with argument list
        out, error, code = ps.execute( os.path.join( self._bin_path, 'raster2pgsql.exe' ), args )
        if code == 0:

            # create temp path            
            with tempfile.TemporaryDirectory() as tmp_path:

                # write raster2pgsql to file
                with open( os.path.join( tmp_path, 'raster2pgsql.sql' ), "wb" ) as fp:
                    fp.write( out  )

                # execute raster2pgsql commands 
                out, error, code = self.executeTransactionFromFile( os.path.join( tmp_path, 'raster2pgsql.sql' ) )
            
        return out, error, code


    def executeTemplateOperation( self, template, parameters ):

        """
        populate and execute template command buffer with parameter values 
        """

        # transpose template with actual parameter values
        for key, value in parameters.items():

            # map demarked key with value
            label = '!' + key.upper() + '!'
            template = template.replace( label, value )

        # execute transposed template from file 
        with tempfile.TemporaryDirectory() as tmp_path:
            pathname = os.path.join( tmp_path, 'script.sql' )
            out, error, code = self.executeTransactionFromFile( fs.writeFile( pathname, template ) )

        return out, error, code


    def checkColumnExists( self, schema, table, column_name ):

        """
        check column exists in specified table
        """

        # construct boolean query of information_schema
        query = """
                SELECT EXISTS (SELECT 1 
                FROM information_schema.columns 
                WHERE table_schema='{schema}' AND table_name='{table}' AND UPPER(column_name)='{column_name}' );
                """.format ( schema=schema, table=table, column_name=column_name.upper()  )

        records = self.getRecords( query )
        return records[ 0 ][ 0 ]


    def checkTableExists( self, schema, table ):

        """
        check table object exists
        """

        # construct boolean query of information_schema
        query = """
                SELECT EXISTS (SELECT 1 
                FROM information_schema.tables
                WHERE table_schema='{schema}' AND table_name='{table}' );
                """.format ( schema=schema, table=table )

        records = self.getRecords( query )
        return records[ 0 ][ 0 ]


    def createOrReplaceTable( self, schema, table, fields ):

        """
        create or replace table 
        """

        # drop + create table
        self.dropTable( schema, table )
        return self.createTable( schema, table, fields )


    def dropTable( self, schema, table ):

        """
        drop table
        """

        # drop table if exists
        query = """
                DROP TABLE IF EXISTS {schema}.{table}
                """.format ( schema=schema, table=table )

        return self.executeCommand( query )


    def createTable( self, schema, table, fields ):

        """
        create table with fields arg
        """

        # create table with fields defined by arguments
        query = """
                CREATE TABLE IF NOT EXISTS {schema}.{table}
                """.format ( schema=schema, table=table )

        query += " ( id SERIAL PRIMARY KEY "
        for k, v in fields.items():
            query += ", {name} {type}".format( name=k, type=v )
        query += " )"

        # execute create table command
        return self.executeCommand( query )


    def postProcessRasterTable( self, schema, table, column_name='rast', constraints=None ):

        """
        drop and recreate index + constraints
        """

        # recreate constraints
        self.dropRasterConstraints( schema, table, column_name, constraints )
        self.addRasterConstraints( schema, table, column_name, constraints )

        # recreate spatial index
        self.dropRasterSpatialIndex( schema, table, column_name )
        self.addRasterSpatialIndex( schema, table, column_name )

        return


    def addRasterConstraints( self, schema, table, column_name='rast', constraints=None ):

        """
        add constraints to table containing raster objects
        """

        # apply default constraint settings
        if constraints is None:

            # form and execute query
            query = """
                    SELECT AddRasterConstraints( '{schema}'::name, '{table}'::name, '{column_name}'::name );
                    """.format ( schema=schema, table=table, column_name=column_name )
            
            error = self.executeCommand( query )

        return error


    def dropRasterConstraints( self, schema, table, column_name='rast', constraints=None ):

        """
        drop constraints on raster table 
        """

        # apply default constraint settings
        if constraints is None:

            # form and execute query
            query = """
                    SELECT DropRasterConstraints( '{schema}'::name, '{table}'::name, '{column_name}'::name );
                    """.format ( schema=schema, table=table, column_name=column_name )
            
            error = self.executeCommand( query )

        return error


    def addRasterSpatialIndex( self, schema, table, column_name='rast' ):

        """
        add spatial index to table containing raster objects
        """

        # create index based on raster convex hull
        query = """
                CREATE INDEX IF NOT EXISTS {schema}_{table}_{column_name}_st_convexhull_idx ON {schema}.{table}
                    USING gist( ST_ConvexHull( {column_name} ) );
                """.format ( schema=schema, table=table, column_name=column_name )

        return self.executeCommand( query )


    def dropRasterSpatialIndex( self, schema, table, column_name='rast' ):

        """
        drop spatial index on table
        """

        # drop existing index if exists
        query = """
                DROP INDEX IF EXISTS {schema}_{table}_{column_name}_st_convexhull_idx;
                """.format ( schema=schema, table=table, column_name=column_name )
        
        return self.executeCommand( query )


    def addRasterTableOverview( self, schema, table, overview_factor, column_name='rast', algo='NearestNeighbor' ):

        """
        drop spatial index on table
        """

        # drop overview table if exists
        overview_table = 'o_{overview_factor}_{table}'.format(  overview_factor=overview_factor,
                                                                table=table )

        self.dropTable( schema, overview_table )

        # create overview
        error = self.executeCommand(    """
                                        SELECT ST_CreateOverview ( '{schema}.{table}'::regclass, '{column_name}', {overview_factor}, '{algo}' );
                                        """.format (    schema=schema, 
                                                        table=table, 
                                                        column_name=column_name, 
                                                        overview_factor=overview_factor,
                                                        algo=algo ) )
        if error is None:

            # add primary key id column to overview table - required by qgis
            error = self.executeCommand(    """
                                            ALTER TABLE {schema}.{overview_table} ADD COLUMN id SERIAL PRIMARY KEY;
                                            """.format ( schema=schema, overview_table=overview_table ) )

        return error

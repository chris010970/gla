import os
import tempfile
import psycopg2 

from src.utility import ps


class Server:


    def __init__( self, obj ):

        """
        constructor
        """

        self._obj = obj
        self._bin_path = 'C:\\Program Files\\PostgreSQL\\12\\bin'
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


    def executeQuery( self, query ):

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


    def loadRaster( self, parameters  ):

        """
        load raster into database using raster2pgsql
        """

        # standard argument list - tile size configurable
        args = [ '-R', '-C', '-d', '-F' ]

        args.extend( [ '-t', parameters[ 'TILE_SIZE' ] ] )
        args.append( parameters[ 'PATHNAME' ] )
        args.append( '{}.{}'.format( parameters[ 'SCHEMA' ], parameters[ 'TEMP_TABLE' ] ) )

        #  execute raster2pgsql with argument list
        out, error, code = ps.execute( os.path.join( self._bin_path, 'raster2pgsql.exe' ), args )
        if code == 0:

            # create temp path            
            with tempfile.TemporaryDirectory() as tmp_path:

                # write raster2pgsql to file
                with open( os.path.join( tmp_path, 'raster2pgsql.sql' ), "w" ) as fp:
                    fp.write( out.decode( 'utf-8')  )

                # execute raster2pgsql commands 
                out, error, code = self.executeTransactionFromFile( os.path.join( tmp_path, 'raster2pgsql.sql' ) )
            
        return out, error, code

import os
import tempfile

from server import Server
from product import Product

from src.utility import parser


class Repository:


    def __init__( self, obj ):

        """
        constructor
        """

        self._obj = obj

        # create list of product objects
        self._products = []
        for item in self._obj[ 'products' ]:
            self._products.append( Product ( item ) )

        # create list of servers
        self._servers = []        
        for item in self._obj[ 'servers' ]:
            self._servers.append( Server ( item ) )

        # create list of templates
        self._templates = {}       
        for key, value in self._obj[ 'templates' ].items():    

            # read sql template file
            with open ( str( value ) ) as fp:
                command = fp.read()

            # append buffer and id 
            self._templates[ key ] = command

        return
        

    def getName( self ):

        """
        get repository name
        """

        return self._obj[ 'name' ]


    def getPath( self ):

        """
        get repository root path
        """

        return self._obj[ 'path' ]


    def getKeywords( self ):

        """
        get keywords
        """

        return self._obj[ 'keywords' ]


    def getDescription( self ):

        """
        get description
        """

        return self._obj[ 'description' ]


    def getProductNameList( self ):

        """
        get product name list
        """

        names = []

        # create list of product names
        for item in self._products:
            names.append( item.getName() )
    
        return names


    def getProduct( self, name ):

        """
        get product 
        """

        product = None

        # search repository list for name match
        for item in self._products:
            if item.getName() == name:
                product = item
                break
    
        return product


    def ingestImage( self, pathname, product ):

        """
        ingest image into database as postgis raster objects
        """
       
        def transposeTokens( command, parameters  ):

            """
            transpose parameter values into template sql
            """

            for key, value in parameters.items():

                # map demarked key with value
                label = '!' + key.upper() + '!'
                command = command.replace( label, value )
 
            return command

            
        # for each server                
        code = 0
        for server in self._servers:

            # raster already in database                
            if not self.isRegistered( server, pathname ):

                # create temp path            
                with tempfile.TemporaryDirectory() as tmp_path:

                    # compile list of product-specific parameter values to specialise sql scripts
                    parameters = self.getParameterList( pathname, product, server )
                    parameters[ 'TEMP_TABLE' ] = os.path.basename( tmp_path )

                    # execute preprocess sql commands
                    with open( os.path.join( tmp_path, 'preprocess.sql' ), "w" ) as fp:
                        fp.write( transposeTokens( self._templates[ 'preprocess' ], parameters ) )

                    # execute batch script and check for errors                        
                    out, error, code = server.executeTransactionFromFile( os.path.join( tmp_path, 'preprocess.sql' ) )

                    # load raster as tiles into database table
                    out, error, code = server.loadRaster( parameters )

                    # execute preprocess sql commands
                    with open( os.path.join( tmp_path, 'postprocess.sql' ), "w" ) as fp:
                        fp.write( transposeTokens( self._templates[ 'postprocess' ], parameters ) )

                    # execute batch script and check for errors                        
                    out, error, code = server.executeTransactionFromFile( os.path.join( tmp_path, 'postprocess.sql' ) )

        return


    def isRegistered(self, server, pathname ):

        """
        Placeholder
        """

        # query pathname in catalog table
        records = server.executeQuery( "SELECT pathname FROM {repository}.cat WHERE pathname = '{pathname}'".format(    repository=self.getName(), 
                                                                                                                        pathname=pathname ) )
        if len( records ) == 1:
            return True

        return False


    def getParameterList( self, pathname, product, server ):

        """
        compile parameter values for transaction into dict 
        """

        parameters = {}

        # get parameters from pathname
        parameters[ 'PATHNAME' ] = pathname
        parameters[ 'PATH' ] = os.path.dirname( pathname )

        dt = parser.getDateTime( pathname )
        parameters[ 'TIMESTAMP' ] = dt.strftime("%Y-%m-%d %H:%M:%S")

        # get schema and product table names
        parameters[ 'DATABASE' ] = server.getDatabase()
        parameters[ 'SCHEMA' ] = self.getName()
        parameters[ 'PRODUCT' ] = product.getName()

        # get records for product and measurement ancillary tables
        parameters[ 'PRODUCT_DATA' ] = product.getSqlRecord()
        parameters[ 'MEASUREMENT_DATA' ] = product.getMeasurementSqlRecords()

        # misc
        parameters[ 'TILE_SIZE' ] = product.getTileSize()

        return parameters


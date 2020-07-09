import os
import yaml

from src.database.objects.server import Server
from src.database.objects.product import Product

from src.utility import fs
from src.utility.gsclient import GsClient


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

            # load config parameters from file
            with open( item[ 'config-file' ], 'r' ) as f:
                self._servers.append( Server( yaml.safe_load( f ) ) )

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


    def getServerList( self ):

        """
        get server list
        """

        return self._servers


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


    def getTemplate( self, operation ):

        """
        get template
        """

        return self._templates[ operation ] if operation in self._templates else None


    def getProductImageList( self, product, path=None ):

        """
        get image list
        """

        images = None

        # use default path
        if path is None:
            path = self._obj[ 'path' ] 

        # attempt to parse path as gcs uri
        bucket, prefix = GsClient.parseUri( path )
        if bucket is not None:

            # add credential variables to environment
            if self._obj[ 'credentials' ] is not None:
                GsClient.updateCredentials( self._obj[ 'credentials' ] )

            # create client and get image list                
            client = GsClient( bucket )
            images = client.getImageUriList( prefix, pattern=product.getPattern() )

        else:

            # get local file list matched with regexp
            images = fs.getPathNameList( path, product.getPattern() )

        return images

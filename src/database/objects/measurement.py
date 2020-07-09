import os

class Measurement:


    def __init__( self, obj ):

        """
        constructor
        """

        self._obj = obj 
        return

        
    def getName( self ):

        """
        get measurement name
        """

        return self._obj[ 'name' ]


    def getDescription( self ):

        """
        get description
        """

        return self._obj[ 'description' ] if 'description' in self._obj else ''


    def getKeywords( self ):

        """
        get keywords
        """

        return self._obj[ 'keywords' ] if 'keywords' in self._obj else ''


    def getUnits( self ):

        """
        get units
        """

        return self._obj[ 'units' ] if 'units' in self._obj else ''

from src.database.objects.measurement import Measurement

class Product:


    def __init__( self, obj ):

        """
        constructor
        """

        # initialise member variables
        self._obj = obj
        
        # create product objects from xml schema
        self._measurements = []
        for item in self._obj[ 'measurements' ]:
            self._measurements.append ( Measurement( item ) )

        return
        

    def getName( self ):

        """
        get product name
        """

        return self._obj[ 'name' ]


    def getDescription( self ):

        """
        get description
        """

        return self._obj[ 'description' ]


    def getKeywords( self ):

        """
        get keywords
        """

        return self._obj[ 'keywords' ]


    def getPattern( self ):

        """
        get source
        """

        # get pattern regexp
        return self._obj[ 'pattern' ]


    def getTileSize( self ):

        """
        get tile size
        """

        return self._obj[ 'tile_size' ] if 'tile_size' in self._obj else '512x512'  # default gdal tile size


    def getMeasurementNameList( self ):

        """
        get measurement name list
        """

        # create list of product names
        names = []

        for item in self._measurements:
            names.append( item.getName() )
    
        return names


    def getSqlRecord( self ):

        """
        get sql record string
        """

        # format product identify as sql record
        return " ( '{name}', '{description}', '{keywords}' ) ".format ( name=self.getName(), 
                                                                        description=self.getDescription(),
                                                                        keywords=self.getKeywords() ) 
        

    def getMeasurementSqlRecords( self ):
        
        """
        get array of measurement value sql records
        """

        # feed measurement values into sql command set
        sql = ''
        for idx, item in enumerate ( self._measurements ): 

            # add measurement record to string
            sql +=   " ( '{name}', '{description}', '{keywords}', '{units}' ) ".format ( name=item.getName(),
                                                                                        description=item.getDescription(),
                                                                                        keywords=item.getKeywords(),
                                                                                        units=item.getUnits() )                                                                                                            
            # mind the last comma
            if idx < len ( self._measurements ) - 1:
                sql += ','

        return sql


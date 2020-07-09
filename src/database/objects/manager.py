import os
import gdal
import yaml

from src.database.objects.repository import Repository


class Manager:

    """
    constructor
    """

    def __init__( self, pathname ):

        # initialise member variables
        self._repositories = []

        # load config parameters from file
        with open(  pathname, 'r' ) as f:
            self._config = yaml.safe_load( f )

        if not isinstance( self._config[ 'repository' ], list ):
            self._config[ 'repository' ] = [ self._config[ 'repository' ] ]

        # create repository objects
        for item in self._config[ 'repository' ]:
            self._repositories.append( Repository( item ) )

        return


    def getRepositoryNameList( self ):

        """
        get list of repository names
        """

        names = []

        # create list of repository names
        for item in self._repositories:
            names.append( item.getName() )
    
        return names


    def getRepository( self, name ):

        """
        retrieve repository object based on name
        """

        repository = None

        # search repository list for name match
        for item in self._repositories:
            if item.getName() in name:
                repository = item
                break
    
        return repository


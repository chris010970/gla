import os
import argparse
from datetime import datetime

from extractor import Extractor
from src.database.objects.manager import Manager


def validGeometryArgument ( arg ):

    """
    parse custom argparse *date* type 
    """
    
    try:

        # split period separated strings into attributes
        tokens = arg.split('.')
        if len( tokens ) == 2:
            return { 'schema' : tokens[ 0 ], 'table' : tokens[ 1 ], 'name' : tokens[ 1 ].lower() }
        else:
            return { 'schema' : tokens[ 0 ], 'table' : tokens[ 1 ], 'name' : tokens[ 2 ].lower() }
    except ValueError:
        msg = "geometry path argument ({0}) not valid! Expected format, schema.table.name!".format(arg)
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
    parser.add_argument('config_file', action="store")
    parser.add_argument('repository', action="store")
    parser.add_argument('product', action="store")
    parser.add_argument('geometry', type=validGeometryArgument, action="store" )

    # filter options
    parser.add_argument('-s','--start_date', type=validDateArgument, help='start date', default=None )
    parser.add_argument('-e','--end_date', type=validDateArgument, help='end date', default=None )

    # options
    parser.add_argument('-o','--out_schema', help='out schema', default=None )
    parser.add_argument('-n','--no_clip', help='no clip', action="store_true" )
    parser.add_argument('-v','--overviews', default=None, nargs='+', type=int, help='overviews' )
    parser.add_argument('-p','--epsg', default=27700, type=int, help='epsg' )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()
    manager = Manager( args.config_file )

    # get repository
    repo = manager.getRepository( args.repository )
    if repo is not None:

        # get product
        product = repo.getProduct( args.product )
        if product is not None:

            # create and execute extraction
            extractor = Extractor( repo )
            extractor.process ( product, args )

    return
    

# execute main
if __name__ == '__main__':
    main()

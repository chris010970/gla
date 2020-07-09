import os
import argparse

from ingester import Ingester


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-aoi')
    parser.add_argument('geometry_file', action="store")
    parser.add_argument('config_file', action="store")

    # naming options
    parser.add_argument('-schema', default='aoi', action="store" )
    parser.add_argument('-table', default=None, action="store" )

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments and validate
    args = parseArguments()
    if os.path.exists( args.geometry_file ) and os.path.exists( args.config_file ):

        # setup defaults
        if args.table is None:
            args.table = os.path.splitext( os.path.basename( args.geometry_file ) )[ 0 ]

        # create instance and execute ingestion    
        obj = Ingester( args.config_file )
        obj.process( args )


    return
    

# execute main
if __name__ == '__main__':
    main()

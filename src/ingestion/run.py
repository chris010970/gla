import os
import glob
import argparse

from manager import Manager

def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-ard')
    parser.add_argument('config', action="store") # 'C:\\Users\\Chris.Williams\Desktop\\ingest.yml'
    parser.add_argument('repository', action="store")
    parser.add_argument('product', action="store")

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()
    manager = Manager( args.config )

    # get repository
    repo = manager.getRepository( args.repository )
    if repo is not None:

        # get product
        product = repo.getProduct( args.product )
        if product is not None:

            # ingest images
            repo.ingestImages( product ) 
            
    return
    

# execute main
if __name__ == '__main__':
    main()

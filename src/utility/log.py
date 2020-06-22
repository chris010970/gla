import os
import logging


def getFileLogger( name, pathname ):

    """
    initialise logger
    """

    # create logger
    logger = logging.getLogger( name )
    logger.setLevel(logging.DEBUG)
        
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if not os.path.exists ( os.path.dirname( pathname ) ):
        os.makedirs( os.path.dirname( pathname ) )

    # create file handler and set level to debug
    fh = logging.FileHandler( pathname )
    fh.setLevel(logging.DEBUG)

    # set formatters
    fh.setFormatter(formatter)
    logger.addHandler(fh)    

    return logger

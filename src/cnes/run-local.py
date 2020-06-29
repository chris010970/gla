import os
import glob
import argparse

# local imports
from spot import Spot
from pleiades import Pleiades
from dataset import Dataset


def parseArguments(args=None):

    """
    parse command line arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='process-ard')
    parser.add_argument('scene', action="store")

    # options
    parser.add_argument('-roi', nargs=4,default=None, action="store", type=float )
    parser.add_argument('-dem_path', default=None, action="store" )
    parser.add_argument('-geoid_pathname', default=None, action="store" )
    
    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

    # assume single scene - else parse directory for dataset zip files
    scenes = [ args.scene ]
    if os.path.isdir( args.scene ):
        scenes = glob.glob( os.path.join( args.data_path, '**/*.zip' ), recursive=True )

    for scene in scenes:

        if os.path.exists( scene ) and scene.endswith('.zip'):

            # get dataset id
            _name = Dataset.getClassName( scene )
            if _name is not None:

                # create object and process ard
                _class = globals()[ _name ]            
                obj = _class (  scene,
                                dem_path=args.dem_path,
                                geoid_pathname=args.geoid_pathname,
                                pan_method='bayes',
                                roi=args.roi )

                obj.processToArd()

    return
    

# execute main
if __name__ == '__main__':
    main()


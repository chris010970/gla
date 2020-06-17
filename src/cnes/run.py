import os
import osr
import glob 
import gdal
import shutil
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
    parser = argparse.ArgumentParser(description='process ard')
    parser.add_argument('scene', action="store")
    parser.add_argument('-roi', nargs=4,default=None, action="store", type=float )
    
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

            # construct paths
            raw_path = os.path.dirname( scene )
            ard_path = raw_path.replace( 'raw', 'ard' )

            # get dataset id
            _id = Dataset.getId( scene )
            if _id is not None:

                if _id == 'PHR': 

                    # create spot object and process ard
                    obj = Pleiades( scene,
                                    dem_path='D:\\data\\ancillary\\srtm',
                                    geoid_pathname='D:\\data\\ancillary\\geoid\\egm96.grd',
                                    pan_method='bayes',
                                    roi=args.roi )

                    obj.processToArd( ard_path )

                elif _id == 'SPOT': 

                    # create spot object and process ard
                    obj = Spot( scene,
                                dem_path='D:\\data\\ancillary\\srtm',
                                geoid_pathname='D:\\data\\ancillary\\geoid\\egm96.grd',
                                pan_method='bayes',
                                roi=args.roi )

                    obj.processToArd( ard_path )



    return
    

# execute main
if __name__ == '__main__':
    main()



"""

    files = glob.glob( os.path.join( args.data_path, '*.zip' ) )

    root_path = 'C:\\Users\\Chris.Williams\\Documents\\Data\\gla\\ssgp\\raw'

    spot = Spot()
    for f in files:

        path = os.path.join( root_path, spot.getSubPath( f  ) )

        if not os.path.exists( path ):
            os.makedirs( path )

        print( '{} -> {}'.format( f, path ) )
        shutil.move( f, path )
"""



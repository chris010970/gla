import os
import gdal
import yaml

from loader import Loader
from repository import Repository

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


    def sliceDataset( self, pathname, out_path, chunk_size=4096 ):

        """
        get list of repository names
        """

        # open dataset
        src_ds = gdal.Open( pathname, gdal.GA_ReadOnly )
        if src_ds is not None:

            # create out path
            if not os.path.exists( out_path ):
                os.makedirs( out_path )

            # step through tile locations
            for y1 in range ( 0, src_ds.RasterYSize, chunk_size ):
                
                for x1 in range ( 0, src_ds.RasterXSize, chunk_size ):

                    # compute rightside coordinates
                    x2 = min( ( x1 + chunk_size ), src_ds.RasterXSize )
                    y2 = min( ( y1 + chunk_size ), src_ds.RasterYSize )

                    # create vrt pathname
                    vrt_pathname = os.path.join( out_path, 'slice_{}_{}_{}_{}.vrt'.format( x1, y1, x2-x1, y2-y1 ) )
                    gdal.Translate( vrt_pathname, src_ds, bandList=[1,2,3], srcWin=[ x1, y1, (x2-x1), (y2-y1) ] )

            # close dataset
            src_ds = None

        return






manager = Manager( 'C:\\Users\\Chris.Williams\Desktop\\ingest.yml' )

#manager.sliceDataset( 'D:\\data\\scratch\\ssgp\\raw\\38012\\20190225_111340\\tmp1\pan\\IMG_PHR1A_PAN_201902251113405_ORT_3952991101-2_MOSAIC_CAL_ROI.TIF',
#                        'D:\\data\\scratch\\vrt_out\\' )

names =  manager.getRepositoryNameList()
repo = manager.getRepository( names[ 0 ] )

print ( repo.getPath() )
names = repo.getProductNameList()

product = repo.getProduct( names[ 0 ] )
print ( product.getMeasurementNameList() )

product = repo.getProduct( names[ 0 ] )
print ( product.getMeasurementNameList() )

#img_pathname = 'D:\\data\\vrt\\38012\\20190225_111340\\slice_0_4096_4096_4096.vrt'
#repo.ingestImage( img_pathname, product )


loader = Loader( repo, "D:\\data\\vrt" )

#img_pathname = "D:\\data\\scratch\\ssgp\\raw\\38012\\20190225_111340\\tmp1\pan\\IMG_PHR1A_PAN_201902251113405_ORT_3952991101-2_MOSAIC_CAL_ROI.TIF"
img_pathname = "d:\\data\\scratch\\ssgp\\raw\\38012\\20180212_112109\\IMG_PHR1A_PAN_201802121121090_ORT_3613627101-2_R1C1_CAL_ROI.TIF"

loader.process( img_pathname, product )

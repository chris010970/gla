import os

# local imports
from dataset import Dataset
from src.utility import ps

class Spot ( Dataset ):

    def __init__(   self, 
                    scene, 
                    **kwargs ):

        """
        constructor
        """

        # initialise base object
        super().__init__( scene, **kwargs )

        # define norad ids
        self._norad_id = {  
                            'SPOT5' : 27421,
                            'SPOT6' : 38755,
                            'SPOT7' : 40053 
                        }

        # get platform and tle
        platform = self.getPlatform( scene, r'_DS_SPOT[\d]_' )
        self._platform = platform[ platform.find( 'SPOT') :  ]

        self._tle = self._norad_id [ self._platform ]

        return


    def processToArd( self ):

        """
        manage processing from raw dataset to ard
        """

        # create tmp working directory
        root_path = os.path.join( os.path.dirname( self._scene ), 'tmp' )

        path = os.path.join( root_path, 'scene' )
        if not os.path.exists( path ):
            os.makedirs( path )

        # extract dataset to tmp path
        out, err, code = ps.extractZip( self._scene, path )
        self._log_file.write( '{}\n{}'.format ( out, err ) )

        if code == 0:
        
            # get image list and corresponding srtm tiles
            images = self.getImageLists( path ); 
            self.getSrtmTiles( images[ 'P' ] )

            # panchromatic and multispectral image sets
            mosaic = {}
            for _id in self._image_ids:

                # generate calibration images
                out_path = os.path.join( root_path, 'cal/{}'.format( _id ) )
                cal_images = self.getCalibratedImages( images[ _id ], out_path ) 

                # create mosaic 
                out_path = os.path.join( root_path, 'mosaic/{}'.format( _id ) )
                mosaic[ _id ] = self.getTileFusionImages( cal_images, out_path )

                # optionally grab roi into mosaic
                if self._roi is not None:                    
                    out_path = os.path.join( root_path, 'roi/{}'.format( _id ) )
                    mosaic[ _id ] = self.getRoiImage( mosaic[ _id ], out_path )


            # superimpose multispectral image on panchromatic geometry
            out_path = os.path.join( root_path, 'roi_compress/MS' )
            mosaic_ms_compress = self.convertImage( mosaic[ 'MS' ], out_path, options=['COMPRESS=DEFLATE', 'TILED=YES' ] )
             
            out_path = os.path.join( root_path, 'pan' )
            mosaic[ 'MS' ] = self.getSuperimposedImage( mosaic, out_path )

            # generate pansharpened image
            out_path = os.path.join( root_path, 'pan' )
            pan_image = self.getPansharpenImage( mosaic, out_path )


        # return pansharpened image and compressed multispectral mosaic + geom files
        return [ pan_image, pan_image.replace( '.TIF', '.geom' ), mosaic_ms_compress, mosaic_ms_compress.replace( '.TIF', '.geom' ) ]

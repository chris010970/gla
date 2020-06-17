import os
import re
import glob
from datetime import datetime

# otb imports
import otbApplication

# local imports
from src.utility import parser
from src.utility import ps

class Spot:

    def __init__(   self, 
                    scene, 
                    dem_path='D:\\data\\ancillary\\srtm',
                    geoid_pathname='D:\\data\\ancillary\\geoid\\egm96.grd',
                    ram=2048 ):

        '''
        constructor
        '''

        # define norad ids
        self._norad_id = {  
                            'SPOT5' : 27421,
                            'SPOT6' : 38755,
                            'SPOT7' : 40053 
                        }

        # multispectral / panchromatic
        self._image_ids = [ 'MS', 'P' ]

        # copy arguments
        self._ram = ram
        self._scene = scene
        self._dem_path = dem_path
        self._geoid_pathname = geoid_pathname

        # get attributes
        self._platform = self.getPlatform( scene )
        self._tle = self._norad_id [ self._platform ]
        self._datetime = self.getDateTimeString( scene )

        return


    def processToArd( self, ard_path ):

        '''
        manage processing from raw dataset to ard
        '''

        # create tmp working directory
        root_path = os.path.join( os.path.dirname( self._scene ), 'tmp' )

        path = os.path.join( root_path, 'scene' )
        if not os.path.exists( path ):
            os.makedirs( path )

        # extract dataset to tmp path
        # out, err, code = ps.extractZip( self._scene, path )
        code = 0
        if code == 0:
        
            # get image list and corresponding srtm tiles
            images = self.getImageLists( path ); 
            self.getSrtmTiles( images[ 'P' ] )

            # process calibration
            cal_images = {}
            for _id in self._image_ids:

                # create calibration path
                out_path = os.path.join( root_path, 'cal/{}'.format( _id ) )
                if not os.path.exists( out_path ):
                    os.makedirs( out_path )

                # generate calibration images
                cal_images[ _id ] = self.getCalibratedImages( images[ _id ], out_path, milli=True ) 

            # create pansharpen path
            out_path = os.path.join( root_path, 'pan' )
            if not os.path.exists( out_path ):
                os.makedirs( out_path )

            # generate pansharpened images
            pan_images = self.getPansharpenImages( cal_images, out_path ) 

            # generate orthorectificed pansharpened images
            if not os.path.exists( ard_path ):
                os.makedirs( ard_path )

            ortho_images = self.getOrthorectifiedImages( pan_images, ard_path ) 
            ard_images = self.getContrastEnhancedImages( ortho_images )


        return ard_images


    def getPlatform( self, scene ):

        '''
        retrieve satellite name and tle from pathname
        '''

        platform = None

        # identify platform name
        filename = os.path.basename( scene )
        m = re.search( '_SPOT[0-9]{1}_', filename )
        if m:
            platform =  str(m.group(0) ).strip( '_' )

        return platform


    def getSubPath( self ):

        '''
        retrieve satellite name and tle from pathname
        '''

        # construct unique tle / datetime folder name
        return os.path.join( self._tle, self._datetime )


    def getDateTimeString( self, scene ):

        '''
        retrieve scene acquisition datetime from pathname
        '''

        return self.getDateTime( scene ).strftime( '%Y%m%d_%H%M%S' )


    def getDateTime( self, scene ):

        '''
        retrieve scene acquisition datetime from pathname
        '''

        dt = None

        # identify satellite name
        filename = os.path.basename( scene )
        m = re.search( '_[0-9]{15}_', filename )
        if m:

            # strip name and retrieve tle from dict
            value =  str(m.group(0) ).strip( '_' )
            dt = datetime.strptime( str( value ), '%Y%m%d%H%M%S%f')
            
        return dt


    def getTile( self, pathname ):

        '''
        get tile code from filename
        '''

        tile = None

        # identify tile code
        if os.path.splitext( pathname )[1].upper() == '.TIF':

            # apply regexp
            m = re.search( '_R[0-9]{1}C[0-9]{1}_', os.path.basename( pathname ) )
            if m:
                tile =  str(m.group(0) ).strip( '_' )
            
        return tile


    def getImageLists( self, path ):

        '''
        get pathnames of multispectral and panchromatic images in dataset
        '''

        # get dataset image lists
        images = {}
        for _id in self._image_ids:

            image_path = os.path.join( path, '**/IMG_{platform}_{id}_*/IMG_{platform}_{id}_*.TIF'.format ( platform=self._platform, id=_id ) )

            # glob search path and sort lists
            images[ _id ] = glob.glob( image_path, recursive=True )
            images[ _id ].sort()

        return images


    def getSrtmTiles( self, images ):

        '''
        download srtm tiles overlapping image list
        '''

        # create app and populate parameter values
        app = otbApplication.Registry.CreateApplication('DownloadSRTMTiles')

        app.SetParameterStringList( 'il', images )
        app.SetParameterString( 'tiledir', self._dem_path )

        # execute download
        app.ExecuteAndWriteOutput()
        return


    def getCalibratedImages( self, images, out_path, level='toa', milli=False ):

        '''
        generate optical calibration images
        '''

        # create application
        app = otbApplication.Registry.CreateApplication('OpticalCalibration')

        out_images = []
        for image in images:

            # check out pathname exists
            out_pathname = os.path.join( out_path, os.path.basename( image ).replace( '.TIF', '_CAL.TIF' ) )
            if not os.path.exists( out_pathname ):

                # initialise arguments
                app.SetParameterString('in', image )
                app.SetParameterString('level', level )            
                app.SetParameterString('out', out_pathname )
                app.SetParameterString('ram', str( self._ram ) )

                # output to 0 -> 1000 16bit rather than 0 -> 1.0 32bit float
                app.SetParameterString('milli', str( milli ) )            
                if milli:
                    app.SetParameterOutputImagePixelType('out', otbApplication.ImagePixelType_uint16 )

                # execute and write products
                app.ExecuteAndWriteOutput()

            # add to list
            out_images.append( out_pathname )

        return out_images


    def getPansharpenImages( self, cal_images, out_path ):

        '''
        generate pansharpened images
        '''

        def getMultispectralImage( tile ):

            '''
            find multispectral image matching panchromatic image
            '''

            match = None
            for image in cal_images[ 'MS' ]:

                # find image with matching tile substr
                if self.getTile( image ) == tile:
                    match = image
                    break

            return match


        out_images = []

        # iterate through panchromatic image list
        app = otbApplication.Registry.CreateApplication('BundleToPerfectSensor')
        for p_image in cal_images[ 'P' ]:

            # get matching multispectral image
            ms_image = getMultispectralImage( self.getTile( p_image ) )
            if ms_image is not None:
                                
                # create output pathname
                out_pathname = os.path.join( out_path, os.path.basename( ms_image ).replace( '_MS_', '_PAN_' ) )
                if not os.path.exists( out_pathname ):

                    # initialise parameters
                    app.SetParameterString('inp', p_image )
                    app.SetParameterString('inxs', ms_image )
                    app.SetParameterString('out', out_pathname )
                    app.SetParameterString('ram', str( self._ram ) )

                    # configure elevation parameters
                    app.SetParameterString('elev.dem', self._dem_path )
                    app.SetParameterString('elev.geoid', self._geoid_pathname )

                    # generate pansharpen images
                    app.ExecuteAndWriteOutput()

                # add to list
                out_images.append( out_pathname )


        return out_images


    def getOrthorectifiedImages( self, pan_images, out_path ):

        '''
        generate pansharpened orthorectified images
        '''

        out_images = []

        # iterate through panchromatic image list
        app = otbApplication.Registry.CreateApplication('OrthoRectification')
        for image in pan_images:

            # create output pathname
            out_pathname = os.path.join( out_path, os.path.basename( image ).replace( '_PAN_', '_PAN_ORTHO_' ) )
            if not os.path.exists( out_pathname ):

                # initialise parameters
                app.SetParameterString('io.in', image )
                app.SetParameterString('io.out', out_pathname )
                #app.SetParameterString('opt.ram', self._ram )

                # configure elevation parameters
                app.SetParameterString('elev.dem', self._dem_path )
                app.SetParameterString('elev.geoid', self._geoid_pathname )

                # configure projection parameters
                #app.SetParameterString('map', 'epsg' )
                #app.SetParameterString('map.epsg', 27700 )

                # generate pansharpen images
                app.ExecuteAndWriteOutput()

            # add to list
            out_images.append( out_pathname )

        return out_images


    def getContrastEnhancedImages( self, images, out_path=None, hfact=2.0 ):

        '''
        generate pansharpened orthorectified images
        '''

        out_images = []

        # iterate through panchromatic image list
        app = otbApplication.Registry.CreateApplication('ContrastEnhancement')
        for image in images:

            if out_path == None:
                out_path = os.path.basename( image )

            # create output pathname
            out_pathname = os.path.join( out_path, os.path.basename( image ).replace( '.TIF', '_CE.TIF' ) )
            if not os.path.exists( out_pathname ):

                # initialise parameters
                app.SetParameterString('in', image )
                app.SetParameterString('out', out_pathname )
                app.SetParameterOutputImagePixelType('out', otbApplication.ImagePixelType_uint8 )

                app.SetParameterFloat('nodata', 0.0)
                app.SetParameterInt('bins', 256)
                app.SetParameterString('mode','lum')

                # local enhancement parameters
                app.SetParameterInt('spatial.local.w', 500)
                app.SetParameterInt('spatial.local.h', 500)
                app.SetParameterFloat('hfact', hfact)

                # generate pansharpen images
                app.ExecuteAndWriteOutput()

            # add to list
            out_images.append( out_pathname )

        return out_images



#pathname = 'C:\\Users\\Chris.Williams\\Desktop\SPOT\\UKSA_SPOT288_SO18034610-87-01_DS_SPOT7_201809271058114_FR1_FR1_SV1_SV1_W001N52_02845.zip'
#obj = Spot()
#print ( obj.getSubPath( pathname ) )


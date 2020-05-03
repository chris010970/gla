import os
import re
import cv2
import glob
import argparse 
import subprocess
import numpy as np

from osgeo import gdal
from datetime import datetime

# execute sub-process
def execute( name, arguments ):

    p = subprocess.Popen( [name] + arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = p.communicate()
    code = p.poll()

    return out, err, code


def getBandStatistics( pathname ):

    """
    generate pansharpened multispectral images using panchromatic band
    """

    stats = []

    # open image
    ds = gdal.Open( pathname, gdal.GA_ReadOnly )
    mask = np.array( ds.GetRasterBand(ds.RasterCount).ReadAsArray() )

    # check empty image
    if np.count_nonzero ( mask ) > 0 :
    
        for idx in range( 1, ds.RasterCount ):

            # read data and compute stats
            data = np.array( ds.GetRasterBand(idx).ReadAsArray() )
            p2, p98 = np.percentile( data[ mask != 0 ], (2, 98) )

            stats.append ( {    'idx' : idx,
                                'mean' : np.mean( data[ mask != 0 ] ),
                                'std' : np.std( data[ mask != 0 ] ),
                                'p2' : p2,
                                'p98' : p98
                                } )

    return ds, stats


def getOtbPansharpenedImage( pan, ms, out_path ):

    """
    generate pansharpened multispectral images using panchromatic band
    """

    # construct filename
    out_pathname = os.path.join( out_path, os.path.basename( ms ).replace( '.TIF', '_PS.TIF' ) )
    if not os.path.exists ( out_pathname ):

        # execute gdal script if pansharpened image not created
        args = [    '-inp', pan,
                    '-inxs', ms,
                    '-out', out_pathname, 'uint16',
                    '-ram', '1024' ]

        out, err, code = execute ( 'C:\\Users\\Chris.Williams\\Desktop\\OTB-7.1.0-Win64\\bin\\otbcli_BundleToPerfectSensor.bat', args )

    return out_pathname


def getGdalPansharpenedImage( pan, ms, out_path ):

    """
    generate pansharpened multispectral images using panchromatic band
    """

    # construct filename
    out_pathname = os.path.join( out_path, os.path.basename( ms ).replace( '.TIF', '_PS.TIF' ) )
    if not os.path.exists ( out_pathname ):

        # execute gdal script if pansharpened image not created
        args = [    r"c:\\Program Files\\Python36\\Scripts\\gdal_pansharpen.py",
                    "-r", "lanczos",
                    pan,
                    ms,
                    out_pathname ]

        out, err, code = execute ( 'python', args )

    return out_pathname


def getDateTime( pathname ):

    """
    parse date time from pathname
    """

    dt = None

    # parse for date time sub directory
    m = re.search( '_[0-9]{15}_', pathname )
    if m:
        dt = datetime.strptime( str(m.group(0))[:-2], '_%Y%m%d%H%M%S')

    return dt


def parseArguments(args=None):

    """
    parse arguments
    """

    # parse command line arguments
    parser = argparse.ArgumentParser(description='sentinel-2 processor')
    parser.add_argument('data_path', action="store")
    parser.add_argument('aoi_path', action="store")
    parser.add_argument('out_path', action="store")

    return parser.parse_args(args)


def main():

    """
    main path of execution
    """

    # parse arguments
    args = parseArguments()

    # get aois and data paths
    aois = glob.glob( os.path.join( args.aoi_path, '*.kml' ) )
    paths = glob.glob( os.path.join( args.data_path, '*/' ) )

    # iterate data paths
    for path in paths:

        scenes = { 'pan' : sorted ( glob.glob( os.path.join( path, '**/IMG_SPOT*_P*.tif' ), recursive=True ) ),
                    'ms' : sorted ( glob.glob( os.path.join( path, '**/IMG_SPOT*_MS*.tif' ), recursive=True ) ) }

        if len ( scenes[ 'pan' ] ) == len ( scenes[ 'ms' ] ):

            # get datetime from pathname
            dt = getDateTime( scenes[ 'pan'][ 0 ] )
            for idx in range( len ( scenes[ 'pan' ] ) ):

                # create output path
                out_path = os.path.join( args.out_path, dt.strftime( '%Y%m%d_%H%M%S' ) )
                if not os.path.exists( out_path ):
                    os.makedirs( os.path.join( out_path ) )

                # do pansharpening
                #ps_pathname = getGdalPansharpenedImage( scenes[ 'pan' ][ idx ], scenes[ 'ms' ][ idx ], out_path )
                ps_pathname = getOtbPansharpenedImage( scenes[ 'pan' ][ idx ], scenes[ 'ms' ][ idx ], out_path )
                ds = gdal.Open( ps_pathname, gdal.GA_ReadOnly )

                # for each aoi
                for aoi in aois:

                    aoi_name = os.path.splitext( os.path.basename( aoi ) )[ 0 ]

                    # clip image to aoi - reproject to epsg 3857 (google maps)
                    clip_pathname = ps_pathname.replace( '.TIF', '_{}.TIF'.format ( aoi_name ) )
                    options = '-t_srs epsg:3857 -cutline {} -tr 1.5 1.5 -crop_to_cutline -srcnodata 0 -dstalpha -r lanczos'. format ( aoi  )

                    if not os.path.exists( clip_pathname ):
                        gdal.Warp( clip_pathname, ds, options=options )

                    # construct rgb pathname
                    out_path = os.path.join( os.path.join( args.out_path, 'aoi' ), aoi_name )
                    if not os.path.exists( out_path ):
                        os.makedirs( out_path )
                        
                    rgb_pathname = os.path.join( out_path, dt.strftime( '%Y%m%d_%H%M%S.TIF' ) )
                    if not os.path.exists( rgb_pathname ):

                        # get band statistics
                        clip_ds, stats = getBandStatistics( clip_pathname )
                        if len( stats ) > 0:

                            # determine optimal 16bit to 8bit scaling
                            options = '-b 1 -b 2 -b 3 -b 5 -ot Byte'
                            channels = [ 1, 2, 3 ]

                            for idx, stat in enumerate ( stats ):

                                if stat[ 'idx' ] in channels:

                                    #lo = int ( max( stat[ 'mean' ] - ( stat[ 'std' ] * 2 ), 0 ) )
                                    #hi = int ( min( stat[ 'mean' ] + ( stat[ 'std' ] * 2 ), 65535 ) )
                                    #options += ' -scale_{} {} {} 0 255'.format( idx + 1, lo, hi )

                                    options += ' -scale_{} {} {} 0 255'.format( idx + 1, stat[ 'p2' ], stat[ 'p98' ] )

                            # apply translation
                            print ( 'processing {}'.format ( rgb_pathname ) )
                            gdal.Translate( rgb_pathname, clip_ds, options=options)


    return

# execute main
if __name__ == '__main__':
    main()

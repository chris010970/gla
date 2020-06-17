import osr
import gdal
import pyproj

from shapely import geometry
from shapely.ops import transform


class BBox:

    def __init__( self, pts, epsg=4326 ):

        """
        constructor
        """

        # create shapely object
        pt0 = ( pts[ 0 ], pts[ 1 ] )                    
        pt1 = ( pts[ 2 ], pts[ 3 ] )                     

        self._poly = geometry.Polygon( [    [ pt0[ 0 ], pt0[ 1 ] ], 
                                            [ pt1[ 0 ], pt0[ 1 ] ], 
                                            [ pt1[ 0 ], pt1[ 1 ] ],
                                            [ pt0[ 0 ], pt1[ 1 ] ],
                                            [ pt0[ 0 ], pt0[ 1 ] ] ] )
        # epsg code
        self._epsg = epsg
        return


    def transform( self, target ):

        """
        compute transformation of polygon to new coordinate system
        """

        # create source to destination transform
        prj = pyproj.Transformer.from_proj( pyproj.Proj(init='epsg:{}'.format( self._epsg ) ),
                                            pyproj.Proj(init='epsg:{}'.format( target ) ) ) 

        # apply projection
        return transform( prj.transform, self._poly )  


    def getImageRoi( self, image ):

        """
        compute intersection between image and polygon in pixel coordinates
        """

        # open image
        coords = None

        ds = gdal.Open( image, gdal.GA_ReadOnly )
        if ds is not None:

            # get geotransform
            prj = osr.SpatialReference( wkt=ds.GetProjection() )
            epsg = prj.GetAttrValue('AUTHORITY', 1 )

            # get image bounding polygon
            geo = ds.GetGeoTransform()

            image = BBox ( [    geo[0], 
                                geo[3] + geo[5] * ds.RasterYSize, 
                                geo[0] + geo[1] * ds.RasterXSize, 
                                geo[3]  ],  
                                epsg )

            # check for intersection
            tx_poly = self.transform( epsg )
            if tx_poly.intersects ( image._poly ):

                bounds = tx_poly.bounds

                # convert reprojected polygon to image coordinates
                x_min = ( bounds[ 0 ] - geo[ 0 ] ) / geo[ 1 ]
                y_min = ( bounds[ 3 ] - geo[ 3 ] ) / geo[ 5 ]
                
                x_max = ( bounds[ 2 ] - geo[ 0 ] ) / geo[ 1 ]
                y_max = ( bounds[ 1 ] - geo[ 3 ] ) / geo[ 5 ]

                # check out-of-bounds
                x_min = int( max( x_min, 0 ) ); y_min = int( max( y_min, 0 ) )
                x_max = int( min( x_max, ds.RasterXSize - 1 ) ); y_max = int( min( y_max, ds.RasterYSize - 1 ) )

                coords = ( x_min, y_min, x_max, y_max )                

        return coords

"""
aoi = BBox( [ -1, 54, 0, 55 ] )
coords = aoi.getImageRoi( 'D:\\data\\projects\\gla\\raw\\38012\\20180515_111226\\tmp\\pan\\IMG_PHR1A_PAN_201805151112269_ORT_3613657101-2_R1C1_CAL.TIF')
print ( coords )
"""

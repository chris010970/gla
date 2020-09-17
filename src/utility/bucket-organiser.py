import os

from spot import Spot
from pleiades import Pleiades
from src.utility.gsclient import GsClient

GsClient.updateCredentials( 'C:\\Users\\Chris.Williams\\.gcs\\gla001-232b82940cbf.json' )
client = GsClient( 'gla-datastore001' )

"""
blob_names = client.getBlobNameList( 'pleiades', [ '.zip' ] )
root_path = 'ssgp/raw'

for name in blob_names:

    obj = Pleiades( name )    

    path = os.path.join ( root_path, obj.getSubPath() )
    pathname = os.path.join ( path, os.path.basename( name ) )

    blob = client.moveBlob( name, dst_name=pathname )
"""

blob_names = client.getBlobNameList( 'spot', [ '.zip' ] )
root_path = 'ssgp/raw'

for name in blob_names:

    obj = Spot( name )    

    path = os.path.join ( root_path, obj.getSubPath() )
    pathname = os.path.join ( path, os.path.basename( name ) )

    blob = client.moveBlob( name, dst_name=pathname )


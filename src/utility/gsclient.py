import os
import re

from pathlib import Path
from google.cloud import storage

class GsClient:

    def __init__( self, name, chunk_size=None ):

        """
        constructor
        """

        # initialise sdk attributes
        self._name = name
        self._client = storage.Client()
        self._bucket = self._client.get_bucket( name )

        # set default chunk size
        if chunk_size is not None:
            storage.blob._DEFAULT_CHUNKSIZE = chunk_size 
            storage.blob._MAX_MULTIPART_SIZE = chunk_size 

        return

    @staticmethod
    def updateCredentials( pathname ):

        """
        update environmental variable
        """

        # add credentials to environment
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=pathname
        return


    @staticmethod
    def isUri( uri ):

        """
        check google storage uri
        """

        # check uri
        return 'gs://' in uri


    @staticmethod
    def parseUri( uri ):

        """
        parse bucket name and prefix from uri string
        """

        bucket = None; prefix = None

        # check gcs compliant
        drive = 'gs://'
        if drive in uri:
            
            # look for prefix
            tail = uri[ len( drive )  : ]
            tokens = tail.split( '/' )

            bucket = tokens[ 0 ]
            prefix = ''

            # retrieve prefix
            if len( tokens ) > 1:
                prefix = '/'.join( tokens[ 1 : ] )

        return bucket, prefix        


    def uploadFile( self, pathname, prefix=None, flatten=False ):

        """
        update environmental variable
        """

        if flatten:

            # upload files to out path
            blob_name = os.path.basename( pathname )
            if prefix is not None:
                blob_name = '{}/{}'.format( prefix, os.path.basename( pathname ) )
            
        else:

            # get posix form of pathname
            drive = os.path.splitdrive( pathname )
            blob_name = Path( drive[ 1 ] ).as_posix()
            
            # append pathname to out path
            if prefix is not None:
                blob_name = '{}/{}'.format( prefix, blob_name )

        # create blob in cloud
        blob = self._bucket.blob( blob_name.lstrip('/') )
        blob.upload_from_filename( pathname )

        return blob.public_url


    def downloadBlob( self, uri, out_path, flatten=False, overwrite=False ):

        """
        download files from gs bucket to local filesystem
        """

        pathname = None

        # grab blob
        blob = storage.blob.Blob( uri, self._bucket )
        if blob.exists():

            # copy all files to out path
            if flatten:
                pathname = os.path.join( out_path, os.path.basename( uri ) )
            else:
                # preserve directory structure
                pathname = os.path.join( out_path, uri )

            if not os.path.exists( pathname ) or overwrite:

                # make sub-directory if required
                if not os.path.exists ( os.path.dirname( pathname ) ):
                    os.makedirs( os.path.dirname( pathname ) )

                # stream blob to file
                with open( pathname, 'w+b' ) as z:
                    blob.download_to_file( z )            

        return pathname


    def getBlobNameList( self, prefix, pattern='.*' ):

        """
        get blob names matching regexp
        """

        match_names = []

        # get filelist in bucket prefix subfolder
        blobs = self._bucket.list_blobs(prefix=prefix, delimiter=None)
        for blob in blobs:

            # convert blob to dict
            d = self.getBlobAsDict( blob )

            # apply regexp match to key
            x = re.search( pattern,  d[ 'name' ] )
            if x is not None:                
                match_names.append( d[ 'name' ] )

        
        return match_names


    def getBlobList( self, prefix, pattern='.*' ):

        """
        get blobs whose name matching regexp
        """

        match_blobs = []

        # get filelist in bucket prefix subfolder
        blobs = self._bucket.list_blobs(prefix=prefix, delimiter=None)
        for blob in blobs:

            # apply match to bucket files
            d = self.getBlobAsDict( blob )

            # apply regexp match to key
            x = re.search( pattern,  d[ 'name' ] )
            if x is not None:                
                match_blobs.append( d )

        return match_blobs


    def moveBlob( self, name, **kwargs):
                
        """
        move blob to new dstination - optionally new bucket
        """

        new_blob = None

        # get destination details        
        dst = {     'name' : kwargs.pop( 'dst_name', name ),
                    'bucket' : kwargs.pop( 'dst_bucket', self._bucket ) }
                        
        # check src and dst are different
        dst[ 'name' ] = str( Path( dst[ 'name' ] ).as_posix() ).lstrip( '/' )
        if self._bucket != dst[ 'bucket' ] or name != dst[ 'name' ]:
        
            # grab src and copy to dst
            src_blob = self.getBlob( name )
            if src_blob.exists():
                new_blob = self._bucket.copy_blob( src_blob, dst[ 'bucket' ], dst[ 'name' ] )
            else:
                # source and dstination are the same
                print ( 'Blob does not exist: {}'.format( name ) )

            # delete src if dst exists
            if new_blob is not None and new_blob.exists():
                src_blob.delete()
        
        else:
            # source and dstination are the same
            print ( 'Identical source and destination: {} {}'.format ( self._bucket, name ) )

        return new_blob
        

    def copyBlob( self, name, **kwargs):
                
        """
        copy blob to new destination - optionally new bucket
        """

        new_blob = None

        # get destination details        
        dst = {    'name' : kwargs.pop( 'dst_name', name ),
                    'bucket' : kwargs.pop( 'dst_bucket', self._bucket ) }
                        
        # check src and dst are different
        dst[ 'name' ] = dst[ 'name' ].lstrip( '/' )
        if self._bucket != dst[ 'bucket' ] or name != dst[ 'name ']:
        
            # grab src and copy to dst
            src_blob = self.getBlob( name )
            if src_blob is not None:
                new_blob = self._bucket.copy_blob( src_blob, dst[ 'bucket' ], dst[ 'name' ] )
            else:
                # source and dstination are the same
                print ( 'Blob does not exist: {}'.format( name ) )
        
        else:
            # source and dstination are the same
            print ( 'Identical source and dstination: {} {}'.format ( self._bucket, name ) )

        return new_blob


    def getBlob( self, name ):
        
        """
        get blob
        """

        return storage.blob.Blob( name, self._bucket )


    def getBlobAsDict( self, blob ):
        
        """
        converts google.cloud.storage.Blob to context format (GCS.BucketObject)
        """

        # unpack blob into dictionary
        return {
            'name': blob.name,
            'bucket': blob.bucket.name,
            'contentType': blob.content_type,
            'timeCreated': blob.time_created,
            'timeUpdated': blob.updated,
            'timeDeleted': blob.time_deleted,
            'size': blob.size,
            'MD5': blob.md5_hash,
            'ownerID': '' if not blob.owner else blob.owner.get('entityId', ''),
            'CRC32c': blob.crc32c,
            'encryptionAlgorithm': blob._properties.get('customerEncryption', {}).get('encryptionAlgorithm', ''),
            'encryptionKeySHA256': blob._properties.get('customerEncryption', {}).get('keySha256', ''),
        }


    def getImageUriList( self, prefix, pattern=None ):

        """
        get blob
        """

        uris = []

        # convert matching blob names to gdal virtual fs uris
        keys = self.getBlobNameList( prefix, pattern=pattern )
        for key in keys:
            uris.append( '/vsigs/{}/{}'.format( self._name, key ) )

        return uris

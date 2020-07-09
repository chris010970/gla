import os
import re
import fnmatch


def getPathNameList ( path, pattern ):

    """
    apply regexp to filter file list
    """

    # get pattern matched file list
    result = []
    for root, dirs, files in os.walk(path):
                
        for name in files:

            pathname = os.path.join( root, name )
            x = re.search( pattern, pathname )

            if x is not None:
                result.append( os.path.join(root, name) )

    return result


def getFileList ( path, pattern ):

    """
    apply regexp to filter file list
    """

    # get pattern matched file list
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            x = re.search( pattern, name )
            if x is not None:                
                result.append( os.path.join(root, name) )

    return result


def getPathList ( path, pattern ):

    """
    apply regexp to filter path list
    """

    # get pattern matched sub-folder list
    result = []
    for root, dirs, files in os.walk(path):

        x = re.search( pattern, root )
        if x is not None:
            result.append( root )

    return result


def getFile ( path, pattern ):

    """
    validate single file satisfies reg exp
    """

    # get uniquely named file in path
    result = None
    filelist = getFileList( path, pattern )

    if len ( filelist ) == 1:
        result = filelist[ 0 ]

    return result


def getPath ( path, pattern ):

    """
    validate single path satisfies reg exp
    """

    # get uniquely named file in path
    result = None
    pathlist = getPathList( path, pattern )

    if len ( pathlist ) == 1:
        result = pathlist[ 0 ]

    return result


def removeFileList( filelist ):

    """
    delete files in list
    """

    # remove file list
    for pathname in filelist:
        if os.path.exists( pathname ):
            os.remove( pathname )

    return


def writeFile( pathname, buffer ):

    """
    create file with bufefr
    """

    # execute preprocess sql commands
    with open( pathname, "w" ) as fp:
        fp.write( buffer )

    return pathname

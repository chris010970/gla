import sys
import os
import subprocess


def execute( name, arguments, logger = None ):

    """
    execute sub-process
    """

    # create and execute sub-process
    p = subprocess.Popen( [name] + arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = p.communicate()
    code = p.poll()

    return out, err, code


def extractZip( pathname, out_path, overwrite=True ):

    """
    execute zip archive to out path
    """

    # create out path if necessary
    if not os.path.exists( out_path ):
        os.makedirs( out_path )

    # optionally apply overwrite 
    if overwrite:
        out_path = '-o{}'.format( out_path )

    # run 7zip to extract 
    pname = 'C:\\Program Files\\7-Zip\\7z.exe'
    out, err, code = execute( pname, [ 'x', pathname, out_path, '-y' ] )

    return out, err, code


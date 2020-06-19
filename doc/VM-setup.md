* GCP VM Instance configuration

1. Install Python 3.5 from source (dependency of OTB 7.1.0 Linux package)
    * mkdir ~/src; cd ~/src
    * sudo yum install wget
    * wget https://www.python.org/ftp/python/3.5.9/Python-3.5.9.tgz    
    * tar xvfz Python-3.5.9.tgz 
    * ./configure --enable-optimizations --with-ensurepip=install --enable-shared --prefix=/usr/local LDFLAGS="-Wl,--rpath=/usr/local/lib"
    * sudo make altinstall
    * sudo ln -s /usr/local/lib/libpython3.5m.so.1.0 /usr/local/lib/libpython3.5m.so.rh-python35-1.0
    * pip3.5 install numpy

2. Download Orfeo Toolbox Linux package
    * mkdir ~/apps; cd ~/apps
    * wget https://www.orfeo-toolbox.org/packages/OTB-7.1.0-Linux64.run
    * chmod a+x OTB-7.1.0-Linux64.run 
    * ./OTB-7.1.0-Linux64.run

3. Install GDAL 3.0.2 from source (dependency of OTB 7.1.0 Linux package)
    * cd ~/src
    * export LD_LIBRARY_PATH=/home/sac/apps/OTB-7.1.0-Linux64/lib/:/usr/local/lib
    * wget https://github.com/OSGeo/gdal/releases/download/v3.0.2/gdal-3.0.2.tar.gz
    * ./configure --with-proj=/home/sac/apps/OTB-7.1.0-Linux64 --with-geotiff=/home/sac/apps/OTB-7.1.0-Linux64 -with-hdf5=/home/sac/apps/OTB-7.1.0-Linux64  --with-python=/usr/local/bin/python3.5 
    * make
    * sudo make install

4. Install GitHub repository and runtime environment
    * git clone https://github.com/chris010970/gla.git
    * source ~/apps/OTB-7.1.0-Linux64/otbenv.profile 
    * mkdir /data/ancillary/srtm
    * mkdir /data/ancillary/geoid
    * wget https://gitlab.orfeo-toolbox.org/orfeotoolbox/otb-data/-/raw/master/Input/DEM/egm96.grd
    * mv egm96.grd /data/ancillary/geoid/
    * export GDAL_CACHEMAX=16384
    * export LD_LIBRARY_PATH=/home/sac/apps/OTB-7.1.0-Linux64/lib/:/usr/local/lib
    * source ~/apps/OTB-7.1.0-Linux64/otbenv.profile 
    
5. Install additional persistent SSD disk
    * sudo lsblk
    * sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,discard /dev/sdb
    * sudo mkdir /mnt/disks
    * sudo mkdir /mnt/disks/data
    * sudo mount -o discard,defaults /dev/sdb /mnt/disks/data
    * sudo chmod a+rw /mnt/disks/data/


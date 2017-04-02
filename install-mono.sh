cd /usr/local/src
wget http://download.mono-project.com/sources/mono/mono-4.8.0.520.tar.bz2
tar jxf mono-4.8.0.520.tar.bz2
cd mono-4.8.0
./configure --prefix=/opt/mono
make && make install


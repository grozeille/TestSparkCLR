mkdir /tmp/spark-clr
cd /tmp/spark-clr
wget https://github.com/Microsoft/Mobius/releases/download/v1.6.200/spark-clr_2.10-1.6.200.zip
unzip spark-clr_2.10-1.6.200.zip
rm -f spark-clr_2.10-1.6.200.zip
cd /opt
mv /tmp/spark-clr /opt


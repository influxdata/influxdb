### Packaging

In order to support all distros and old libc we build and package on
centos6.4 (64 bit). The package script takes care of cross compiling
Influxdb for 32bit architectures.

TODO: move the following to chef recipe.

Below are the steps needed to setup a centos6.4 vm to build Influxdb.

```
sudo yum install git-core glibc.i686 hg bzr protobuf-compiler glibc-devel.i686 libstdc++.i686 libstdc++-devel.i686 python-pip
sudo pip install --upgrade awscli
git clone git@github.com:influxdb/influxdb.git
wget http://go.googlecode.com/files/go1.1.2.linux-amd64.tar.gz
mkdir bin
cd bin
tar -xzvf ../go1.1.2.linux-amd64.tar.gz
mv go go1.1.2
ln -s go1.1.2 go
cd ..
curl -L https://get.rvm.io | bash -s stable
rvm install 1.9.3
cd influxdb/
git clean -dfx
git checkout .
git pull --rebase
# PATH=$HOME/bin/go/bin:$PATH GOROOT=~/bin/go/ ./test.sh
PATH=$HOME/bin/go/bin:$PATH GOROOT=~/bin/go/ ./release.sh 0.1.1.rc3
```

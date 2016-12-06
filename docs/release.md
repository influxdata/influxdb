## Creating a release
The release process is handled via our [circle.yml](https://github.com/influxdata/chronograf/blob/master/circle.yml).

A release tag of the format `1.1.0-beta1` needs to be added.  Afterwhich, circle
will build our packages for all of our platforms.

### Creating Release tag
You can create a release tag from [Github](https://github.com/influxdata/chronograf/releases)
or create an annotated tag:

```sh
git tag -a 1.1.0-beta1 -m "Release 1.1.0-beta1"
git push --tags
```

### Release platforms
* Linux
    * amd64
    * i386
    * armhf
    * arm64
    * armel
    * static_i386
    * static_amd64
* OS X 
    * amd64
* Windows
    * amd64
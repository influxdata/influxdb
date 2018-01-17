readTrusted 'Dockerfile_jenkins_ubuntu32'

pipeline {
  agent none

  stages {
    stage('gometalinter') {
      agent {
        docker {
          image 'jsternberg/gometalinter-diff'
          alwaysPull true
        }
      }

      when {
        expression {
          return env.CHANGE_TARGET != ""
        }
      }

      steps {
        sh "gometalinter-diff remotes/origin/${CHANGE_TARGET}"
      }
    }

    stage('64bit') {
      agent {
        docker {
          image 'golang:1.9.2'
        }
      }

      steps {
        sh """
        mkdir -p /go/src/github.com/influxdata
        cp -a $WORKSPACE /go/src/github.com/influxdata/influxdb

        cd /go/src/github.com/influxdata/influxdb
        go get github.com/sparrc/gdm
        gdm restore
        """

        sh """
        cd /go/src/github.com/influxdata/influxdb
        go test -parallel=1 ./...
        """
      }
    }

    stage('32bit') {
      agent {
        dockerfile {
          filename 'Dockerfile_jenkins_ubuntu32'
        }
      }

      steps {
        sh """
        mkdir -p /go/src/github.com/influxdata
        cp -a $WORKSPACE /go/src/github.com/influxdata/influxdb

        cd /go/src/github.com/influxdata/influxdb
        go get github.com/sparrc/gdm
        gdm restore
        """

        sh """
        cd /go/src/github.com/influxdata/influxdb
        go test -parallel=1 ./...
        """
      }
    }
  }
}

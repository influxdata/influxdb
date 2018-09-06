readTrusted 'Dockerfile_jenkins_ubuntu32'

pipeline {
  agent none

  stages {
    stage('Update changelog') {
      agent any

      when {
        anyOf {
          branch 'master'
          expression { BRANCH_NAME ==~ /^\d+(.\d+)*$/ }
        }
      }

      steps {
        sh "docker pull jsternberg/changelog"
        withDockerContainer(image: "jsternberg/changelog") {
          withCredentials(
            [[$class: "UsernamePasswordMultiBinding",
              credentialsId: "hercules-username-password",
              usernameVariable: "GITHUB_USER",
              passwordVariable: "GITHUB_TOKEN"]]) {
            script {
              if (env.GIT_PREVIOUS_SUCCESSFUL_COMMIT) {
                sh "git changelog ${env.GIT_PREVIOUS_SUCCESSFUL_COMMIT}"
              } else {
                sh "git changelog"
              }
            }
          }
        }

        sshagent(credentials: ['jenkins-hercules-ssh']) {
          sh """
          set -e
          if ! git diff --quiet; then
            git config remote.origin.pushurl git@github.com:influxdata/influxdb.git
            git commit -am 'Update changelog'
            git push origin HEAD:${BRANCH_NAME}
          fi
          """
        }
      }
    }

    stage('64bit') {
      agent {
        docker {
          image 'golang:1.11'
        }
      }

      steps {
        sh """
        mkdir -p /go/src/github.com/influxdata
        cp -a $WORKSPACE /go/src/github.com/influxdata/influxdb

        cd /go/src/github.com/influxdata/influxdb
        go get github.com/golang/dep/cmd/dep
        dep ensure -vendor-only
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
        go get github.com/golang/dep/cmd/dep
        dep ensure -vendor-only
        """

        sh """
        cd /go/src/github.com/influxdata/influxdb
        go test -parallel=1 ./...
        """
      }
    }
  }
}

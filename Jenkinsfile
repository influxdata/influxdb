readTrusted 'Dockerfile_jenkins_ubuntu32'

pipeline {
  agent none

  stages {
    stage('Update changelog') {
      agent any

      when {
        anyOf {
          expression { BRANCH_NAME ==~ /^1(.\d+)*$/ }
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
          image 'golang:1.13'
          args '-e "GOCACHE=/tmp"'
        }
      }

      steps {
        sh """
				cd $WORKSPACE
				go mod download
        """

        sh """
        cd $WORKSPACE
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
				cd $WORKSPACE
				go mod download
        """

        sh """
        cd $WORKSPACE
        go test -parallel=1 ./...
        """
      }
    }
  }
}

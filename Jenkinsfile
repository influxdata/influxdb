#!/usr/bin/env groovy
//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

import hudson.model.*

pipeline {
    agent any
    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '3'))
    }
    environment {
        THRIFT_HOME = '/opt/thrift-0.11.0'
        version = "${getVersion()}"
        BINTRAY_USER = getParam('BINTRAY_USER')
        BINTRAY_API_KEY = getParam('BINTRAY_API_KEY')
    }
    stages {

        stage('Checkout') {
            steps {
                this.notifyBuild('STARTED', version)
                git credentialsId: 'github', poll: false, url: 'git@github.com:senx/warp10-platform.git'
                sh 'git fetch --tags'
                echo "Building ${version}"
            }
        }

        stage('Build') {
            steps {
                sh './gradlew clean build -x test'
                sh 'node changelog.js > CHANGELOG.md'
            }
        }

        stage('Test') {
            options { retry(3) }
            steps {
                sh './gradlew -Djava.security.egd=file:/dev/urandom test'
                junit allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'
                step([$class: 'JUnitResultArchiver', allowEmptyResults: true, keepLongStdio: true, testResults: '**/build/test-results/**/*.xml'])
            }
        }


        stage('Pack and Tar') {
            steps {
                sh './gradlew jar pack -x test'
                archiveArtifacts allowEmptyArchive: true, artifacts: '**/build/libs/*.jar', fingerprint: true
                sh './gradlew createTarArchive -x test'
                archiveArtifacts allowEmptyArchive: true, artifacts: '**/build/libs/*.tar.gz', fingerprint: true
            }
        }

        stage('Publish') {
            steps {
                sh './gradlew warp10:uploadArchives'
                sh './gradlew warpscript:uploadArchives'
            }
        }

        stage('Deploy') {
            when {
                expression { return isItATagCommit() }
            }
            parallel {
                stage('Deploy to Bintray') {
                    options {
                        timeout(time: 2, unit: 'HOURS')
                    }
                    input {
                        message 'Should we deploy to Bintray?'
                    }
                    steps {
                        sh './gradlew crypto:clean crypto:bintrayUpload -x test'
                        sh './gradlew token:clean  token:bintrayUpload -x test'
                        sh './gradlew warp10:clean warp10:bintrayUpload -x test'
                        sh './gradlew warp10:clean warpscript:bintrayUpload -x test'
                        this.notifyBuild('PUBLISHED', version)
                    }
                }
            }
        }
    }

    post {
        success {
            this.notifyBuild('SUCCESSFUL', version)
        }
        failure {
            this.notifyBuild('FAILURE', version)
        }
        aborted {
            this.notifyBuild('ABORTED', version)
        }
        unstable {
            this.notifyBuild('UNSTABLE', version)
        }
    }
}

void notifyBuild(String buildStatus, String version) {
    // build status of null means successful
    buildStatus = buildStatus ?: 'SUCCESSFUL'
    String subject = "${buildStatus}: Job ${env.JOB_NAME} [${env.BUILD_DISPLAY_NAME}] | ${version}"
    String summary = "${subject} (${env.BUILD_URL})"
    // Override default values based on build status
    if (buildStatus == 'STARTED') {
        color = 'YELLOW'
        colorCode = '#FFFF00'
    } else if (buildStatus == 'SUCCESSFUL') {
        color = 'GREEN'
        colorCode = '#00FF00'
    } else if (buildStatus == 'PUBLISHED') {
        color = 'BLUE'
        colorCode = '#0000FF'
    } else {
        color = 'RED'
        colorCode = '#FF0000'
    }

    // Send notifications
    this.notifySlack(colorCode, summary, buildStatus)
}

void notifySlack(color, message, buildStatus) {
    String slackURL = getParam('slackUrl')
    String payload = "{\"username\": \"${env.JOB_NAME}\",\"attachments\":[{\"title\": \"${env.JOB_NAME} ${buildStatus}\",\"color\": \"${color}\",\"text\": \"${message}\"}]}"
    sh "curl -X POST -H 'Content-type: application/json' --data '${payload}' ${slackURL}"
}

String getParam(key) {
    return params.get(key)
}

String getVersion() {
    return sh(returnStdout: true, script: 'git describe --abbrev=0 --tags').trim()
}

boolean isItATagCommit() {
    String lastCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
    String tag = sh(returnStdout: true, script: "git show-ref --tags -d | grep ^${lastCommit} | sed -e 's,.* refs/tags/,,' -e 's/\\^{}//'").trim()
    return tag != ''
}

@Library('shared-library') _

pipeline {

    agent {
        docker {
            label 'memphis-jenkins-big-fleet,'
            image 'python:3.11.9-bullseye'
            args '-u root'
        }
    }

    environment {
            HOME           = '/tmp'
            SLACK_CHANNEL  = '#jenkins-events'
    }

    stages {

        stage('Prepare Environment') {
            when {
                anyOf {
                    allOf {
                        branch 'master'
                        triggeredBy 'UserIdCause' // Manual trigger on master
                    }
                    allOf {
                        branch 'latest'
                    }
                }
            }            
            steps {
                script {
                    sh 'git config --global --add safe.directory $(pwd)'
                    env.GIT_AUTHOR = sh(script: 'git log -1 --pretty=%an', returnStdout: true).trim()
                    env.COMMIT_MESSAGE = sh(script: 'git log -1 --pretty=%B', returnStdout: true).trim()
                    def triggerCause = currentBuild.getBuildCauses().find { it._class == 'hudson.model.Cause$UserIdCause' }
                    env.TRIGGERED_BY = triggerCause ? triggerCause.userId : 'Commit'
                }                
                sh """
                export DEBIAN_FRONTEND=noninteractive
                apt update -y
                apt install -y python3 python3-pip python3-dev gcc make
                # wget -qO - https://packages.confluent.io/deb/7.0/archive.key | apt-key add -
                # add-apt-repository "deb https://packages.confluent.io/clients/deb \$(lsb_release -cs) main"
                apt update
                pip install --user pdm
                pip install requests
                """
            }
        }   

            
        stage('Beta Release') {
            when {
                allOf {
                    branch 'master'
                    triggeredBy 'UserIdCause' // Manual "Build Now"
                }
            }           
            steps {
                sh '''
                sed -i -E 's/^(name *= *")superstream-clients(")/\\1superstream-clients-beta\\2/' pyproject.toml
                '''                
                sh 'pip install --quiet build twine'
                sh 'python -m build'
                withCredentials([usernamePassword(credentialsId: 'superstream-pypi', usernameVariable: 'USR', passwordVariable: 'PSW')]) {
                    sh """
                        twine upload dist/* \
                        -u $USR \
                        -p $PSW
                    """                     
                }                                                  
            }
        }
        stage('Prod Release') {
            when {
                branch 'latest'
            }            
            steps {
                sh 'pip install --quiet build twine'
                sh 'python -m build'
                withCredentials([usernamePassword(credentialsId: 'superstream-pypi', usernameVariable: 'USR', passwordVariable: 'PSW')]) {
                    sh """
                        twine upload dist/* \
                        -u $USR \
                        -p $PSW
                    """                     
                }                
            }
        }
        stage('Create Release'){
            when {
                branch 'latest'
            }       
            steps { 
                sh 'pip install toml'
                script {
                    def version = sh(
                        script: '''
                        python3 -c "import toml; print(toml.load('pyproject.toml')['project']['version'])"
                        ''',
                        returnStdout: true
                    ).trim()

                    env.versionTag = version
                }                              
                sh """
                    curl -L https://github.com/cli/cli/releases/download/v2.40.0/gh_2.40.0_linux_amd64.tar.gz -o gh.tar.gz 
                    tar -xvf gh.tar.gz
                    mv gh_2.40.0_linux_amd64/bin/gh /usr/local/bin 
                    rm -rf gh_2.40.0_linux_amd64 gh.tar.gz
                """
                withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
                sh """
                GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git config --global user.email "jenkins@superstream.ai"
                GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git config --global user.name "Jenkins"                
                GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git tag -a $versionTag -m "$versionTag"
                GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git push origin $versionTag
                """
                }                
                withCredentials([string(credentialsId: 'gh_token', variable: 'GH_TOKEN')]) {
                sh """
                gh release create $versionTag dist/superstream_clients-${env.versionTag}.tar.gz --generate-notes
                """
                }                
            }
        }                              
    }
    post {
        always {
                cleanWs()
        }
        success {
            script {
                if (env.GIT_BRANCH == 'latest') {   
                    sendSlackNotification('SUCCESS')         
                    notifySuccessful()
                }
            }
        }
        
        failure {
            script {
                if (env.GIT_BRANCH == 'latest') { 
                    sendSlackNotification('FAILURE')              
                    notifyFailed()
                }
            }            
        }
        aborted {
            script {
                if (env.BRANCH_NAME == 'latest') {
                    sendSlackNotification('ABORTED')
                }
                // Get the build log to check for the specific exception and retry job
                AgentOfflineException()
            }          
        }
    }    
}

def notifySuccessful() {
    emailext (
        subject: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
        body: """SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':
        Check console output and connection attributes at ${env.BUILD_URL}""",
        to: 'tech-leads@superstream.ai'
    )
}
def notifyFailed() {
    emailext (
        subject: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
        body: """FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':
        Check console output at ${env.BUILD_URL}""",
        to: 'tech-leads@superstream.ai'
    )
}

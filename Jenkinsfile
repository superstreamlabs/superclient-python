
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
            // when {
            //     branch '*-beta'
            // }            
            steps {
                script {
                    def version = readFile('version-beta.conf').trim()
                    env.versionTag = version
                    echo "Using version from version-beta.conf: ${env.versionTag}"               
                }
                // sh """
                //   sed -i -r "s/superclient/superclient-beta/g" pyproject.toml
                // """ 
                // sh "sed -i \'s/version = \"[0-9]\\+\\.[0-9]\\+\\.[0-9]\\+\"/version = \"${env.versionTag}\"/g\' pyproject.toml"
                // sh """  
                //     C_INCLUDE_PATH=/usr/include/librdkafka LIBRARY_PATH=/usr/include/librdkafka /tmp/.local/bin/pdm build
                // """
                sh 'pip install build'
                sh '''
                    pip install toml
                    python -c "
        import toml
        data = toml.load('pyproject.toml')
        data['project']['name'] = 'superclient-beta'
        with open('pyproject.toml', 'w') as f:
            toml.dump(data, f)
        "
                '''                
                sh 'python -m build'
                sh 'ls dist/'
                // withCredentials([usernamePassword(credentialsId: 'superstream-pypi', usernameVariable: 'USR', passwordVariable: 'PSW')]) {
                //         sh """
                //             python3 patch/patch.py --src "dist/superstream_confluent_kafka_beta-${env.versionTag}-cp311-cp311-linux_x86_64.whl" --output "dist/" --prefix "superstream_confluent_kafka_beta-${env.versionTag}"
                //         """
                //         sh"""
                //             rm dist/superstream_confluent_kafka_beta-${env.versionTag}-cp311-cp311-linux_x86_64.whl
                //             /tmp/.local/bin/pdm publish --no-build --username $USR --password $PSW
                //         """
                // }                                                  
            }
        }
        // stage('Prod Release') {
        //     when {
        //         branch '2.4.0'
        //     }            
        //     steps {
        //         script {
        //             def version = readFile('version.conf').trim()
        //             env.versionTag = version
        //             echo "Using version from version.conf: ${env.versionTag}"               
        //         }
        //         sh "sed -i \'s/version = \"[0-9]\\+\\.[0-9]\\+\\.[0-9]\\+\"/version = \"${env.versionTag}\"/g\' pyproject.toml"
        //         sh """  
        //             C_INCLUDE_PATH=/usr/include/librdkafka LIBRARY_PATH=/usr/include/librdkafka /tmp/.local/bin/pdm build
        //         """
        //         withCredentials([usernamePassword(credentialsId: 'superstream-pypi', usernameVariable: 'USR', passwordVariable: 'PSW')]) {
        //                 sh """
        //                     python3 patch/patch.py --src "dist/superstream_confluent_kafka-${env.versionTag}-cp311-cp311-linux_x86_64.whl" --output "dist/" --prefix "superstream_confluent_kafka-${env.versionTag}"
        //                 """
        //                 sh"""
        //                     rm dist/superstream_confluent_kafka-${env.versionTag}-cp311-cp311-linux_x86_64.whl
        //                     /tmp/.local/bin/pdm publish --no-build --username $USR --password $PSW
        //                 """
        //         }                
        //     }
        // }
        // stage('Create Release'){
        //     when {
        //         branch '2.4.0'
        //     }       
        //     steps {               
        //         sh """
        //             curl -L https://github.com/cli/cli/releases/download/v2.40.0/gh_2.40.0_linux_amd64.tar.gz -o gh.tar.gz 
        //             tar -xvf gh.tar.gz
        //             mv gh_2.40.0_linux_amd64/bin/gh /usr/local/bin 
        //             rm -rf gh_2.40.0_linux_amd64 gh.tar.gz
        //         """
        //         withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
        //         sh """
        //         GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git config --global user.email "jenkins@memphis.dev"
        //         GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git config --global user.name "Jenkins"                
        //         GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git tag -a $versionTag -m "$versionTag"
        //         GIT_SSH_COMMAND='ssh -i $check -o StrictHostKeyChecking=no' git push origin $versionTag
        //         """
        //         }                
        //         withCredentials([string(credentialsId: 'gh_token', variable: 'GH_TOKEN')]) {
        //         sh """
        //         gh release create $versionTag dist/superstream_confluent_kafka-${env.versionTag}.tar.gz --generate-notes
        //         """
        //         }                
        //     }
        // }                              
    }
    post {
        always {
                cleanWs()
        }
        // success {
        //   script {  
        //     if (env.BRANCH_NAME == '2.4.0') {
        //         sendSlackNotification('SUCCESS')
        //     }
        //   }
        // }
        // failure {
        //   script {
        //     if (env.BRANCH_NAME == '2.4.0') {  
        //         sendSlackNotification('FAILURE')
        //     }
        //   }
        // }        
        // aborted {
        //   script {
        //     if (env.BRANCH_NAME == '2.4.0') {
        //         sendSlackNotification('ABORTED')
        //     }
        //     // Get the build log to check for the specific exception
        //     def buildLog = currentBuild.rawBuild.getLog(50)
        //     // Log the build log for debugging purposes (you can remove this once confirmed)
        //     echo "Build Log:\n${buildLog.join('\n')}"
        //     // Check if the log contains the specific exception using a regular expression
        //     if (buildLog.find { it =~ /org\.jenkinsci\.plugins\.workflow\.support\.steps\.AgentOfflineException/ }) {
        //         echo 'AgentOfflineException found, retrying the build...'
        //         // Check if the build has parameters and rerun the job accordingly
        //         def paramsList = currentBuild.rawBuild.getAction(hudson.model.ParametersAction)?.parameters
        //         if (paramsList) {
        //             build(job: env.JOB_NAME, parameters: paramsList)
        //         } else {
        //             echo 'No parameters found, rerunning without parameters'
        //             build(job: env.JOB_NAME)
        //         }
        //     } else {
        //         echo 'Abort not related to AgentOfflineException, not retrying.'
        //     }
        //   }
        // }
    }    
}

// SlackSend Function
def sendSlackNotification(String jobResult) {
    def jobUrl = env.BUILD_URL
    def messageDetail = env.COMMIT_MESSAGE ? "Commit/PR by @${env.GIT_AUTHOR}:\n${env.COMMIT_MESSAGE}" : "No commit message available."
    def projectName = env.JOB_NAME

    // Define the color based on the job result
    def color = jobResult == 'SUCCESS' ? 'good' : (jobResult == 'ABORTED' ? '#808080' : 'danger')

    slackSend (
        channel: "${env.SLACK_CHANNEL}",
        color: color,
        message: """\
*:rocket: Jenkins Build Notification :rocket:*

*Project:* `${projectName}`
*Build Number:* `#${env.BUILD_NUMBER}`
*Status:* ${jobResult == 'SUCCESS' ? ':white_check_mark: *Success*' : (jobResult == 'ABORTED' ? ':warning: *Aborted*' : ':x: *Failure*')}

:information_source: ${messageDetail}
Triggered by: ${env.TRIGGERED_BY}
:link: *Build URL:* <${jobUrl}|View Build Details>
"""
    )
}
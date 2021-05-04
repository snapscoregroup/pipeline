pipeline {

    agent {
        node {
            label 'master'
        }
    }

    environment {
    		def jdk = tool name: 'openjdk14'
    		JAVA_HOME="${jdk}"
    		GITHUB_CREDENTIALS = credentials('snapscore-ci')
    }

    stages {
        stage('Environment check') {
            steps {
                sh 'echo "jdk installation path is: ${jdk}"'
                sh '${jdk}/bin/java -version'
                sh '$JAVA_HOME/bin/java -version'
            }
        }

        stage ('Clone sources') {
            steps {
                checkout scm
            }
        }

        stage ('Gradle Build Test') {
            steps {
                sh "./gradlew clean test build"
            }
        }

        stage ('Gradle Publish') {
            when {
                branch "master";
            }
            steps {
                sh "./gradlew publish"
            }
         }

    }

    post {

        success {
          slackSend (channel: '#jenkins', color: '#00FF00', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }

        failure {
          slackSend (channel: '#jenkins', color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }

    }

}

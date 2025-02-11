pipeline {
    agent any

    stages {
        stage('Pull Latest Code') {
            steps {
                script {
                    echo 'Pulling latest code from Git'
                    sh 'git pull origin exit17' // Change branch if needed
                }
            }
        }

        stage('Stop Existing Containers') {
            steps {
                script {
                    echo 'Stopping existing containers'
                    sh "echo '7631' | sudo -S docker compose down"
                }
            }
        }

        stage('Start Containers') {
            steps {
                script {
                    echo 'Starting containers in detached mode'
                    sh "echo '7631' | sudo -S docker compose up -d"
                }
            }
        }
    }
}

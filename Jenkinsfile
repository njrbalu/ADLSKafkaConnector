#!/usr/bin/env groovy

pipeline {
    agent { label 'linux' }

    options {
        timeout(time: 1, unit: 'HOURS')
        timestamps()
        disableConcurrentBuilds()
    }


    environment {
        // Setup main version number
        TEAMS_URL = "https://outlook.office.com/webhook/f1cf3571-5517-4907-a0d0-e2db0869b138@fbc493a8-0d24-4454-a815-f4ca58e8c09d/JenkinsCI/72b40d1d7d5640ca8faccc065d21763b/2dc675f8-3989-44a3-8d24-2ce2c41f0f68"
        MAVEN_OPTS = "-Xmx1024m"
        SONAR_PROJECT_KEY = 'kafka-adls-connector'
        VERSION_NUMBER= '1.0.1'
    }

    stages {
        stage('Checkout') {
            steps {
            	teamsNotifyStart "${env.TEAMS_URL}"
                checkout scm
          		sh 'chmod -R 755 *'      
            }
        }
		
        stage('Build') {
            steps {
                sh 'mvn versions:set -DnewVersion=$VERSION_NUMBER'
                sh 'mvn clean compile install -DskipTests'
            }
        }
        
        stage('UnitTests') {
            steps {
                sh 'mvn test'
            }
        }

        stage('SonarQubeScan') {
            when { branch 'develop' }
            steps {
                withSonarQubeEnv('SonarQube') {
                    withCredentials([string(credentialsId: 'sonartoken', variable: 'SONAR_TOKEN')]) {
                        sh '''mvn sonar:sonar \
                            -Dsonar.projectVersion=$VERSION_NUMBER \
                            -Dsonar.login=$SONAR_TOKEN \
                            -Dsonar.projectKey=$SONAR_PROJECT_KEY \
                            -Dsonar.jacoco.reportPaths=target/jacoco.exec \
                            -Dsonar.junit.reportPaths=target/surefire-reports/*.xml                       
                        '''
                    }
                }
            }
        }

        stage('Tests-QualityGate') {
            steps {
                junit 'target/surefire-reports/*.xml'
            }
        }

        stage('Coverage-QualityGate') {
            steps {
                jacoco minimumLineCoverage: '80'
            }
        }

        stage('SonarQube-QualityGate') {
            when { branch 'develop' }
            steps {
                script { checkSonarQualityGate }
            }
        }

        stage('Archive') {
            when { anyOf { branch 'develop' } }
            steps(){
                archiveArtifacts 'target/*.jar'
            }
        }
    }

    post {
        always {
            cleanWs()
        }
        success {
            script {
                teamsNotifySuccess "${env.TEAMS_URL}"
            }
        }
        unstable {
            script {
                teamsNotifyUnstable "${env.TEAMS_URL}"
            }
        }
        failure {
            script {
                teamsNotifyFailure "${env.TEAMS_URL}"
            }
        }
    }


}
// Jenkinsfile -- market-data-loader CI
// Installs requirements.txt into a per-build venv and runs pytest --
// same test invocation scripts/deploy.py's own unit-test step uses
// (python -m pytest tests -q). Uses a venv rather than the system
// Python install so this build never fights another job's or the
// user's own installed package versions.
// Requires "python" to be on PATH for whichever account the Jenkins
// service runs as -- see the repo's CI setup guide if this stage
// can't find it (a common gotcha: Python installed "for me only" is
// invisible to a service running as a different/Local System account).

pipeline {
    agent any

    options {
        timestamps()
        timeout(time: 20, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }

    stages {
        stage('Set up venv') {
            steps {
                bat 'python -m venv .venv'
                bat '.venv\\Scripts\\python -m pip install --upgrade pip'
                bat '.venv\\Scripts\\pip install -r requirements.txt pytest'
            }
        }
        stage('Test') {
            steps {
                bat '.venv\\Scripts\\python -m pytest tests -q --junitxml=pytest-report.xml'
            }
        }
    }

    post {
        always {
            junit testResults: 'pytest-report.xml', allowEmptyResults: true
        }
    }
}

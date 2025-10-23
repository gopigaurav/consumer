pipeline {
    agent any

    environment{
        REGISTRY = "gopigaurav"   // ECR or docker hub
        IMAGE_NAME = "consumer"                  // change to consumer in consumer repo
        INFRA_REPO = "git@github.com:your-org/gitops_infra_kubernetes.git"
        BRANCH = "main"
    }

    stages{
        stage('checkout') {
            steps{
                checkout sm
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    docker.build("${REGISTRY}/${IMAGE_NAME}:${env.BUILD_NUMBER}")
                }
            }
        }

        stage('Push Docker Image') {
            steps {
                script {
                    docker.withRegistry('', 'dockerhub-creds') {
                        docker.image("${REGISTRY}/${IMAGE_NAME}:${env.BUILD_NUMBER}").push()
                        docker.image("${REGISTRY}/${IMAGE_NAME}:${env.BUILD_NUMBER}").push("latest")
                    }
                }
            }
        }

        stage('Update Infra GitOps Repo') {
            steps {
                script {
                    sh """
                        rm -rf infra-gitops
                        git clone ${INFRA_REPO}
                        cd infra-gitops/overlays/dev
                        yq e -i '.image.tag = "${env.BUILD_NUMBER}"' ${IMAGE_NAME}-values.yaml
                        git config user.email "ci@yourcompany.com"
                        git config user.name "ci-bot"
                        git commit -am "Update ${IMAGE_NAME} to build ${env.BUILD_NUMBER}"
                        git push origin ${BRANCH}
                    """
                }
            }
        }
    }
}
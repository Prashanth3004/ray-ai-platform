pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        AWS_REGION = 'us-east-1'
        AWS_ACCOUNT_ID = '145023571604'
        ECR_REPOSITORY = '145023571604.dkr.ecr.us-east-1.amazonaws.com/ray-ai-platform'
        EKS_CLUSTER = 'ray-ai-platform'
        S3_BUCKET = 'ray-ai-platform-145023571604'
        TF_IN_AUTOMATION = 'true'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Terraform Format and Validate') {
            steps {
                sh '''
                    set -eu
                    terraform -chdir=terraform/networking fmt -check -recursive
                    terraform -chdir=terraform/eks fmt -check -recursive

                    terraform -chdir=terraform/networking init -backend=false
                    terraform -chdir=terraform/networking validate

                    terraform -chdir=terraform/eks init -backend=false
                    terraform -chdir=terraform/eks validate
                '''
            }
        }

        stage('Terraform Plan') {
            when {
                changeRequest()
            }
            steps {
                sh '''
                    set -eu

                    # JM Comment: Initialize and plan the networking state layer first.
                    terraform -chdir=terraform/networking init
                    terraform -chdir=terraform/networking plan -input=false

                    # JM Comment: EKS reads the networking layer's local state.
                    terraform -chdir=terraform/eks init
                    terraform -chdir=terraform/eks plan -input=false
                '''
            }
        }

        stage('Build and Push Image') {
            when {
                branch 'master'
            }
            steps {
                script {
                    env.IMAGE_TAG = "${env.BUILD_NUMBER}-${env.GIT_COMMIT.take(7)}"
                }
                sh '''
                    set -eu

                    # JM Comment: Push an immutable build-specific image tag.
                    aws ecr get-login-password --region "$AWS_REGION" |
                      docker login --username AWS --password-stdin \
                      "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

                    docker build \
                      -f docker/Dockerfile \
                      -t "$ECR_REPOSITORY:$IMAGE_TAG" .

                    docker push "$ECR_REPOSITORY:$IMAGE_TAG"

                    printf '%s\\n' "$IMAGE_TAG" > image-tag.txt
                '''
                archiveArtifacts artifacts: 'image-tag.txt', fingerprint: true
            }
        }

       
stage('Trigger RayJob') {
    when {
        branch 'master'
    }
    steps {
        sh '''
            set -eu

            # JM Comment: Render both Ray containers with the exact image built by this Jenkins run.
            export ECR_IMAGE="${ECR_REPOSITORY}:${IMAGE_TAG}"
            envsubst '${ECR_IMAGE}' < kubernetes/rayjob-template.yaml > rayjob-rendered.yaml

            # JM Comment: Fail fast unless both head and worker images match this build.
            test "$(grep -Fc "image: \\"${ECR_IMAGE}\\"" rayjob-rendered.yaml)" -eq 2 || {
                echo "ERROR: Head and worker images do not match ${ECR_IMAGE}"
                grep -n 'image:' rayjob-rendered.yaml
                exit 1
            }

            # JM Comment: Remove the previous RayJob so its FAILED status cannot affect this run.
            kubectl delete rayjob ray-ai-pipeline -n default --ignore-not-found --wait=true

            # JM Comment: Validate and create a fresh RayJob using the current image.
            kubectl apply --dry-run=server -f rayjob-rendered.yaml
            kubectl apply -f rayjob-rendered.yaml

            # JM Comment: Poll the new RayJob status for up to 30 minutes.
            for i in $(seq 1 180); do
              STATUS=$(kubectl get rayjob ray-ai-pipeline \
                -n default -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)

              echo "RayJob status: ${STATUS:-Pending}"

              case "$STATUS" in
                SUCCEEDED)
                  exit 0
                  ;;
                FAILED|STOPPED)
                  kubectl describe rayjob ray-ai-pipeline -n default
                  exit 1
                  ;;
              esac

              sleep 10
            done

            echo "RayJob timed out after 30 minutes."
            kubectl describe rayjob ray-ai-pipeline -n default
            exit 1
        '''
    }
}

        stage('Verify S3 Artifacts') {
            when {
                branch 'master'
            }
            steps {
                sh '''
                    set -eu

                    # JM Comment: Verify the expected pipeline outputs exist.
                    aws s3api head-object \
                      --bucket "$S3_BUCKET" \
                      --key artifacts/metrics.json

                    aws s3api head-object \
                      --bucket "$S3_BUCKET" \
                      --key artifacts/model.pt
                '''
            }
        }
    }

    post {
        always {
            echo 'Jenkins pipeline finished. Review stage results for failures.'
        }
    }
}
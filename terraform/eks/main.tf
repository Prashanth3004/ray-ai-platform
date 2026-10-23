
#Provision the managed EKS cluster for the Ray AI platform.

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

data "terraform_remote_state" "networking" {
  backend = "local"

  config = {
    path = "../networking/terraform.tfstate"
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.0"

  name               = var.cluster_name
  kubernetes_version = var.kubernetes_version

  vpc_id     = data.terraform_remote_state.networking.outputs.vpc_id
  subnet_ids = data.terraform_remote_state.networking.outputs.private_subnet_ids

  endpoint_public_access  = true
  endpoint_private_access = true

  enable_irsa = true

  addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni    = {}
  }

  eks_managed_node_groups = {
    system = {
      name           = "ray-system"
      instance_types = ["c7i-flex.large"]
      capacity_type  = "ON_DEMAND"

      min_size     = 1
      max_size     = 2
      desired_size = 1

      labels = {
        workload = "system"
      }
    }

    ray_workers = {
      name           = "ray-workers"
      instance_types = ["c7i-flex.large"]
      capacity_type  = "SPOT"

      min_size     = 0
      max_size     = 3
      desired_size = 0

      labels = {
        workload = "ray-worker"
      }
    }
  }

  tags = {
    Project     = "ray-ai-platform"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}
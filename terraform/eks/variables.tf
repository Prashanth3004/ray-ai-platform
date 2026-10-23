
# Configure the EKS cluster.

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_profile" {
  type    = string
  default = "ray-ai-terraform"
}

variable "cluster_name" {
  type    = string
  default = "ray-ai-platform"
}

variable "kubernetes_version" {
  type    = string
  default = "1.34"
}

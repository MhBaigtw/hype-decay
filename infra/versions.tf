terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State is LOCAL for the first apply, on purpose.
  #
  # This root module creates the state bucket and the lock table, so it cannot
  # keep its own state in them yet -- that is the Terraform bootstrap
  # chicken-and-egg. After the first successful apply, uncomment the block
  # below and run:
  #
  #     terraform init -migrate-state
  #
  # Terraform then copies terraform.tfstate into the bucket it just created and
  # every later task uses the remote backend with locking.
  #
  backend "s3" {
    bucket         = "hype-decay-tfstate-820697996849"
    key            = "guardrails/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "hype-decay-tflock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.region

  # Every resource gets these, so Cost Explorer can attribute spend to this
  # project and an untagged resource stands out as something created by hand.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Task      = "task-1-guardrails"
    }
  }
}

data "aws_caller_identity" "current" {}

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
    bucket       = "hype-decay-tfstate-820697996849"
    key          = "guardrails/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.region

  # Every resource gets these, so Cost Explorer can attribute spend to this
  # project and an untagged resource stands out as something created by hand.
  #
  # Task is deliberately NOT a default tag. A module-wide value is true only of
  # the resources made in that task: it said task-1-guardrails on the curated
  # bucket and the manifest, which Task 2 created, and bumping it per task would
  # relabel the Task 1 state bucket instead. Each top-level resource carries the
  # task that introduced it.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

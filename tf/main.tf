terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {}

resource "aws_s3_bucket" "example" {
  bucket = "ttc-api"

  tags = {
    Name        = "TTC API data lake"
    Environment = "Dev"
  }
}
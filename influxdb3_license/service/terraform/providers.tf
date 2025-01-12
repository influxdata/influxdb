# providers.tf

terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      # version = "~> 6.15"
    }
    # random = {
    #   source  = "hashicorp/random"
    #   version = "~> 3.0"
    # }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "random" {
  # No additional configuration needed
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
  zone    = "${var.region}-a"
  #zone   = "us-central1-a"
}
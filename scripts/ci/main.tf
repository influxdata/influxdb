terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 2.70"
    }
  }
}

####################
# Declare variables

# Variables without default values
# these variables need to be changed - see terraform.tfvars
variable "key_name" { }
variable "package_path" { }
variable "instance_name" { }

# Variables with default values
variable "additional_files_dir" {
  type = string
  default = ""
}

variable "instance_type" {
  type = string
  default = "t3.micro"
}

variable "region" {
  type = string
  default = "us-west-2"
}

####################
# Declare data
locals {
  additional_files_dest = "/home/ubuntu/files"
  package_path = "/tmp/workspace/packages"
  ubuntu_home = "/home/ubuntu"
  ubuntu_user = "ubuntu"
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

####################
# Declare resources
provider "aws" {
  profile = "default"
  region  = var.region
}

# The security group defines access restrictions
resource "aws_security_group" "influxdb_test_sg" {
  ingress {
    description = "Allow ssh connection"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow all egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# The data node for the cluster
resource "aws_instance" "test_node" {
  count = 1

  ami           = data.aws_ami.ubuntu.id
  instance_type = var.instance_type
  key_name = var.key_name
  vpc_security_group_ids = [aws_security_group.influxdb_test_sg.id]

  tags = {
    Name = var.instance_name
  }

  provisioner "file" {
    source = var.package_path
    destination = "${local.ubuntu_home}/influxdb.deb"

    connection {
      type     = "ssh"
      user     = local.ubuntu_user
      host     = self.public_dns
      agent    = true
    }
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir -p ${local.additional_files_dest}",
    ]

    connection {
      type     = "ssh"
      user     = local.ubuntu_user
      host     = self.public_dns
      agent    = true
    }
  }

  provisioner "file" {
    source = var.additional_files_dir
    destination = "${local.additional_files_dest}"

    connection {
      type     = "ssh"
      user     = local.ubuntu_user
      host     = self.public_dns
      agent    = true
    }
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x ${local.additional_files_dest}/*.sh",
    ]

    connection {
      type     = "ssh"
      user     = local.ubuntu_user
      host     = self.public_dns
      agent    = true
    }
  }
}

####################
# Declare outputs
output "test_node_ssh" { value = aws_instance.test_node.0.public_dns }

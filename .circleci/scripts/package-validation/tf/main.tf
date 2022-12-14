terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 2.70"
    }
  }
}

variable "architecture" {
  type = string
}

variable "identifier" {
  type = string
}

variable "package_path" {
  type = string
}

provider "aws" {
  region = "us-east-1"
}

data "aws_ami" "centos" {
  most_recent = true

  # This information is sourced from https://wiki.centos.org/Cloud/AWS
  # and should pull the latest AWS-provided CentOS Stream 9 image.
  filter {
    name   = "name"
    values = [format("CentOS Stream 9 %s*", var.architecture)]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["125523088429"]
}

resource "aws_security_group" "influxdb_test_package_sg" {
  ingress {
    description = "Allow ssh connection"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow all outgoing"
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "centos" {
  count                  = 1
  ami                    = data.aws_ami.centos.id
  instance_type          = var.architecture == "x86_64" ? "t2.micro" : "c6g.medium"
  key_name               = "circleci-oss-test"
  vpc_security_group_ids = [aws_security_group.influxdb_test_package_sg.id]

  tags = {
    Name = format("circleci_%s_centos_%s", var.identifier, var.architecture)
  }

  provisioner "file" {
    source      = var.package_path
    destination = "/home/ec2-user/influxdb2.rpm"

    connection {
      type     = "ssh"
      user     = "ec2-user"
      host     = self.public_dns
      agent    = true
    }
  }

  provisioner "file" {
    source      = "../validate"
    destination = "/home/ec2-user/validate"

    connection {
      type     = "ssh"
      user     = "ec2-user"
      host     = self.public_dns
      agent    = true
    }
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/ec2-user/validate",
    ]

    connection {
      type     = "ssh"
      user     = "ec2-user"
      host     = self.public_dns
      agent    = true
    }
  }
}

output "node_ssh" {
  value = aws_instance.centos.0.public_dns
}

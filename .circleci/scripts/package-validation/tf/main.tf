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

data "aws_ami" "test_ami" {
  most_recent = true

  filter {
    name   = "name"
    values = ["al20*-ami-20*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
  filter {
    name   = "architecture"
    values = [var.architecture]
  }

  owners = ["137112412989"]
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

resource "aws_instance" "test_instance" {
  count                  = 1
  ami                    = data.aws_ami.test_ami.id
  instance_type          = var.architecture == "x86_64" ? "t2.micro" : "c6g.medium"
  key_name               = "circleci-oss-test"
  vpc_security_group_ids = [aws_security_group.influxdb_test_package_sg.id]

  tags = {
    Name = format("circleci_%s_test_%s", var.identifier, var.architecture)
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
  value = aws_instance.test_instance.0.public_dns
}

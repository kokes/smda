provider "aws" {
  region = "eu-central-1"
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

resource "aws_iam_role" "range_role" {
    name = "range_testing"
    assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_instance_profile" "range_instance_profile" {
    name = "range_instance_profile"
    role = aws_iam_role.range_role.id
}

resource "aws_iam_role_policy" "range_role_policy" {
  name = "range_role_policy"
  role = aws_iam_role.range_role.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::okokes-ranges"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject"
      ],
      "Resource": ["arn:aws:s3:::okokes-ranges/*"]
    }
  ]
}
EOF
}

resource "aws_key_pair" "keypair" {
  key_name   = "okokes_personal"
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDs1/qftqKrzxK72esYXaYbnbfRoFxybQMWhIORG2op10YD2xgN4jCVKBXpXtwMPip+qdSUUy6mCthI051ZqJBSNoEuhalCVAUkMYvooxshX39pOT/LhsCpwtEB2WDZggRPx8yMfNmsXosK5nA+wjAKxCvR264ExfkLiNfhOWJ+l+/bvwI0uYGPUc9NEkC2IBN3T8GEKB7PK8GVGZ+4UTZg9PfTpIvPe6RIMqN2f8Pad3/2cMSwpxOjdqj4KS5SnsAeXS/1M94Hvf6w6sE1ZwiHkW3srVIcAJqZgK/l3N5ro17tMKjzXOQdkgHM6AmtGxl0w5YlM4myOs65xApOTdv6w0XR0zw2dsGUrWC43yBA2KKw1R583u9ftooH9TNyQ2le6bj4np69T+tBVmU9G9WBZaHASF/B5XRSYlwU/Rx2rig02qwK7PkBTbx4KbDKMGl/4q1Cdz98KtPVEaMEvtH2oHLPpsCxriQ3qC+o4l8Jk8fR0WewqHk+LbW5B29yT1M="

  tags = {
    Name = "s3_ranges"
  }
}

resource "aws_security_group" "ssh_sg" {
  name = "s3_ranges_access"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "s3_ranges"
  }
}

resource "aws_s3_bucket" "bucket" {
  bucket = "okokes-ranges"
  acl    = "private"

  tags = {
    Name = "s3_ranges"
  }
}

resource "aws_instance" "s3_ranges" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = "c5.large"
  key_name                    = "okokes_personal"
  associate_public_ip_address = true

  vpc_security_group_ids = [
    aws_security_group.ssh_sg.id
  ]

  iam_instance_profile = aws_iam_instance_profile.range_instance_profile.id

  tags = {
    Name = "s3_ranges"
  }
}

output "instance_ip" {
  value = aws_instance.s3_ranges.public_ip
}

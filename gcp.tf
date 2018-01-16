variable "project" {}
variable "region" {}
variable "home_ip" {}
variable "user" {}

provider "google" {
  credentials = "${file("account.json")}"
  project     = "${var.project}"
  region      = "${var.region}"
}

resource "google_compute_firewall" "allow-ssh-home" {
  name = "allow-ssh-home"
  network = "default"

  allow {
    protocol = "tcp"
    ports = ["22"]
  }

    source_ranges = ["${var.home_ip}"]
}

// Create a new instance
resource "google_compute_instance" "default" {
  name = "test"
  machine_type = "f1-micro"
  zone = "us-central1-a"
   
  boot_disk {
    initialize_params {
      image = "ubuntu-1710"
    }
  }

  network_interface {
    network = "default"

    access_config {
      nat_ip = ""
    }
  }

  metadata {
    sshKeys = "${var.user}:${file("~/.ssh/id_rsa.pub")}"
  }

  service_account {
    scopes = ["storage-rw"]
  }

  provisioner "file" {
    source = "~/Repos/diachronic"
    destination = "/home/${var.user}"
    connection {
    type = "ssh"
    user = "${var.user}"
    private_key = "${file("~/.ssh/id_rsa")}"
    timeout = "45s"
    }
  }

  provisioner "remote-exec" {
    inline = [
    "sudo apt-get -y install p7zip-full python3-pip",
    "pip3 install -r ~/diachronic/requirements.txt"
    ]
    connection {
    type = "ssh"
    user = "${var.user}"
    private_key = "${file("~/.ssh/id_rsa")}"
    }
  }

}

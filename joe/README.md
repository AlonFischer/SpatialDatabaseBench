# CS6400 Project

## Getting Started

Install docker

Create a docker group and add your user to it:
```
sudo groupadd docker
sudo usermod -aG docker $USER
```

Install pip and venv:

```
sudo apt install python3-pip python3-venv
```

Install pipx:

```
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```

Install virtualenv:

```
pipx install virtualenv
```

Create a virtualenv:

```
virtualenv venv
```

Activate the virtual environment:

```
source venv/bin/activate
```

Install required packages:
```
pip install -r requirements.txt
```

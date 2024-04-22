# Docker for Data Engineers

Code for blog at: https://www.startdataengineering.com/post/docker-for-de/

In order to run the code in this post you'll need to install the following:
 
1. [git version >= 2.37.1](https://github.com/git-guides/install-git)
2. [Docker version >= 20.10.17](https://docs.docker.com/engine/install/) and [Docker compose v2 version >= v2.10.2](https://docs.docker.com/compose/#compose-v2-and-the-new-docker-compose-command).

**Windows users**: please setup WSL and a local Ubuntu Virtual machine following **[the instructions here](https://ubuntu.com/tutorials/install-ubuntu-on-wsl2-on-windows-10#1-overview)**. Install the above prerequisites on your ubuntu terminal; if you have trouble installing docker, follow **[the steps here](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-22-04#step-1-installing-docker)** (only Step 1 is necessary). Please install the **make** command with `sudo apt install make -y` (if its not already present). 

All the commands shown below are to be run via the terminal (use the Ubuntu terminal for WSL users).

```bash
git clone https://github.com/josephmachado/docker_for_data_engineers.git
cd efficient_data_processing_spark
# start containers
docker compose up --build -d --scale spark-worker=2
docker ps # see list of running docker containers and their settings
# stop containers
docker compose down
```

# Testing PySpark Applications

Code for blog at: https://www.startdataengineering.com/post/test-pyspark/

## Create fake upstream data

In our upstream (postgres db), we can create fake data with the [datagen.py](./capstone/upstream_datagen/datagen.py) script, as shown:

```bash
docker exec spark-master bash -c "python3 /opt/spark/work-dir/capstone/upstream_datagen/datagen.py"
```

## Run simple etl

```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/work-dir/etl/simple_etl.py
```

## Run tests

```bash
docker exec spark-master bash -c 'python3 -m pytest --log-cli-level info -p no:warnings -v /opt/spark/work-dir/etl/tests'
```
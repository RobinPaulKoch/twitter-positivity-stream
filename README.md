
<h1 align="center">
# twitter-positivity-stream
</h1>


<h1 align="center">
	<img
		width="300"
		alt="The Lounge"
		src="https://github.com/RobinPaulKoch/twitter-positivity-stream/blob/master/images/tweepybirb.jpg">
</h1>

<h3 align="center">
	A streamer built on top of the Twitter Python API "Tweepy"
</h3>


<p align="center">
</p>

## Overview

- ** A Data Engineering project.
- ** Features a data architecture to stream tweets based on provided topic.
- ** Example of Integration with a MySQL database
- ** Scheduling framework based on Luigi (TO DO)
- ** A scalable multithreading approach to the workload provided (TO DO)
- ** Dockerfile provided for containerization
- ** Pipenv provided for ease of installation (requires Python 3.7)


## Installation and usage

I recommend installing from source

### Running from source

The following commands install and run the development version of The Lounge.

#### 1. First clone into the project
```sh
git clone https://github.com/RobinPaulKoch/twitter-positivity-stream.git
cd twitter-positivity-stream
```

#### 2. Then install the virtual environment with:
```sh
pipenv install Pipfile
pipenv shell
```

#### If instead you want to directly deploy the application as a container I recommend standing in the root folder and running:
```sh
docker build --tag twitter-positivity-stream .
```
#### NOTE:

⚠️ When built with Docker you will probably have to set up a "MySQL" image yourself


## Goal of the application

Simply follow the instructions to run The Lounge from source above, on your own
fork.

## Architecture of the solution

## Scalability

## Change Management and new releases

## Scheduling & Deployment

## Plans for further releases

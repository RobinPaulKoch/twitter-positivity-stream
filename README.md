
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
- ** Scheduling framework based on Luigi 
- ** A scalable multithreading approach to communicating with the Tweepy API
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

#### If instead you want to run the workflow locally on the computer using the Python Luigi package you can run the following command from the terminal standing the root directory of where you installed the project:

```sh
python -m luigi --module Luigi_datamodel DataPipeline --local-scheduler
```

By removing the parameter "--local-scheduler" the workflow will run via the configured port on your computer and is traceable via the Luigi interface.


## Goal of the application

The goal of this application is to provide a scalable Python workflow on how to stream tweet data into a MySQL database from Twitter's provided Tweepy API. Besides this application features how to apply sentiment and subjectivity analysis to the extracted tweets using TextBlob's provided pretrained lexicon-based network. 

## Architecture of the solution
ADD SOLUTION DIAGRAM

## Scalability
### Threading
This twitter streaming package provide the option to multithread the call to the API.

### Mutliprocessing + Containerized parallel deployment
A container of this application can be build using the Dockerfile or one can write his or her own docker-compose.yml file to build from. The workload can then be divided over multiple containers using global parameters, such as retrieving different time periods of tweets or different search queries of tweets. 
In a further release I want to explore division of the workforce and parallel execution using the existing Luigi file and the containerized application.

## Change Management and new releases
DevOps:
Develop features on Git Develop branch -> Pull request to merge from master branch -> testing master version -> new release in form of Git Tag -> Git Tag can be containerizsed and deployed on the cloud as running application or locally.

###NOTE: No testing framework is yet set up to test the master branch. This I plan for further releases

## Scheduling & Deployment
The application can be deployed on the cloud. Scheduling is easiest to be approximately every 15 to 30 minutes due to the limited API request limit of the 100 last tweets.
I plan to functionalities to "fill the gap" if no tweets were retrieved for a period longer than 15 to 30 minutes.

## Plans for further releases
- Parallel script running with workflow management
- dynamically looping the API Search request to fill the gap between last tweet id stored in a database and recent tweets to not have any gaps in the database.
- In-depth guide on how to appoint global parameters to Docker Containers to divide workload 

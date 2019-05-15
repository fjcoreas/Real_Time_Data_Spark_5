# Lab

## Downloading laboratory boilerplate

- Clone repo an move to downloaded folder
 
```terminal
git clone https://gitlab.com/liesner/bts-rtda-lab-5.git
cd bts-rtda-lab-5
```

## Data sets

- The data is a variation of  [population-city](https://github.com/datasets/population-city) data-sets.
    - ```data/city_male_population``` witch contain tha male population distributed by city
    - ```data/city_female_population``` witch contain tha female population distributed by city
- Each comma separate value on the rows of both data sets correspond to the headers: 

```terminal
Country or Area
Year
Area
Sex
City
City type
Record Type
Reliability
Source Year
Value
Value Footnotes
```    


## You can find here the lecture steps for references
[Lecture steps to reproduce](docs/conference_steps.md)


## Practical activity

## First some boilerplate explanations

```
app (.)
|   built.sbt   **sbt configuration file, contains all dependency of the app**
|   docker-compose.yaml     **docker-compose configuration file**
|   Dockerfile  **docker build file**
|   Readme.md 
|   deploy.sh **script to submit spark app on local cluster, to be runned inside docker container**
|   Data
|   |  city_male_population **distribute text file dataset with male population by year**
|   |  city_female_population **distribute text file dataset with female population by year**
|   src
|   |   main
|   |   |   scala
|   |   |   |   cli
|   |   |   |   |   App.scala **Main app class, contain main(...) method, app entry point
|   |   |   |   models
|   |   |   |   |   CityPopulationEntry.scala   **case class to represent a population entry in dataset**
|   |   |   |   services
|   |   |   |   |   CityPopulationProcess.scala     **Main processing class**
|   |   test
|   |   |   |   cli
|   |   |   |   |   AppTest.scala
|   |   |   |   models
|   |   |   |   |   CityPopulationEntryTest.scala
|   |   |   |   services
|   |   |   |   |   CityPopulationProcessTest.scala
```


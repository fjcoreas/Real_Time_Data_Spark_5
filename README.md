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

## Assignment

Note: 
- For all assignment input files will be the same ```data/city_male_population``` and ```data/city_female_population```. You always need to create an output file with the result of operations in the folder ```tmp/```:
    - The app should receive as parameters the input file path "./data/city_female_population" "./data/city_male_population" and the output path "tmp/output_name"    

- Sample of input file
```terminal
data/city_male_population

"Kazakhstan",2003,"Total","Male","Rudni","City proper","Estimate - de facto","Final figure, complete",2004,47320,
"Kazakhstan",2003,"Total","Male","Rudni","Urban agglomeration","Estimate - de facto","Final figure, complete",2004,52951,
"Kazakhstan",2003,"Total","Male","Semipalatinsk","City proper","Estimate - de facto","Final figure, complete",2004,120918,
"Kazakhstan",2003,"Total","Male","Semipalatinsk","Urban agglomeration","Estimate - de facto","Final figure, complete",2004,134910,
"Kazakhstan",2003,"Total","Male","Shimkent","City proper","Estimate - de facto","Final figure, complete",2004,241324,
"Kazakhstan",2003,"Total","Male","Shimkent","Urban agglomeration","Estimate - de facto","Final figure, complete",2004,241324,
"Kazakhstan",2003,"Total","Male","Taldykorgan","City proper","Estimate - de facto","Final figure, complete",2004,43574,
"Kazakhstan",2003,"Total","Male","Taldykorgan","Urban agglomeration","Estimate - de facto","Final figure, complete",2004,53921,
"Kazakhstan",2003,"Total","Male","Taraz","City proper","Estimate - de facto","Final figure, complete",2004,149143,
"Kazakhstan",2003,"Total","Male","Taraz","Urban agglomeration","Estimate - de facto","Final figure, complete",2004,149143,
"Kazakhstan",2003,"Total","Male","Temirtau","City proper","Estimate - de facto","Final figure, complete",2004,73390,
"Kazakhstan",2003,"Total","Male","Temirtau","Urban agglomeration","Estimate - de facto","Final figure, complete",2004,78024,
"Kazakhstan",2003,"Total","Male","Uralsk","City proper","Estimate - de facto","Final figure, complete",2004,87388,
"Kazakhstan",2003,"Total","Male","Uralsk","Urban agglomeration","Estimate - de facto","Final figure, complete",2004,100739,
"Kazakhstan",2003,"Total","Male","Ust-Kamenogorsk","City proper","Estimate - de facto","Final figure, complete",2004,134246,
"Kazakhstan",2003,"Total","Male","Ust-Kamenogorsk","Urban agglomeration","Estimate - de facto","Final figure, complete",2004,139247,
```

0- Add the functionality to get total count of females and males grouped by common year component (output_name = "total_count_grouped_by_common_year_component")
1- Add the functionality to get the distinct cities on female collection (output_name = distinct_city_female)
2- Add the functionality to get the distinct cities on both collections (output_name = distinct_city_all).
3- Add the functionality to get total count of people, male or female by year (output_name = total_count_by_year)
4- Add the functionality to get the total count of people that live on Urban Agglomeration by year.  

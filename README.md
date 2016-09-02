# bosch-spark

## Context :

This project uses the data of the bosch Kaggle challenge 
https://www.kaggle.com/c/bosch-production-line-performance/

Bosch records data at every step along its assembly lines.

In this competition, Bosch is challenging Kagglers to predict internal failures using thousands of measurements and tests made for each component along the assembly line. 
This would enable Bosch to bring quality products at lower costs to the end user.

## What's the goal of this Java project ?

The goal is to discover Bosch data !

Goal of this program : Find the list of all the components, which are manufactured in the same lines and stations.

### Steps :
  - Load Numerical Data
  - Find for each component (in the numerical file) the list of lines and stations where the component is manufactured
  - Load Categorical Data
  - Find for each component (in the categorical file) the list of lines and stations where the component is manufactured
  - Merge the 2 lists in order to find the list of all the components, which are manufactured in the same lines and stations.

<< There is about 7927 different paths for manufacturing all the components >>

## Set up

The program is developped in Java, in Spark 1.6.
The compilation is done my Maven.

This program is configured to run in local mode.

## Data
  - Train Numeric
  - Train Categorical
  - 
  
## TODO
  - Add comment
  - Add new features to discover data... 

# That's all for today ! Enjoy ;-)

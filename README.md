# bosch-spark

## Context :

This project uses the data of the bosch Kaggle challenge 
https://www.kaggle.com/c/bosch-production-line-performance/

Bosch records data at every step along its assembly lines.

In this competition, Bosch is challenging Kagglers to predict internal failures using thousands of measurements and tests made for each component along the assembly line. 
This would enable Bosch to bring quality products at lower costs to the end user.

## What's the goal of this Java project ?

The goal is to discover Bosch data !

Goal of this program : Find the list of all the components, which are manufactured in the same machines (identified by the line and the station).

### Steps :
 - Load Numerical Data
 - Find for each component (in the numerical file) the machines list where the component is manufactured
 - Load Categorical Data
 - Find for each component (in the categorical file) the machines list where the component is manufactured
 - Merge the 2 machine lists in order to find the complete machines list where the component is manufactured. And finally, know all the components which share the same machines.
 
<< There is about 7927 different paths for manufacturing all the components >>

## Set up

The program is developped in Java, in Spark 1.6.
The compilation is done my Maven.

This program is configured to run in local mode.

That take about 25min to execute this program on my own computer ( 16Go RAM) .
  
## TODO
  - Add comment
  - Add new features to discover data... 

# That's all for today ! Enjoy ;-)

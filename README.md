## Environment and platform
The mainly environment I use for this project is PySpark based on Jupyter Notebook. Since I'm using a Windows PC, I created a Docker container by the image of pyspark-notebook. The image information is listed below:
https://hub.docker.com/r/jupyter/pyspark-notebook/

To test the program, I also installed a complete Spark environment on a virtual machine of Ubuntu:
Python: 3.7.6    Hadoop: 2.7  Spark: 2.4.5


## How to run
Use command './run236.sh [Stations file path] [Recordings directory path] [Output directory path]'
In case you get some errors running this script(due to path or environment settings), you can open 'main.ipynb" too see the result of every step and open 'result.txt' to see the final result :)


## Project Description
Some assumptions: 
1. On one day(e.g. Jan. 1st), in one state(e.g. CA), there are more than one stations recorded the percipitation. I computed the average of these records.
2. For the last digit of PRCP, I use the method 'Simply multiply' as you provided in the Project Summary.
3. The way I compute the average PRCP of one month: Sum the PRCP of every day in this month. Sum 4 of these months in 4 years(e.g. Jan. 2006+ Jan. 2007 + Jan. 2008+ Jan. 2009). Then divided by 4 to get the average PRCP of this month. (avg Jan.)

The overall goal is to find the state in US that has the most stable rainfall(minimum difference between the months with the highest rainfall and the lowest rainfall). To achive this, I divided the program into three part:
  1. Use Map and Filter operations to get the stations in US
  2. Use Union operations to aggregate the 4 recordings file. Use Join operations to join the recordings and stations we found in the first step.
  3. Use Map and Reduce operations to compute the average percipitaion of a month in a state. Use Sort opertions to sort the result, and get the difference between min and max.


## Description of each step
  1. Read WeatherStationLocations.csv, use filter() to find stations in US. Use Map() to select 'USAF' and 'ST' colomns.
  Output: [USAF, ST]

  2. Union the 4 year recordings use sc.union(). and remove the header. Join the result with the stations in step1.
  Output: [USAF, ST, DATE, PRCP]

  3. Use map() to get the last digit of PRCP and compute the actual percipition. Use mapValues() and reduceByKey() to compute the average percitipation of one day in one state. 
  Output: [ST, Month, PRCP] (Same ST&Month will appear X times. X is the amount of days in this month)

  Use reduceByKey() to sum the percipitaions of one day and get the monthly percipitation.
  Output: [ST, Month, PRCP] (Same ST&Month will apear 12 times]

  Sort the result use sortBy() to get the max PRCP and min PRCP. And compute the difference.
  Output: [ST, diff]

  Finally use join() to put the result together. Output the result into a file.
  Output: [ST, max_month, max_PRCP, min_month, min_PRCP, diff]


## Total Runtime
218.55s
  

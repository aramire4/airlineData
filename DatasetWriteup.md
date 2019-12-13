  My final project utilized 3 datasets. Airline data from stat-computing.org that has basic flight information for 
individual flights. This data includes fly date, departure time, arrival time, delay time, reason for delay, cancellation, 
flight time, distance, and the airport codes for origin and destination. http://stat-computing.org/dataexpo/2009/the-data.htm 
(Dataset 1). Another dataset used was taken from academic torrents which were US domestic flights from 1990 to 2009. 
This data set was more generalized than the previous one. It had data that dealt with flights between two places over a day. 
This included: origin city, destination city, the amount of passengers, the amount of seats available, the amount of 
flights flown in that time, the distance of the flight, the fly date, and population of the origin and destination. 
http://academictorrents.com/details/a2ccf94bbb4af222bf8e69dad60a68a29f310d9a (Dataset 2). The final dataset I used was 
openflight.org’s Airport location data. This had data about the location of a given airport like the airport name (& code), 
longitude, and latitude of that airport. https://openflights.org/data.html (Dataset 3). 
I chose to use airline and flight data because it seemed like it could reveal a lot about a wide variety of things, 
specifically growth over time, trends, and how different aspects of the data affected the dataset as a whole.

The first questions I thought to answer with this data set was basic ones. Which airports have the most traffic? 
Which time of year has the most travel? To answer the first of these questions (which airport had the most traffic) I 
grouped Dataset 2’s dataframe by ‘origin and ‘fly_date and then aggregated the sum of flights. I did the same to ‘destination 
and ‘fly_date, and then joined both groupings where ‘origin === ‘destination and ‘fly_date column was equal. This gave me the 
total number of flights for a given airport split up by year, I sorted this on the number of flights and found that the 
Chicago airport had by far the most flights yearly. To find what time of year had the most travel, I grouped on month and 
aggregated the total number of flights. I did this for each year in the dataset, and then ordered each by the number of 
flights to find that winter holiday travel was not as highly travelled as I initially hypothesized. I graphed this change 
in two ways, one over all the airports and one for San Antonio. For all airports I got Dataset 3 and used this to plot a dot 
at the airport’s latitude and longitude coordinates, changing the size to match the number of flights that came out of that 
airport that year.I graphed this change in travel over the years for San Antonio airports to visualize what this meant for my 
home town city. Finding clear patterns that could be linked to the city’s history. 

Speaking of San Antonio, the other portion of what I look for in this dataset were data that could reveal things about my 
hometown's airport. I wanted to answer questions like: where do San Antonians travel the most? What cities travel to 
San Antonio the most? And how has the San Antonio airport changed over time? To answer where San Antonians were travelling 
the most, I first filtered Dataset 2 to where San Antonio was the origin city for the beginning and last years of the dataset 
(1990 and 2009), I then did a group by on destination cities and aggregated the number of flights. I ordered by the number of 
flights only to find that the cities travelled to the most were overwhelmingly Dallas each month and year. I used this 
procedure to answer the question of where San Antonians were travelling the most. This time, the answers were a little more 
evenly spread. I found that San Antonians travel to Dallas, Houston, Atlanta, Chicago, Phoenix, and Las Vegas the most. 
I chose to answer the last question by visualizing the amount of flights travelled by month in San Antonio by plotting dots on 
a graph which demonstrated the amount of flights (y axis) and flight time (yyyymm on the x axis) . This showed a slow but 
steady increase which rose significantly in 2003 (which corresponded to the increase in parking structures and other 
infrastructure that the airport added in that year) and a sizable decline in 2009 (which corresponds to the loss of  business 
in that year to other state airports).

Lastly I set out to use machine learning to try and predict certain things about flights. For my first machine learning 
algorithm, I chose to use a linear regression on various fields in Dataset 2 to try and predict the number of flights that 
are going between those two cities. For the fields, I chose to use power sets of 3 using the fields origin population, 
destination population, seats available, and passengers. There was an average error in the predictions of 14.581, not the 
best error but I think if I had utilized other fields in the data the resulting error could have been lower. 

The second machine learning algorithm I used was a clustering algorithm that was initially supposed to help me predict the 
delay time given NOAA temperature data along with Dataset 1. I ran into some problems here, mainly joining the NOAA data with 
Dataset 1. NOAA stores the temperature data in a way that makes it joinable with the weather station data, however this data
did not sit well when joining with Dataset 1. This was mainly due to the naming scheme used by both. NOAA set the station 
name and the city name while Dataset 1 grouped flights with airport codes and city names that did not necessarily match up 
with all of the NOAA data. The best way I could think to group this was using a udf that got the difference in latitude and 
longitude to find the closest station to a given airport, but testing this took forever. So instead I elected to predict 
whether a flight was cancelled or not based on a clustering of the month, the latitude, and cancellation. I tried to do this
with a differing number of k-means sets, and found that latitude did not really have any bearing on the result of the cluster 
but month did have some correlation. The algorithm grouped the month January with a higher chance of a flight being cancelled. This had a silhouette score of 0.4138, meaning the cluster groupings were fairly similar.

Although some of my data was hindered by ability to join it, I found my results to still be enlightening towards trends in 
U.S. domestic flights. I found changes over the years, trends in the yearly data that corresponds to real world events, and 
answers to most of the questions I originally asked these datasets to answer. I believe that if I had a better understanding 
for some of the errors that I got when initially trying to do some of the more difficult problems, like the machine learning 
problems, I could have had a better answers. However, I am pleased with how the data turned out and what it reveals.

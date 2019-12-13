A review of all the questions I initially chose to answer from each dataset I chose at the beginning of the year.

https://data.world/data-society/european-soccer-data
- European Soccer Data
    - This is a big data set that contains a lot of things pertaining to European soccer. It of course includes information on matches (wins, losses, ties, penalties, etc.), along with individual player data (stats, Fifa ratings, etc.).  It also includes data on things that may not necessarily pertain to a team winning a game, starting in 2016 the dataset has information about the Fifa video game’s player statistics. I am not necessarily a fan of soccer, but I do recognize how popular it is. This popularity is no doubt what caused this dataset to be as huge as it is (300 MB).

- Is there a correlation to the types of fouls and certain teams?
    - This could have been one of the machine learning algorithms. I could have used a clustering on all the teams and the foul types to find which fouls were grouped with with team.
    
- Does receiving certain fouls make a team more likely to lose a game?
    - I could have used this question for my other machine learning algorithm. I could have done a linear regression on the types of fouls, and the amount of each and tried to predict the outcome of the game based on this. This question might have been a little harder to interpret with the data given.
    
- Do adding certain people make a team better or worse?
    - For this question I was initially thinking how a team’s composition affects their chances of winning (e.g. win totals for the year). This would have been a harder question to answer because of the different ways I could go about answering it. One idea I had was to compare a given team from one year, and then compare the team with itself from the next year and see if adding certain players affected their win totals.
    
- What position is the most important for a roster?
    - This question could have just looked at average plus-minus of player by position, but I could have also gone more in depth to quantify what makes a player at a position important to the roster.
    
- What positions require the most skill to play (i.e. which position has the best players)?
    - My initial thoughts for this question was to just find which position had the biggest number of star players. Though in the dataset, the only way I could think to do this is to match on the names of the top 50 or so best soccer players in Europe and find the average position. Another way to do it would be to see which position scores the most goals per game.
    
- Is one team more likely to win a game over another team?
    - For this I would need some sort of equation that takes in player stats for each team and predicts the outcome of the game. I could have also used a machine learning algorithm, but I am not sure how that would be applied to this particular question.
    
- Does a home field advantage have an effect on the outcome of a game?
    - A simple check to see how many wins occur when a team plays at home vs when they play at another stadium.
    
- Which team has the best home field advantage?
    - A simple call to see which team has the most wins when playing on their home field.

- How have teams’ win records changed over an extended period of time?
    - I could have used this question for one of my visualizations, and easily plot a teams wins over time.

- Which countries are the most dominant in the sport?
    - Querying the dataset to find which teams have the most recent wins, all time wins, and best win percentage.
    
http://stat-computing.org/dataexpo/2009/the-data.html
Airline data
http://academictorrents.com/details/a2ccf94bbb4af222bf8e69dad60a68a29f310d9a
US Domestic Flights
http://stat-computing.org/dataexpo/2009/supplemental-data.html
Airport codes
https://openflights.org/data.html
Airport codes and locations

This data set contains basic ticket information for flights from 1990 to 2009, including: destination city, origin city, and fly date. It also has more information that would not usually be included with the ticket information like the number of passengers, number of flights, and the population of the destination and origin at the time of the flight. All of this data can reveal some interesting information on areas of the U.S. along with some travel trends

- Which airport receives the most/least traffic?

- Which places receive seasonal spikes in travel?
    - I tried to answer this question, but I could not really find an elegant way of doing it. My initial thought was to group by airport, and then group that by fly date (yyyymm). Using this I would find the difference from month to month and average them by month to find a seasonal spike. I chose not to implement this due to time constraints
    
- Have there been breaks in the usual trends?
    - Using my hypothetical answer from the last question, I could find spans of the year that did not fall in line with the usual yearly spike.

- Over the years have some places gradually gained or lost travel?
    - I answered this question for some cities, like San Antonio, but in hindsight I would have liked to see which airport had the biggest jump in travel through it. 

- Does the population of an area correlate to travel to that area?
    - I used a regression to answer this question but my answer was not what I expected. I would have liked to try answering this with more fields thrown into the regression.

- How many people travel yearly? Daily? Weekly? Has this changed over the years?
- What are the longest and shortest flights?

- How long are individual planes used? (daily, yearly, through the dataset). Has this changed through the years?
    - This answer to this question was immediately proven unattainable to me. The RITA data was supposed to have the tail number of the plane with each flight entry, however very few entries actually had this information included.

- Can we predict the delay time based on the temperature at the origin and destination city?
    - I would have liked to find a way to join the temperature data with the flight data to actually do this question, but the data was being finicky.
- What time of year has the most flights?
Graphs:
- Stations in the U.S. graphed by latitude and longitude and circle size is correlated to the number of flights
- Individual flights yearly graphed as lines from origin to destination, possibly colored by time of year
- Change in flights over years (bar chart)
  - Bar charts were not working for me for some reason but I made do with scatter plots.

YouTube Trending Videos
https://www.kaggle.com/datasnaek/youtube-new
- This dataset contains information on multiple YouTube trending videos split up by the countries that they were most trending in. The fields of interest that this dataset has include: trending date, title of the video, channel name, category, publish, tags, views, and likes. The dataset is constantly being updated with more video information along with more countries’ data being added regularly.

- Are there common occurrences among the trending videos (similar tags, certain titles, or categories)?
  - This question could have been expanded upon to fit my machine learning question. I could have clustered the tags to find with ones correlated with trending videos. The only problem with this being that it is difficult to cluster strings, meaning I may have had to map my tags to numbers before clustering.

- Is there a correlation between category and view count?
    - Do certain types of videos get more views? This question could have been answered by doing a group by on category and aggregating the total view count. I could have also visualized this data to show this potential correlation.

- Do different countries’ trending videos give any insight to anything about the country?
    - This would have been an interesting question to answer, I could have used the method I used in the last question, but modifier to also be grouped by country. This could have given an insight as to what different countries find entertaining.

- How are other countries’ trending videos different from the trending videos in the U.S.
    - This question could have been grouped with the previous question. Printing the results of the query for each country’s top trending videos would have given a way to compare them to the U.S.’s trending videos.

- Have certain categories/tags gained popularity over the time?
    - Checking to see how many more videos and views a certain category or tag has gotten over the period of the dataset

- Can we predict how many views a given video will get given the channel that publishes it, the category, and tags?
    - This would have been my other machine learning algorithm. I would have liked to do a regression on the video category, tags, and other fields as I see fit in order to find the number of views. This question would have needed a little bit of tweaking because I feel like more fields would determine what makes a video popular. Potentially time of year having an influence in what tags are popular. This question could have been split up by month and by year.

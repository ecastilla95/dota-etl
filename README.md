# DotaETL

DotaETL is a small project that analyzes data from Dota's Public API.
It uses features from the Scala Lang library and popular Scala libraries to showcase some of the experience of the developer.

## Usage

The program only requires a single user input, which is the number of matches that will be analyzed.
This input should be an input between 1 and 20.

The program will output to console information similar to the following:

```21/08/28 19:17:36 INFO DotaETL:
Logging data for Dota games of user YrikGood:
{
  "max_kda" : 18,
  "min_kda" : 0.5,
  "avg_kda" : 3.191672077922078,
  "max_kp" : 77.77777777777779,
  "min_kp" : 17.77777777777778,
  "avg_kp" : 49.70551963116619,
  "games_as_radiant" : 11,
  "radiant_max_kda" : 18,
  "radiant_min_kda" : 0.5,
  "radiant_avg_kda" : 3.9729437229437226,
  "games_as_dire" : 9,
  "dire_max_kda" : 5.25,
  "dire_min_kda" : 0.5454545454545454,
  "dire_avg_kda" : 2.2367845117845118
}

21/08/28 19:17:36 INFO DotaETL: Matches studied: 20
21/08/28 19:17:37 INFO DotaETL: Elapsed time: 25704878800ns
```

## Data schema

We describe the previous output as the following:

### Header
Logging data for Dota games of user YrikGood - the game being played (Dota 2) and the user (YrikGood) are at the moment hardcoded features of the program.

### JSON

#### KDA
KDA stands for Kills/Deaths/Assists and is the amount of Kills and Assists of a player divided by their number of Deaths (or 1 if they didn't die)

Consequently, max, min and avg KDA are the maximum, minimum and average KDA of the player given the sample.

The values can be decimal and they can vary from 0 to, virtually, infinity, but it is very rare that any given player has over 50 kills in any given game.

#### KP
KP stands for Kill Participation and is the percentage of takedowns of the player relative to the total number of kills of their team.
A takedown is either a kill or an assist.

Consequently, max, min and avg KP are the maximum, minimum and average KP of the player given the sample.

This fields are percentages, therefore they are decimal numbers that vary from 0 to 100. 

#### Radiant or Dire
In Dota 2 there are two possible sides or teams, Radiant or Dire. Therefore, "games as X" is the number of games played in that team.

Games as X is a whole number between 0 and 20.

KDA is also shown for Radiant or Dire only, so we can get some insight in which side the player has recently performed best.

If zero games are played in any given team, then the count will be 0 and the team specific statistics won't be reported for that side.

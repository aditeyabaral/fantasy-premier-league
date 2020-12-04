# fantasy-premier-league

A project that involves collecting streaming data about English Premier League matches over a TCP socket, and performing real-time analytics on it.
Project was implemented in PySpark, tested on version 3.0.1

This project was submitted as part of the course requirements of the Big Data course (UE18CS322) at PES University, Bengaluru

To run the project, use the inbuilt runner script with the input JSON file as its command line argument, as follows

```
$ ./run.sh <path to input JSON>
```

There are three possible kinds of inputs:

1. Input requesting player profile after all streaming data
2. Input requesting for a particular match details
3. Input requesting for winning chances in Fantasy Premier League given two playing XIs.

The input formats for each can be found at the link below:

[Sample Input](https://drive.google.com/drive/folders/1Ic8W5Ii8cyN9AKysjcg69c4ZpkWAmfAQ)

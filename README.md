# Relevare (latin for relevant)

Getting information is easy, getting relevant information is not - modern data pipelines can help address the modern information deluge.

[Link](#) to your presentation.

<hr/>

## How to install and get it up and running

step 1

step 2

..

step n

<hr/>

## Introduction

Individuals and Organizations are unable to fully benefit from the information overload, on the contrary recent studies suggest negative impact on individual health and poor organizational decision making. We deserve to be served with what is relevant to our interest, passions and benefit. Relevare will demonstrate streaming architecture using data pipeline and microservices that will allow us to build capabilities to filter through and focus on what is relevant for us.

Our pipeline will consume GDELT as representative information dataset. It will also use Twitter data to identify latest dialogue. It will know of basic weather and financial markets information. With 100 million users using our Relavare dashboard/app/bot, we will serve them with timely notification of relevant events of interest.

While consumption of these notifications may be in many forms, we will simply demonstrate this via demo app. This Relavare app will allow us as user to set our interests and preferences and will show the events of our interest. It will also allow user to indicate their like/dislike so we may better tune further what is relevant.

## Architecture

Kafka - message streaming inbound and outbound

Go and Go-kit OR Ballerina.io - microservices

Flask - demo app frontend

S3 - Daily archives of GDELT events

HDFS - persistent data store

MySQL - User repository, Aggregations, Metrics

Spark - Likely for ML .. don't know for sure yet ..

## Dataset

GDELT - Global Database of Events, Language, and Tone

Twitter -

Weather - source TBD

Financial - yahoo/google/bloomberg Major market indexes for each country

## Engineering challenges

GDELT events volume - while good not challenging enough (3000 events per 15mins), will use all historical events load to examine kafka scalabitiy and tuning aspects. In extended implementation of Relevare, we can potentially consumer multitude of news sources live.. we can demonstrate this simply bringing in all historical events as fast as possible and define scaling limits of current implementation.

Weather, Financial and Twitter volume - likely not significant

Outbound notifications:

  Expecting average 3 notifications per day per individual user. 90M * 3 = 270M event notifications per day.

  Expecting average 100 notifications per day per organization. 10M * 100 = 1000M event notifications per day.

  My expectation is this to stress the Kakfa implementation and provide opportunities to understand scale, issues, bottlenecks, improvement opptys.

Geo-Distributed Streaming pipelines - I would like to implement this for High Availability across US east and west. allowing to understand how real world implementations tradeoffs maybe.

ML Engineering - I want to capture the user like/dislike for notifications to train ML models to further tune ... need to see what ML engineering challenges i will show here vs/ actual application of ML.

## Trade-offs

Will know soon for sure.

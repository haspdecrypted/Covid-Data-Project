# Covid-Data-Project
----------------------------

In this project we will analyse over some data available from [Covid Dataset](https://www.covid19india.org/ "Covid Dataset")

To analyse the daily covid data received from GOI website and show that by transforming in the
below format:

<br>

| Date  | State  | Confirmed  | Recovered  | Deceased  | Total tested  |
| ------------ | ------------ | ------------ | ------------ | ------------ | ------------ |
|   |   |   |   |   |   |
|   |   |   |   |   |   |

<br> 

Technological Stacks Implemented: Hadoop, Hive and Spark

<br>

Data Pipeline: 

<br> 

![covid19 dp](https://user-images.githubusercontent.com/87247136/128860228-c455bfee-bbb1-4ce0-b651-cf0cba46943e.jpeg)

<br>

Mapping: Below table shows us the mapping of the data and different files are used to decide where to get
the data of each column,

<br>

| Report Fields  | Source file  | Source field  | Rule  |
| ------------ | ------------ | ------------ | ------------ |
| Date  | Raw Data 25,26,27,28  | Date Announced  | Directly  |
| State  | Raw Data 25,26,27,28  | Detected State  | Directly  |
| Confirmed  | Raw Data 25,26,27,28  | Num Cases  | Aggregated on state  |
| Recovered  | Raw Data 25,26,27,28  | Num Cases  | Aggregated on state   |
| Deceased  | Raw Data 25,26,27,28  | Num Cases  | Aggregated on state  |
| Total tested  | Raw Data 25,26,27,28  | Total Tested  | Convert cumulative to daily count  |

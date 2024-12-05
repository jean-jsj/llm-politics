# Popularity

We calculate the popularity of each search query using two approaches:

(1) **Population level popularity** 
- NAVER DATALAB (https://datalab.naver.com/) provides data on the relative search frequency of individual queries compared to a chosen reference term. This ratio represents how often a query is searched in relation to the reference term's search frequency. A higher relative search frequency ratio indicates greater popularity of the query among users on the platform.

(2) **Sample level popularity**
- We record the total number of times each unique search query was performed by panel members in our dataset during the analysis period, which spans from August 29, 2022, to September 1, 2023. A higher total search count indicates greater popularity of the query among the panel members in our dataset.

### Table 1. Descriptive Statistics
| Variable | Mean | Standard Deviation | Min | Max
|-----:|---------------|-----------|-----------|-----------
|Population level popularity| 3.411 | 11.475 | 0 | 100
|Sample level popularity| 9.339 | 122.044 | 1 | 30953

### Table 2. Correlation
|  | Population level popularity | Sample level popularity | 
|-----:|---------------|-----------|
|**Population level popularity**| 1.0000 | |
|**Sample level popularity**| 0.2240 | 1.0000 |

### Figure 1. Histogram of Population level popularity
> Note: The histogram is based on the min-max normalized data of the original variable

![datalab_popularity](https://github.com/user-attachments/assets/79e57626-c5c6-4b33-905b-74f8e985a8bc)

### Figure 2. Histogram of Sample level popularity
> Note: The histogram is based on the min-max normalized data of the original variable

![sample_popularity](https://github.com/user-attachments/assets/039b9119-268d-446f-a705-3b69e5be4f62)

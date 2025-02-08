# Political Leaning

We calculate the political leaning of each search query using four approaches:

(1) **rw_leaning** = Aggregate data at the query level based on ranking weights; include neutral news domains

(2) **nw_leaning** = Calculate the unweighted average of the data at the query level; include neutral news domains

(3) **rw_nn_leaning** = Aggregate data at the query level based on ranking weights; exclude neutral news domains

(4) **nw_nn_leaning** = Calculate the unweighted average of the data at the query level; exclude neutral news domains

### Table 1. Descriptive Statistics
| Variable | Mean | Standard Deviation | Min | Max
|-----:|---------------|-----------|-----------|-----------
|rw_leaning| 0.057  | 0.22  | -1 | 1
|nw_leaning| -0.002 | 0.19 | -1 | 1
|rw_nn_leaning| 0.017 | 0.33 | -1 | 1
|nw_nn_leaning| -0.017 | 0.47 | -1 | 1

### Table 2. Correlation
|  | rw_leaning | nw_leaning | rw_nn_leaning | nw_nn_leaning
|-----:|---------------|-----------|-----------|-----------
|**rw_leaning**| 1.0000 | | |
|**nw_leaning**| 0.7440 | 1.0000 | |
|**rw_nn_leaning**| 0.8992 | 0.8206 | 1.0000 |
|**nw_nn_leaning**| 0.6780 | 0.8935 | 0.8366 | 1.0000

### Figure 1. Histogram of rw_leaning
![rw_leaning](https://github.com/user-attachments/assets/20b3c923-526e-404a-82f4-78b5b65aaaff)

### Figure 2. Histogram of nw_leaning
![nw_leaning](https://github.com/user-attachments/assets/c8fa6d89-6128-4d8f-87da-de02d904d4aa)

### Figure 3. Histogram of rw_nn_leaning
![rw_nn_leaning](https://github.com/user-attachments/assets/e10a3061-cf15-42cf-9b85-b1354fd95e9a)

### Figure 4. Histogram of nw_nn_leaning
![nw_nn_leaning](https://github.com/user-attachments/assets/6760ca15-cfc9-4e55-8a2b-299b057b8ee9)

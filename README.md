Gabriel Geyer, Evan Taylor, Andrew Kallmeyer

**1.**
|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

**2.** 

| Graph File               | Iterations | Runtime (s) | Output Verified as MIS? |
|--------------------------|------------|-------------|--------------------------|
| small_edges.csv          |   2        | 0.67        | Yes                      |
| line_100_edges.csv       |   3        | 0.79        | Yes                      |
| twitter_100_edges.csv    |   2        | 0.66        | Yes                      |
| twitter_1000_edges.csv   |   2        | 0.77        | Yes                      |
| twitter_10000_edges.csv  |   3        | 1.62        | Yes                      |


3. **(3 points)**  
a. Run `LubyMIS` on `twitter_original_edges.csv` in GCP with 3x4 cores (vCPUs). Report the number of iterations, running time, and remaining active vertices (i.e. vertices whose status has yet to be determined) at the end of **each iteration**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`.  
b. Run `LubyMIS` on `twitter_original_edges.csv` with 4x2 cores (vCPUs) and then 2x2 cores (vCPUs). Compare the running times between the 3 jobs with varying core specifications that you submitted in **3a** and **3b**.
**3a**
| Iteration | Remaining Active Vertices |
|-----------|----------------------------|
| 1         |       11316811             |
| 2         |       6653976              |
| 3         |           34662            |
| 4         |               388          |

**3b**
| Cluster Configuration  | Total Cores | Total Runtime (s)  | Iterations | 
|------------------------|-------------|--------------------|------------|
| 3×4 vCPUs              | 12          |          309       |  4         |   
| 4×2 vCPUs              | 8           |      1,355.74      |  4         |                
| 2×2 vCPUs              | 4           |      5,661.77      |  4         |                  




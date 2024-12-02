# llm-politics
 
```mermaid
   graph TD
      A(Run api_google_serp.py) --> B(Run run_process_serp.py)
      B --> C(Run run_gpt.py) --> E(Run run_merge_data.py)
      B --> D(Run run_standardize.py) --> E(Run run_merge_data.py)
      F(Run api_naver_news.py) --> E(Run run_merge_data.py)
      E -- output: political_leaning.csv --> G(Run api_naver_datalab.py) -- output: domain_popularity.csv --> H(Run run_prepare_analysis.py) -- output: sample_popularity.csv
```
```

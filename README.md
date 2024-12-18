<div align='center'>
</div>

## Usage

#### 1. Install requirements 
```python 
pip install -r requirements.txt
```

#### 2. Update .env
1) OPENAI_API_KEY_DICT: A dictionary mapping each index to <YOUR_OPENAI_API_KEY>.
2) SERP_API_KEY: Your API key for [serper.dev.](https://serper.dev/.)
3) NAVER_DICT: A dictionary mapping each index to your API key for [Naver Developers.](https://developers.naver.com/main/.)


#### 3. Retrieve data from both the Google SERP API and the Naver News API
```python 
python run_api.py
```

#### 4. Standardize each search query using one of two approaches
```python 
python run_process_data.py
```

#### 5. Define the final variables for analysis
> See the 'results' folder in this repository for further details

```python 
python run_merge_data.py
```

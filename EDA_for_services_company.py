from collections import OrderedDict
from io import StringIO
import os
import json
import re
import pandas as pd
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import plotly.graph_objects as go
import warnings
warnings.filterwarnings("ignore")


def writeDataIntoRetDF(provided_filename, result_df):
    analyzer = SentimentIntensityAnalyzer()

    def calc_number_texts(file_path):
        with open(file_path, 'r') as file:
            count = 0
            for line in file:
                count += 1
            return count


    def get_average_sentiment(date): #Try to get the average sentiment values for all of the texts sent by user on that date 
        date_str = date.strftime('%Y-%m-%d') #get taht string for date
        file_path = os.path.join('stock-price-predictions/tweet/', provided_filename, date_str) #get the path of the tweet file for that specific date -> then try to get the texts
        # print("1")
        if not os.path.isfile(file_path): #doesn't exist
            return None
        # print("2")
        sentiments = []
        try:
            with open(file_path, 'r') as file:
                # print("3")

                for line in file:
                    try:
                        # print("hereee")
                        tweet = json.loads(line)
                        text = ' '.join(tweet['text'])
                        sentiment = analyzer.polarity_scores(text) #Get the sentiment value for each text
                        sentiments.append(sentiment['compound'])
                        # print("----------")
                        # print(sentiment['compound'])
                    except json.JSONDecodeError:
                        print(f"Skipping line due to JSON decode error: {line}")
                    except KeyError:
                        print(f"Skipping line due to missing keys: {line}")
            if sentiments:
                average_sentiment = sum(sentiments) / len(sentiments)
                if (average_sentiment > 10):
                    print(sentiments)

                    print(len(sentiments))
                    print(average_sentiment)
                    print("----------")
                return average_sentiment
            else:
                return None
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return None


    dict_for_date_vs_line = dict()
    #Try to get the number of texts for each date
    
    for root, dirs, files in os.walk(os.path.join('stock-price-predictions/tweet/', provided_filename)):#For each file which is each day in the given filename folder, extract events&sentiment
        for file in files:
            file_path = os.path.join(root, file)
            num = calc_number_texts(file_path)
            dict_for_date_vs_line[file] = num

    # print(dict_for_date_vs_line)
    data_datetime_keys = {}
    for date_str, value in dict_for_date_vs_line.items():
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            data_datetime_keys[date_obj] = value
        except ValueError:
            print(f"Invalid date format for key: '{date_str}'. Skipping.")

    min_date = min(data_datetime_keys.keys())
    max_date = max(data_datetime_keys.keys())

    current_date = min_date
    ordered_data = OrderedDict()
    while current_date <= max_date:
        date_str = current_date.strftime('%Y-%m-%d')
        value = data_datetime_keys.get(current_date, 0)
        ordered_data[date_str] = value
        current_date += timedelta(days=1)


    selected_dates = []
    dates_list = list(ordered_data.items())

    #Try to get the dates that its number of texts >= 2 * average of number of texts in the past 3 days
    for i in range(3, len(dates_list)):
        current_date_str, current_value = dates_list[i]
        prev_values = [dates_list[i - 1][1], dates_list[i - 2][1], dates_list[i - 3][1]]
        sum_prev_values = sum(prev_values)
        if sum_prev_values == 0:
            continue 
        avg_prev_values = sum_prev_values / 3
        if current_value >= 2 * avg_prev_values:
            selected_dates.append((current_date_str, current_value))







    selected_dates_arr = list(selected_dates)
    
    stock_df = pd.read_csv(os.path.join('stock-price-predictions/price/', provided_filename + '.csv'), parse_dates=['Date'])
    stock_df.sort_values(by='Date', inplace=True)
    stock_df.reset_index(drop=True, inplace=True)
    stock_df.set_index('Date', inplace=True)


    selected_dates = [date_str for date_str, _ in selected_dates]
    print(len(selected_dates))
    # print("------")

    for date_str in selected_dates: #For each event, get the column data for its row
        selected_date = pd.to_datetime(date_str)
        row_data = {}
        for i in range(5): #Past 5 days' data extraction
            day = i + 1
            current_day = selected_date - timedelta(days=i)

            try:
                stock_data = stock_df.loc[current_day]
                if isinstance(stock_data, pd.DataFrame):
                    stock_data = stock_data.iloc[0]
                    
                row_data[f'open_day_{day}'] = stock_data['Open']
                row_data[f'high_day_{day}'] = stock_data['High']
                row_data[f'low_day_{day}'] = stock_data['Low']
                row_data[f'close_day_{day}'] = stock_data['Close']
                row_data[f'adj_close_day_{day}'] = stock_data['Adj Close']
                row_data[f'volume_day_{day}'] = stock_data['Volume']
                sentiment_value = get_average_sentiment(current_day)
                row_data[f'sentiment_day_{day}'] = sentiment_value
            except KeyError:
                row_data[f'open_day_{day}'] = None
                row_data[f'high_day_{day}'] = None
                row_data[f'low_day_{day}'] = None
                row_data[f'close_day_{day}'] = None
                row_data[f'adj_close_day_{day}'] = None
                row_data[f'volume_day_{day}'] = None
                row_data[f'sentiment_day_{day}'] = None
        
        for j in range(1, 8):
            future_day = selected_date + timedelta(days=j)
            try:
                future_data = stock_df.loc[future_day]
                if isinstance(future_data, pd.DataFrame):
                    future_data = future_data.iloc[0]

                row_data[f'future_open_day_{j}'] = future_data['Open']
            except KeyError:
                row_data[f'future_open_day_{j}'] = None
                
        result_df = result_df.append(row_data, ignore_index=True)
        # print(result_df)
    return result_df
#Set up the result_df
prev_days = [1, 2, 3, 4, 5] 
future_days = [1, 2, 3, 4, 5, 6, 7]
columns = []
for day in prev_days:
    columns.extend([
        f'open_day_{day}',
        f'high_day_{day}',
        f'low_day_{day}',
        f'close_day_{day}',
        f'adj_close_day_{day}',
        f'volume_day_{day}',
        f'sentiment_day_{day}'  
    ])

for day in future_days:
    columns.append(f'future_open_day_{day}')

result_df = pd.DataFrame(columns=columns)

# print(result_df)
result_df = writeDataIntoRetDF("DIS", result_df)

result_df = writeDataIntoRetDF("AMZN", result_df)
result_df = writeDataIntoRetDF("BABA", result_df)
result_df = writeDataIntoRetDF("WMT", result_df)
result_df = writeDataIntoRetDF("CMCSA", result_df)
result_df = writeDataIntoRetDF("HD", result_df)
result_df = writeDataIntoRetDF("MCD", result_df)
result_df = writeDataIntoRetDF("UPS", result_df)
result_df = writeDataIntoRetDF("CHTR", result_df)
result_df = writeDataIntoRetDF("PCLN", result_df)





result_df.to_csv('input_df.csv', index=False)



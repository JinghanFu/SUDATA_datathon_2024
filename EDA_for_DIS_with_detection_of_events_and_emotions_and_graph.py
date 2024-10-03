import os
import json
import re
import pandas as pd
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import plotly.graph_objects as go

event_keywords = {
    'new_product': ['new product', 'launch', 'release', 'inventory', 'available'],
    'accident': ['accident', 'crash', 'issue', 'problem', 'delay'],
    'earnings_report': ['earnings', 'profit', 'report', 'revenue', 'loss'],
    'partnership': ['partnership', 'collaborate', 'deal', 'agreement'],
}

analyzer = SentimentIntensityAnalyzer()
event_data = []

def extract_events(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            try:
                tweet = json.loads(line)
                text = " ".join(tweet['text'])
                created_at = tweet['created_at']
                detected_events = {}
                for category, keywords in event_keywords.items(): #For each category in the GPT defined events
                    for keyword in keywords:
                        if re.search(rf"\b{keyword}\b", text, re.IGNORECASE): #If one of the keywords is found
                            if category in detected_events:
                                detected_events[category].append(keyword)
                            else:
                                detected_events[category] = [keyword]
                
                #下面的polarity score是chatGPT提到的可以用来判断情绪的方法
                sentiment = analyzer.polarity_scores(text)

                for category in detected_events:
                    if sentiment['compound'] >= 0.05:
                        emotion = 'positive'
                    elif sentiment['compound'] <= -0.05:
                        emotion = 'negative'
                    else:
                        emotion = 'medium'
                    detected_events[category].append(emotion)

                if detected_events:
                    tweet_time = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
                    event_data.append({
                        'date': tweet_time.date(),
                        'time': tweet_time.time(),
                        'text': text,
                        'events': detected_events
                    })

            except json.JSONDecodeError:
                print(f"Skipping line due to JSON decode error: {line}")


for root, dirs, files in os.walk('stock-price-predictions/tweet/DIS'):#For each file which is each day in the DIS folder, extract events&sentiment
    for file in files:
        file_path = os.path.join(root, file)
        extract_events(file_path)

events_df = pd.DataFrame(event_data)
events_df.to_csv('extracted_events_with_emotions.csv', index=False)

#Everything above is the extraction of events and emotions

#Below is the drawing of stock vs time with the events label

#Some formatting and reading
stock_data = pd.read_csv('stock-price-predictions/price/DIS.csv')
stock_data['Date'] = pd.to_datetime(stock_data['Date'])
event_data = pd.read_csv('extracted_events_with_emotions.csv')
event_data['Datetime'] = pd.to_datetime(event_data['date'] + ' ' + event_data['time'])
event_data = event_data.sort_values(by='Datetime', ascending=True)

fig = go.Figure() #:D Interactive Plotly Graph 
fig.add_trace(go.Scatter(
    x=stock_data['Date'],
    y=stock_data['Adj Close'],
    mode='lines',
    name='Adj Close Price',
    line=dict(color='blue')
))


for _, row in event_data.iterrows():
    fig.add_trace(go.Scatter(
        x=[row['Datetime'], row['Datetime']],
        y=[min(stock_data['Adj Close']), max(stock_data['Adj Close'])],
        mode='lines',
        line=dict(color='red', dash='dash'),
        hoverinfo='text',
        text=f"{row['events']}",
        showlegend=False
    ))

fig.update_layout(
    title='Stock Price with Events Overlay',
    xaxis_title='Date',
    yaxis_title='Adjusted Close Price',
    xaxis=dict(
        tickformat='%Y-%m-%d',
        rangeslider_visible=True
    ),
    hovermode='x unified'
)

# fig.show()
fig.write_html("stock_vs_time_with_events_and_emotion.html")
# analyze sentiment classification accuracy

from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import pandas as pd

df = pd.read_excel("sample_reddit_posts.xlsx")
df = df[["text", "correct sentiment"]]
df = df[:-1]

# instanstiate vader sentiment analyzer
vader_analyzer = SentimentIntensityAnalyzer()

polarities = []
subjectivities = []

for text in df["text"].values:
    polarity = vader_analyzer.polarity_scores(text.lower())['compound']
    blob = TextBlob(text.lower())
    subjectivity = blob.sentiment.subjectivity
    polarities.append(polarity)
    subjectivities.append(subjectivity)
    
df["polarity"] = polarities
df["subjectivity"] = subjectivities

def label(polarity):
    if polarity > 0:
        return "Positive"
    elif polarity < 0:
        return "Negative"
    else:
        return "Neutral"

df["pred_label"] = df["polarity"].apply(label)

df2 = df[(df["polarity"] != 0) & (df["subjectivity"] < 0.5) & (df["correct sentiment"] != "Neutral")].reset_index()
del df2["index"]

print(sum(df2["pred_label"] == df2["correct sentiment"]) / len(df2))

y = [1 if x=="Positive" else 0 for x in df2["correct sentiment"].values]
pred = [1 if x=="Positive" else 0 for x in df2["pred_label"].values]

import matplotlib.pyplot as plt
from sklearn import metrics

fpr, tpr, thresholds = metrics.roc_curve(y, pred)
roc_auc = metrics.auc(fpr, tpr)
display = metrics.RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=roc_auc,
                                  estimator_name='Vader Sentiment Clf')
display.plot()

plt.plot([0, 1], [0, 1], linestyle="dashed")

plt.xlim(0, 1)
plt.ylim(0, 1)

acc = round(sum(df2["pred_label"] == df2["correct sentiment"]) / len(df2), 2)
plt.title(f"{acc} accuracy")

plt.savefig('accuracy.jpeg', format='jpeg', quality=100)

plt.show()

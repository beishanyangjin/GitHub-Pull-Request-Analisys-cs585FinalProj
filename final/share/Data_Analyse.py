import matplotlib.pyplot as plt
import pandas as pd
import math
import numpy as np

def languages():
    df = pd.read_csv("C:/Users/marsa/Desktop/Masters/BigData/Most_common_languages.csv/10languages.csv", header=None)

    p = figure(x_range=df[0], height=350, title="Most popular Languages",
               toolbar_location=None, tools="")

    p.vbar(x=df[0], top=df[2], width=0.9)

    p.xgrid.grid_line_color = None
    p.yaxis.axis_label = "Percent"
    p.y_range.start = 0
    show(p)

def contributorCommentLength():
    df = pd.read_csv("hdfs://master:9000/output/Most_active_actors.csv", header=None)
    df.columns = ['actor_id', 'actor_login', 'count', 'length_of_comment']

    print(df)
    plt.hist(df['length_of_comment'], bins=200)
    plt.title('Average Contributor Length of comment')
    plt.xlabel('Average Length of comment for each Contributer')
    plt.ylabel('Count')
    plt.show()

def Time_distir_analyse():
    df = pd.read_csv("hdfs://master:9000/output/commit_time_analysis.csv", header=None)
    plt.bar(df[0],df[1])
    plt.xticks(range(0,24,1))
    plt.title('Time_Distribution_Pull_request')
    plt.xlabel('Time')
    plt.ylabel('Pull_requests')

def Repo_analyse():
    df = pd.read_csv("hdfs://master:9000/output/Repo_PR.csv", header=None)
    df.columns = ['Repo_name','Author_name','Author_id','Individual_contributor','Pull_request','Average_len_comment','Language','median_time']
    print(df.head(10))

    plt.hist(df['Individual_contributor'], bins= 200)
    plt.yscale('log')
    plt.title('Num_Individual_contributor_Distribution')
    plt.xlabel('Individual_contributor')
    plt.ylabel('Num')

def Repo_analyse2():
    df = pd.read_csv("hdfs://master:9000/output/Repo_PR.csv", header=None)
    df.columns = ['Repo_name','Author_name','Author_id','Individual_contributor','Pull_request','Average_len_comment','Language','median_time']
    print(df.head(10))

    plt.hist(df['Pull_request'], bins= 200)
    plt.yscale('log')
    plt.title('PullRequest_of_Repo_Distribution')
    plt.xlabel('Pull_request')
    plt.ylabel('Num')

def contributor():
    df = pd.read_csv("hdfs://master:9000/output/Most_active_actors.csv", header=None)
    df.columns = ['actor_id', 'actor_login', 'countPR', 'length_of_comment']
    lower = df.countPR.quantile(.05)
    upper = df.countPR.quantile(.95)

    df = df.clip(lower=lower, upper=upper)
    print(df)
    plt.hist(df['count'], bins=50)
    plt.yscale('log')
    plt.title('Number of PR per Contributer')
    plt.xlabel('Number of PR per contributor')
    plt.ylabel('Count')
    plt.show()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import matplotlib.pyplot as plt
    import pandas as pd
    from bokeh.plotting import figure, show

    languages()
    contributorCommentLength()
    Time_distir_analyse()
    Repo_analyse()
    Repo_analyse2()
    contributor()

# Wiki-Guardian  *"Catch every Anomaly for you"*

Insight 2018 Data Engineering Project



## Project Summary:

This data pipeline is designed and implemented for real-time detection of user anomaly in streaming Wikipedia edit log.



## Project Description:

Changing and editing on existing topics are extrem common and quite often, especially from core group of user who like to engage more. This project aims to provide an analytic tool for wikipedia to ingest the core user behavier and suggest peer-review candidates for submitted revision.

Use Case:

1. Possible Fraud Usage Detection: Abnormal behavior of Wiki articles modification should be flagged on:

	1. No. of modification per minute from SAME user;
	
		flag the user: edit multiple articles in very short time window;

	2. No. of modification per minute SAME article received;  
	
		flag the article: been edited too frequently in very short time window;

2. Peer Review Candidates Suggestion: 

	For flagged articles, system need to provide peer-review candidates who: either has similar interest as this user; or has edit this article/catogary most time.

3. Web activity monitoring:

	check the most modified articles;

	check the most active user;
	
	

## Data:
1. The uncompressed dataset is about several TB which can be downloaded from [wikipedia site](https://en.wikipedia.org/wiki/Wikipedia:Database_download);

2. The data set contains fields of "user_id", "Edit time", "article", "category";

   


## Architecture
![arch](images/arch_flink.png)

## Live Demo
![top window](https://github.com/kaenyyh/Insight_project_2018b/blob/master/images/topwindow12.gif)

<img src="https://github.com/kaenyyh/Insight_project_2018b/blob/master/images/topwindow12.gif" alt="alt text" width="600" height="300">![bottom window](https://github.com/kaenyyh/Insight_project_2018b/blob/master/images/bottomwindow10.gif)
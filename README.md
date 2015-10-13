# EmailAnalytics
The goal of this project is to build a EmailAnalytics system using Apache Lucene

#EmailAnalytics tool does the following Analytics on the email:

1. Builds/Updates a lucene index using the Emails as an input document.
2. Extracts useful phrases from user mails and returns top "n" 3-worded phrases in the entire corpus which might be relevant.
3. If we provide a date range and k(number of phrases) as a input, the tool returns month-wise/yearly top k phrases over the entire email corpus.
4. Extract the top n phrases within a specified date range rather than the entire index
5. Enforcing uniqueness on mails. This makes sure we do not index same mail multiple times during Lucene index update.
6. Extract the monthly/yearly data(count) for each top n phrases . This is very cool as we can see the trends on these phrases


#Entire design process

The entire process is divided into two parallel processes:

  1. Creation/Update on index.We provide the input CSV file along the folder name where the Lucene Index should be created/updated as an input, so that Lucene Index is created/updated for each Email within the CSV file.
  2. We preprocess each Email and extract the body field and use the email-id+server timestamp as a unique id for the email.
  2. Search on the index on a specified date range. This is a process where we can search for top phrases on the Index for a specified date range and then we would see the top n phrases along with the occurrence of each top n phrases month wise or year wise outputted to a CSV file.

Note: The input mail should be exported in a CSV format in the following manner:
Using Outlook, u can export all of your emails in the below format:

Body,	From: (Name),	From: (Address),From: (Type),	To: (Name),	To: (Address),To: (Type),	CC: (Name),	CC: (Address),	CC: (Type),	BCC: (Name),BCC: (Address),	BCC: (Type)

Sample email body looks as follows:

        Name: Vardhaman
        Email: vardhaman.metpally@gmail.com 
        Referring URL: www.github.com 
        Message: Hey Vardhaman, Your project is pretty cool. I like it.
        Server Timestamp: 2015-10-04 15:47:52 

#Sentiment Analysis of Emails.

To perform sentiment analysis on the emails, we just pass this corpus as an input to sentiment.py in TwitterAnalytics code and bang sentiment analysis is also done.

#Classify emails into folders

Steps to  classify emails into folders like Sports,Entertainment, Economics,etc. 

1. I run Latent Dirichlet Allocation Model(LDA) implementation from my TwitterAnalytics project and discover the topics and the top 50 keywords from these topics

2. Once i get the topics and frequent keyowrds, I label the topics and its keyowrds into Sports,Entertainment,Economics,etc manually using my topic knowledge.

3. I then use these topics as classes and keywords as training features for Multinomial NaiveBased Classfier or Logisitic Regression or SVM Classifier 

4. Train the model and then when a test email comes in, I predict the email into the corresponding folder(class) based on the prediction from my model

#Email Spam Classification 

refer to Logistic Regression project or Decision Trees Project for the Spam Classification problem




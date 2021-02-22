# So I'm probably on the wrong side of a bet #
The bet is an under/over on the first day on which there is no _New York Times_ article that mentions Donald J Trump.
We read the newspaper enough, but not nearly enough to track it to the day.  So we need a little automation

My bet was April 15.  My counterparty took the over on that.  I'm probably going to lose this bet.  The specific criterion is that the day must have at least one NYT article with DJT flagged with a "persons" keyword.

# What this is # 

This is a project that does a daily article search using the _New York Times_ developer API.  I'm not sure exactly what I'm going to do with this data, but it'll probably be fun.

So far, what I have here is
* A daily event, which kicks off a Lambda function that queries for the prior day
* An S3 notification, which kicks off another Lambda function to clean the data into a format that I am expecting the Athena/Presto JSON SerDe to be able to handle; I found that the data wasn't clean enough as is due to dup key names and potentially other problems.

I have yet to start building the data-analysis part of this, but I bet there are fun things that could be done.  If you're in some weird corner of the internet and somehow are seeing this, I'd love your ideas.  



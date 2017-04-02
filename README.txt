####################################################### README ################################

################################################ Create By #######################################

	* Shahar Davidovich
	* Raz Ben-yaish
	
############################################# Running instruction ##########################################

	* make sure the right Credentials file names "prop.properties" is placed in the LocalMain project folider (or same folider as the running jar).
	* make sure all the steps jar is placed in the right place at S3.
	* args <minPmi> <relMinPmi> <language> <include stopwords> 
		* heb = hebrew eng = english 
		* 0 - filter stop words 1 - include stopwords.
	* run and wait for the output. 

######################################### Step describe ###############################################

###################################### Step One ####################################

	
	* Counting the number of times each bigram appears per decade (c(w1, w2)).
	* Filter StopWords if nedded.
	* Input: google 2-gram file
	* Output: a new file containg all bigram with  decade and cw1w2 
		* <bigram> <Decade> <cw1w2>
		
################################## Step Two #####################################

	* counting the numbwe of times each word appears as "w1" per decade (c(w1)).
	* Input:Step One output file
	* Output: a new file containg all bigram with decade cw1w2 and cw1
		* <bigram> <decade> <cw1w2> <cw1>
		
############################### Step Three #####################################

	* counting the numbwe of times each word appears as "w2" per decade (c(w2)).
	* counting the numbwe of times each bigram appears per decade (N).
	* calculating Npmi for each bigram accourding to the formula.
	* Input: Step Two output file
	* Output: a new file containg all bigrams with Decade & Npmi.
		* <bigram> <Decade> <Npmi>

############################## Step Four #########################################

	* sorting all the bigram for each decade ordered by npmi (descending).
	* calculating releative Npmi.
	* filter bigrams accourding to the MinNpmi and relMinPmi threshold.
	* Input: Step Three output file.
	* Output: file containg all bigrams with decade and Npmi wich pass the filter.
		* <bigram> <Decade> <Npmi>

##################################################################################################################

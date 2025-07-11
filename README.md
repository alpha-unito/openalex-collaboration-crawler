# OpenAlex Collaboration Crawler

This repo contains the code to extract and generate a collaboration temporal network. It is composed of several steps, 
divided into different tools. The structure of the workflow is the following:

<img src="extra/OpenAlexGraphGen-wf.png">


# Step 1: Author
This steps aims at extracting authors and generate a JSONL list of authors affiliaiton. This macro step does not 
stores all the information of a given author, but rather creates an output file of an author id, and an array with 
the known country to which he/she was affilaited in a given year. This macro step is composed of three different sub 
steps, all compiled into a single executable (meaning that they can be deployed individually or alltoghether)

### Step 1.1: Author extractor
This step extracts the author informations from the OpenAlex snapshot.

### Step 1.2: Author compressor
Since OpenAlex uses incremental updates in its snapshots, this step compresses author informations to single entries
into the JSONL source file, merging the affiliations.

### Step 1.3: Author filter
This step filter the obtained author by a given country. This means that the output file contains authors that at a 
given point have been affiliated to the provided country.

# Step 2: Paper
This step aims at extracting papers accordingly to the authors obtained from step 1. Contrary to step 1, the results 
of this step is a JSONL containing all the information of a given paper. This step is comprised of 2 steps:

### Step 2.1: Paper extractor
This steps extracts the papers from the OpenAlex snapshot folder. It produces a file containing papers that have been
authored by any of the authors present in the produced file by step 1. Contrary to step 1, the JSONL entry for a given 
paper contains all the information of the paper provided by OpenAlex

### Step 2.2: Paper filter
This step filters the extracted papers accordingly to a given research topic and the author file. 
During this steps, a paper to be kept in the output file, needs to match two checks:
- the OpenAlex ``concepts`` field must have one entry that matches the given filter
- At leas one author of the paper, must be affiliated to the country specified in the filter parameter in the 
    year the paper has been published. 


# Step 3: Graph
This is the final step, that aims to create an adjacency list of collaborations. The created adjacency list(s), is a CSV
file in which each line has four fields:
- year of the pubblication
- OpenAlex ID of the pulication
- OpenAlex author ID 1
- OpenAlex author ID 2

It is possible to split the adjacency list into different time frames. the format to give the filter is defined in the 
following: a list of year (extremes included) separated by a comma. each element is given by <starting_year>-<end_year>
If either starting_year or <ending_year> are missing, it is intended that everything before or after the given year
should go into the same timeframe. 

@feature-loadData
@loadData-scrapers
Feature: Load Data - Scrapers
  As a user I want to Read Create Update and Delete Scrapers
  So that I can manage the stores used with Influxdbv2
# Move exercise create scraper popup here
# N.B. can verify scrapers at endpoint http://localhost:8086/api/v2/scrapers


@tested
Scenario: Load Initial Scrapers tab
  Given I reset the environment
  Given run setup over REST "DEFAULT"
  When API sign in user "DEFAULT"
  When API create a bucket named "Duchamp" for user "DEFAULT"
  When open the signin page
  When UI sign in user "DEFAULT"
  When click nav menu item "LoadData"
  When click load data tab "Scrapers"
  Then the scrapers tab is loaded

@tested
Scenario: Exercise create Scraper popup
  When click the create scraper button empty
  Then the Create Scraper popup is loaded
  When dismiss the Create Scraper popup
  Then the Create Scraper popup is no longer present
  When click the create scraper button from the header
  Then the Create Scraper popup is loaded
  When cancel the Create Scraper popup
  Then the Create Scraper popup is no longer present
  When click the create scraper button empty
  Then the Create Scraper popup is loaded
  When clear the Scraper Popup name input
  Then the form element error message is "Name cannot be empty"
  Then a form input error icon is shown
  Then the Create Scrapper popup create button is disabled
  When enter the name "Mumford" into the Create Scraper popup name input
  Then the form element error message is not shown
  Then no form input error icon is shown
  Then the Create Scrapper popup create button is enabled
  When click the Create Scrapper buckets dropdown
  Then an item for the bucket "DEFAULT" is an item in the buckets dropdown
  Then an item for the bucket "Duchamp" is an item in the buckets dropdown
  When click the Create Scrapper buckets dropdown
  Then NO items in the buckets dropdown are shown
  When clear Scraper Popup the Target Url input
  Then the form element error message is "Target URL cannot be empty"
  Then a form input error icon is shown
  Then the Create Scrapper popup create button is disabled
  When enter the value "http://localhost:8086/metrics" into the Create Scraper popup url input
  Then the form element error message is not shown
  Then no form input error icon is shown
  Then the Create Scrapper popup create button is enabled
  When dismiss the Create Scraper popup
  Then the Create Scraper popup is no longer present

@tested
Scenario Outline: Create Scrapers
  When click the create scraper button from the header
  When clear the Scraper Popup name input
  When enter the name "<NAME>" into the Create Scraper popup name input
  When click the Create Scrapper buckets dropdown
  When select the Scrapper buckets dropdown item "<BUCKET>"
  When clear Scraper Popup the Target Url input
  When enter the value "<ENDPOINT>" into the Create Scraper popup url input
  When click the create scraper create button
  Then the success notification contains "Scraper was created successfully"
  When close all notifications
  Then the create scraper button empty is no longer present
  Then there is a scraper card for "<NAME>"
  Then the scraper card named "<NAME>" has the bucket "<BUCKET>"
  Then the scraper card named "<NAME>" has the endpoint "<ENDPOINT>"

  Examples:
  | NAME | ENDPOINT | BUCKET |
  | Melnik | http://localhost:8086/metrics | DEFAULT |
  | Morlaix | http://localhost:8086/metrics | Duchamp |
  | Brno | http://localhost:10018/bogus | DEFAULT |
  | Brest | http://localhost:10018/bogus | Duchamp |

@error-collateral
Scenario: Filter Scrapers
  Then the scraper name sort order is "Brest,Brno,Melnik,Morlaix"
  When enter the value "Br" into the scraper filter
  Then the scraper name sort order is "Brest,Brno"
  Then the scraper card "Melnik" is no longer present in the list
  Then the scraper card "Morlaix" is no longer present in the list
  When clear the scraper filter
  Then the scraper name sort order is "Brest,Brno,Melnik,Morlaix"

@error-collateral
Scenario: Sort Scrapers by Name
  When click the sort type dropdown
  When click sort by item "Name Desc"
  #When click the scraper sort by name button
  Then the scraper name sort order is "Morlaix,Melnik,Brno,Brest"
  When click the sort type dropdown
  When click sort by item "Name Asc"
  #When click the scraper sort by name button
  Then the scraper name sort order is "Brest,Brno,Melnik,Morlaix"

@error-collateral
Scenario: Sort Scrapers by URL
  When click the sort type dropdown
  When click sort by item "URL Asc"
  #When click the scraper sort By URL button
  Then the scraper name sort order is "Brno,Brest,Melnik,Morlaix"
  When click the sort type dropdown
  When click sort by item "URL Desc"
  #When click the scraper sort By URL button
  Then the scraper name sort order is "Melnik,Morlaix,Brno,Brest"

@error-collateral
Scenario: Sort Scrapers by Bucket
  When click the sort type dropdown
  When click sort by item "Bucket Asc"
  #When click the scraper sort By Bucket button
  Then the scraper name sort order is "Morlaix,Brest,Melnik,Brno"
  When click the sort type dropdown
  When click sort by item "Bucket Desc"
  #When click the scraper sort By Bucket button
  Then the scraper name sort order is "Melnik,Brno,Morlaix,Brest"

@error-collateral
Scenario: Rename Scraper
  When hover over the scraper card name "Brno"
  When click the scraper card name edit control for the card "Brno"
  When Enter the value "Plzeň" for the card "Brno"
  Then the success notification contains "Scraper "Plzeň" was updated successfully"
  Then there is a scraper card for "Plzeň"
  Then the scraper card "Brno" is no longer present in the list

@error-collateral
Scenario Outline: Verify Scraper data
  Then the named query "<NAMED_QUERY>" by user "<USER>" on the bucket "<BUCKET>" contains the values "<EXPECTED_VALUES>"

  Examples:
  |USER|BUCKET|NAMED_QUERY|EXPECTED_VALUES|
  |DEFAULT| Duchamp | Measurements | boltdb_reads_total,go_info,go_threads,influxdb_info,storage_reads_seeks |
  |DEFAULT| DEFAULT | Measurements | boltdb_reads_total,go_info,go_threads,influxdb_info,storage_reads_seeks |

@tested
Scenario Outline: Delete Scraper
  Then the delete button of the scraper card named "<NAME>" is not present
  When hover over scraper card named "<NAME>"
  When click the delete button of the scraper card named "<NAME>"
  When click the confirm delete button of the scraper card named "<NAME>"
  Then the scraper card "<NAME>" is no longer present in the list

  Examples:
    | NAME |
    | Melnik |
    | Morlaix |
    | Plzeň |
    | Brest |

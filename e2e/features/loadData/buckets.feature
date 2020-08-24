@feature-loadData
@loadData-buckets
Feature: Load Data - Buckets
  As a user I want to Read Create Update and Delete Buckets
  So that I can manage the stores used with Influxdbv2


@tested
Scenario: List Initial Buckets
  Given I reset the environment
  Given run setup over REST "DEFAULT"
  When open the signin page
  When UI sign in user "DEFAULT"
  When click nav menu item "LoadData"
  When click load data tab "Buckets"
  Then the buckets tab is loaded
  Then the buckets are sorted as "_monitoring,_tasks,DEFAULT"

@tested
Scenario: Exercise Create Bucket Dialog
  When click the Create Bucket button
  Then the Create Bucket Popup is loaded
  Then the Create button of Create Bucket Popup is disabled
  Then the Retention Policy radio button "never" is active
  Then the Retention Policy intervals controls are not present
  When click the Retention Policy "intervals" button
  Then the Retention Policy radio button "intervals" is active
  Then the Retention Policy radio button "never" is inactive
  Then the Retention Policy intervals controls are present
  # N.B. controls replaced with dropdown selector 2019-09-11
  #Then the Retention Policy warning message contains "Retention period must be at least an hour"
  #When enter "61" into the Retention Policy "Seconds" control
  #Then the Retention Policy "Seconds" control contains the value "1"
  #Then the Retention Policy "Minutes" control contains the value "1"
  #Then the Retention Policy warning message contains "Retention period must be at least an hour"
  #When clear all Retention Policy interval controls
  #When enter "123" into the Retention Policy "Minutes" control
  #Then the Retention Policy "Minutes" control contains the value "3"
  #Then the Retention Policy "Hours" control contains the value "2"
  #Then the Retention Policy warning message has disappeared
  #When clear all Retention Policy interval controls
  #When enter "80" into the Retention Policy "Hours" control
  #Then the Retention Policy "Hours" control contains the value "8"
  #Then the Retention Policy "Days" control contains the value "3"
  #When clear all Retention Policy interval controls
  #When enter "7" into the Retention Policy "Days" control
  #Then the Retention Policy "Days" control contains the value "7"
  #When clear all Retention Policy interval controls
  #When enter "ABCD" into the Retention Policy "Seconds" control
  #Then the Retention Policy "Seconds" control contains the value "0"
  #When enter "ABCD" into the Retention Policy "Minutes" control
  #Then the Retention Policy "Minutes" control contains the value "0"
  #When enter "ABCD" into the Retention Policy "Hours" control
  #Then the Retention Policy "Hours" control contains the value "0"
  #When enter "ABCD" into the Retention Policy "Days" control
  #Then the Retention Policy "Days" control contains the value "0"
  When input the name of the bucket as "ABCD"
  #Then the Retention Policy warning message contains "Retention period must be at least an hour"
  When click the Retention Policy "never" button
  #Then the Retention Policy warning message has disappeared
  Then the Create button of Create Bucket Popup is enabled
  When dismiss the Create Bucket Popup
  Then the Create Bucket Popup is not present
  When click the Create Bucket button
  Then the Create Bucket Popup is loaded
  Then the Create button of Create Bucket Popup is disabled
  When cancel the Create Bucket Popup
  Then the Create Bucket Popup is not present

@tested
Scenario Outline: Create Buckets with Retention Policies
  When click the Create Bucket button
  When input the name of the bucket as "<NAME>"
  When set the retention policy of the bucket as "<RETENTION>"
  When click the Create Bucket popup Create button
  Then the bucket named "<NAME>" is in the list
  Then the bucket named "<NAME>" has a Retention Policy of "<RETENTION>"

Examples:
  |NAME| RETENTION |
  | Trvalá | Never |
  | Měsíční | 30d  |
  | Týdenní |  7d  |
  | Denní | 24h  |
  | Půldenní | 12h |
  | Hodinová  | 1h |
  | Oprava    | 24h  |

@tested
Scenario: Modify Retention Policy
  When click on settings for bucket named "Oprava"
  Then the Edit Bucket popup is loaded
  Then the name edit textbox of the Edit Bucket popup is disabled
  Then the form help text contains "To rename bucket use the RENAME button below"
  When dismiss the Edit Bucket Popup
  Then the Edit Bucket Popup is not present
  When click on settings for bucket named "Oprava"
  Then the Edit Bucket popup is loaded
  When cancel the Edit Bucket Popup
  Then the Edit Bucket Popup is not present
  When click on settings for bucket named "Oprava"
  When set the retention policy of the bucket as "48h"
  When click Edit Bucket Popup Save Changes
  # N.B. fix following once issue 14905 is resolved
  Then the bucket named "Oprava" has a Retention Policy of "48h"

@tested
Scenario: Filter Buckets
  When enter "denn" in the Buckets filter field
  Then the buckets are sorted as "Denní,Půldenní,Týdenní"
  Then the bucket "Oprava" is not in the list

@error-collateral
Scenario: Clear Filter
  When clear the Buckets filter field
  Then the bucket named "Oprava" is in the list
  Then the bucket named "_monitoring" is in the list
  Then the bucket named "_tasks" is in the list
  Then the bucket named "Týdenní" is in the list

@tested
Scenario: Sort Buckets by Name
  When click the sort type dropdown
  When click sort by item "Name Desc"
  #Given ensure buckets name sort order "desc"
  Then the buckets are sorted as "_monitoring,_tasks,Denní,Hodinová,Měsíční,Oprava,Půldenní,DEFAULT,Trvalá,Týdenní"
  When click the sort type dropdown
  When click sort by item "Name Asc"
  # Given ensure buckets name sort order "asc"
  Then the buckets are sorted as "Týdenní,Trvalá,DEFAULT,Půldenní,Oprava,Měsíční,Hodinová,Denní,_tasks,_monitoring"
  When click the sort type dropdown
  When click sort by item "Name Desc"
  #Given ensure buckets name sort order "desc"

@error-collateral
Scenario: Sort Buckets by Retention Policy
  When click the sort type dropdown
  When click sort by item "retentionRules[0].everySeconds-asc"
  #When click buckets sort by retention policy
  Then the buckets are sorted as "Hodinová,Půldenní,Denní,Oprava,_tasks,Týdenní,_monitoring,Měsíční,Trvalá,DEFAULT"
  When click the sort type dropdown
  When click sort by item "retentionRules[0].everySeconds-desc"
  #When click buckets sort by retention policy
  Then the buckets are sorted as "DEFAULT,Trvalá,Měsíční,Týdenní,_monitoring,_tasks,Oprava,Denní,Půldenní,Hodinová"

@tested
Scenario Outline: Delete Buckets
# following check leads to troublesome false positives - todo fix it
#  Then the delete button of the card named "<Name>" is not present
  When hover over bucket card named "<Name>"
  When click the delete button of the card named "<Name>"
  When click the confirm delete button of the card named "<Name>"
  Then the bucket card named "<Name>" is not in the list

Examples:
  | Name |
  | Trvalá |
  | Měsíční |
  | Týdenní |
  | Denní |
  | Půldenní |
  | Hodinová  |
  | Oprava    |


@tested
Scenario: Add Manual Line Protocol Data to Default
  Then the add data popover is not present
  When click add data button for bucket "DEFAULT"
  Then the add data popover for the bucket "DEFAULT" is visible
  When click bucket card popover item "Line Protocol"
  Then the first page of the Line Protocol Wizard is loaded
  When click radio button "Enter Manually"
  Then the data point text area is visible
  When click the Line Protocol wizard precision dropdown
  When click the line Protocol wizard precision "ms"
  When enter "12" datapoints with value named "foo" starting at "-2h" with "fibonacci" data of type "int" and prec "ms"
  When click the Line Protocol wizard write button
  Then the line Protocol wizard second step opens
  Then the Line Protocol wizard step status message is "Data Written Successfully"
  When click the Line Protocol wizard close button
  Then the line Protocol wizard is not present
  When API sign in user "DEFAULT"
  Then the bucket "DEFAULT" for user "DEFAULT" contains:
"""
{ "points": 12, "field": "foo", "measurement": "fibonacci", "start": "-3h", "vals": [1,233], "rows": ["0","-1"] }
"""

@error-collateral
  Scenario: Add Manual Line Protocol Bad Data to Default
    Then the add data popover is not present
    When click add data button for bucket "DEFAULT"
    Then the add data popover for the bucket "DEFAULT" is visible
    When click bucket card popover item "Line Protocol"
    Then the first page of the Line Protocol Wizard is loaded
    When click radio button "Enter Manually"
    Then the data point text area is visible
    When enter "bad data" into the line protocol text area
    When click the Line Protocol wizard write button
    Then the line Protocol wizard second step opens
    Then the Line Protocol wizard step status message contains "Unable to Write Data"
    When click the Line Protocol wizard cancel button
    Then the data point text area is visible
    When click the Line Protocol wizard close icon
    Then the line Protocol wizard is not present

@tested
  Scenario: Add Line Protocol Data from File to Default
    When generate a line protocol testdata file "etc/test-data/line-protocol-hydro.txt" based on:
    """
    { "points": 20, "measurement":"level", "start": "-60h", "algo": "hydro", "prec": "sec", "name": "hydro"}
    """
    When click add data button for bucket "DEFAULT"
    Then the add data popover for the bucket "DEFAULT" is visible
    When click bucket card popover item "Line Protocol"
    Then the first page of the Line Protocol Wizard is loaded
    When click radio button "Upload File"
    When add the file "etc/test-data/line-protocol-bogus.txt" to the Line Protocol Wizard file upload
    Then the popup wizard import file header contains "line-protocol-bogus.txt"
    When click the Line Protocol wizard write button
    #When click the Line Protocol wizard continue button
    Then the popup wizard step state text contains "Unable to Write Data"
    # appears error class no longer being used 13.8.20
    #Then the popup wizard step is in state "error"
    When click the Line Protocol wizard cancel button
    #When click the bucket data wizard previous button
    When click the Line Protocol wizard precision dropdown
    When click the line Protocol wizard precision "ms"
    When add the file "etc/test-data/line-protocol-hydro.txt" to the Line Protocol Wizard file upload
    Then the popup wizard import file header contains "line-protocol-hydro.txt"
    When click the Line Protocol wizard write button
    #When click the Line Protocol wizard continue button
    Then the popup wizard step state text contains "Data Written Successfully"
    # Success class no longer used 13.8.20
    #Then the popup wizard step is in state "success"
    When click the Line Protocol wizard close button
    Then the line Protocol wizard is not present
    #Then the bucket "DEFAULT" for user "DEFAULT" contains "20" datapoints of "hydro" data with value named "level" starting at "-60h"
    Then the bucket "DEFAULT" for user "DEFAULT" contains:
    """
    { "points": 20, "field": "level", "measurement": "hydro", "start": "-72h", "vals": "skip", "rows": ["0","-1"], "name": "hydro" }
    """

@tested
  Scenario: Add Scraper to Default
    When click add data button for bucket "DEFAULT"
    Then the add data popover for the bucket "DEFAULT" is visible
    When click bucket card popover item "Scrape Metrics"
    When enter the name "Courbet" into the Create Scraper popup name input
    When click the create scraper create button
    Then the success notification contains "Scraper was created successfully"
    When click load data tab "Scrapers"
    Then there is a scraper card for "Courbet"

  Scenario: Add Telegraf to Default
    When click nav menu item "LoadData"
    When click load data tab "Buckets"
    When click add data button for bucket "DEFAULT"
    Then the add data popover for the bucket "DEFAULT" is visible
    When click bucket card popover item "Configure Telegraf Agent"
    Then the Create Telegraf Config Wizard is loaded
    When click the buckets dropdown button
    When select the buckets dropdown item "DEFAULT"
    When select the telegraf plugin tile "System"
    When click the Popup Wizard continue button
    When enter the telegraf name "Daumier"
    When click the Popup Wizard continue button
    Then the success notification contains "telegraf plugin: system."
    Then the success notification contains "configurations have been saved"
    When close all notifications
    When click the Popup Wizard continue button
    When click load data tab "Telegrafs"
    Then there is a telegraf card for "Daumier"

    #TODO - new functionality - click bucket name opens data explorerer


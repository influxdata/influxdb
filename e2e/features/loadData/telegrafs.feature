@feature-loadData
@loadData-telegrafs
Feature: Load Data - Telegrafs

Scenario: Load Initial Telegraf tab
  Given I reset the environment
  Given run setup over REST "DEFAULT"
  When API sign in user "DEFAULT"
  When API create a bucket named "Duchamp" for user "DEFAULT"
  When API create a label "Cesko" described as "Pravda vitezi" with color "#AAFFAA" for user "DEFAULT"
  When API create a label "Mesto" described as "Matka mest" with color "#FFAAAA" for user "DEFAULT"
  When open the signin page
  When UI sign in user "DEFAULT"
  When click nav menu item "LoadData"
  When click load data tab "Telegrafs"
  Then the telegrafs tab is loaded

Scenario: Exercise create Telegraf wizard
  When click the create Telegraf button empty
  Then the Create Telegraf Wizard is loaded
  When dismiss the Create Telegraf Wizard
  Then the Create Telegraf Wizard is no longer present
  When click the create Telegraf button in header
  When click the select bucket dropdown in the Create Telegraf Wizard
  When click the bucket item "DEFAULT" in the Create Telegraf Wizard
  When enter the value "NGI" in create Telegraf Wizard filter plugins
  Then the Create Telegraf wizard plugin tile "NGINX" is visible
  Then the Create Telegraf wizard plugin tile "System" is not present
  Then the Create Telegraf wizard plugin tile "Redis" is not present
  Then the Create Telegraf wizard plugin tile "Docker" is not present
  When clear the create Telegraf Wizard plugin filter
  When click the plugin tile "System" in the Create Telegraf Wizard
  When click the Popup Wizard continue button
  Then the create Telegraf Wizard second step is loaded
  Then the create Telegraf plugins sidebar contains "cpu,disk,diskio,mem,net,processes,swap,system"
  Then the create Telegraf plugin sidebar "system" item is in state "success"
  Then the create Telegraf plugin sidebar "cpu" item is in state "success"
  Then the create Telegraf plugin sidebar "mem" item is in state "success"
  When dismiss the Create Telegraf Wizard
  Then the Create Telegraf Wizard is no longer present
  When click the create Telegraf button empty

Scenario Outline: Edit Plugin Values
  When click the plugin tile "<PLUGIN>" in the Create Telegraf Wizard
  When click the Popup Wizard continue button
  Then the create Telegraf Wizard second step is loaded
  Then the create Telegraf plugins sidebar contains "<PLUGIN>"
  Then the create Telegraf plugin sidebar "<PLUGIN>" item is in state "neutral"
  When click the create Telegraf plugin sidebar "<PLUGIN>" item
  Then the create Telegraf edit plugin "<PLUGIN>" step is loaded
 # When click the Popup Wizard done button
 # Then the create Telegraf plugin sidebar "<PLUGIN>" item is in state "error"
 # When click the create Telegraf plugin sidebar "<PLUGIN>" item
  When enter the values <FAKE_VALUES> into the fields <FIELDS>
  Then verify the edit plugin error notification with message "<ERRMSGS>"
  When clear the create Telegraf edit plugin fields <FIELDS>
  When enter the values <TRUE_VALUES> into the fields <FIELDS>
  When click the Popup Wizard done button
  Then the create Telegraf plugin sidebar "<PLUGIN>" item is in state "success"
  When click the wizard previous button
  Then the Create Telegraf Wizard is loaded
  Then the Create Telegraf wizard plugin tile "<PLUGIN>" is selected
  When click the plugin tile "<PLUGIN>" in the Create Telegraf Wizard
  Then the Create Telegraf wizard plugin tile "<PLUGIN>" is not selected
  Then the popup wizard continue button is disabled

  Examples:
  | PLUGIN     | FAKE_VALUES | FIELDS           | ERRMSGS               | TRUE_VALUES |
  | Docker     | SKIP        | endpoint         | SKIP                 | http://localhost:10080 |
  | Kubernetes | ASDF        | url              | Must be a valid URI. | http://localhost:10080 |
  | Redis      | SKIP,SKIP   | servers,password | SKIP                 | tcp://localhost:6379,wumpus |

Scenario: Edit NGINX Plugin Values
  When click the plugin tile "NGINX" in the Create Telegraf Wizard
  When click the Popup Wizard continue button
  Then the create Telegraf Wizard second step is loaded
  Then the create Telegraf plugins sidebar contains "NGINX"
  Then the create Telegraf plugin sidebar "NGINX" item is in state "neutral"
  When click the create Telegraf plugin sidebar "NGINX" item
  Then the create Telegraf edit plugin "NGINX" step is loaded
  When enter the values ASDF into the fields urls
  Then verify the edit plugin error notification with message "NONE"
  When clear the create Telegraf edit plugin fields urls
  When enter the values http://localhost:10080 into the fields urls
  When click the NGINX configuration add button
  Then the NGINX configuration URLs list contains "1" items
  When click delete for the first NGINX configuration URL
  When click confirm delete of NGINX configuration URL
  Then the NGINX configuration URLs list is empty
  When click the Popup Wizard done button
  Then the create Telegraf plugin sidebar "NGINX" item is in state "error"
  When click the create Telegraf plugin sidebar "NGINX" item
  When enter the values http://localhost:10080 into the fields urls
  When click the NGINX configuration add button
  When click the Popup Wizard done button
  Then the create Telegraf plugin sidebar "NGINX" item is in state "success"
  When click the wizard previous button
  Then the Create Telegraf Wizard is loaded
  Then the Create Telegraf wizard plugin tile "NGINX" is selected
  When click the plugin tile "NGINX" in the Create Telegraf Wizard
  Then the Create Telegraf wizard plugin tile "NGINX" is not selected
  Then the popup wizard continue button is disabled

Scenario: Cleanup from Edit Plugin Values
  When dismiss the Create Telegraf Wizard

#N.B. just add UI artifacts - no need to check backend at this point
Scenario Outline: Create Telegraf
  When click the create Telegraf button in header
  When click the select bucket dropdown in the Create Telegraf Wizard
  When click the bucket item "<BUCKET>" in the Create Telegraf Wizard
  When click the plugin tile "<PLUGIN>" in the Create Telegraf Wizard
  When click the Popup Wizard continue button
  When enter the name "<NAME>" in the Create Telegraf Wizard
  When enter the description "<DESCR>" in the Create Telegraf Wizard
  When click the Popup Wizard continue button
  Then the success notification contains "<SUCCESS_MSG>"
  Then the success notification contains "Your configurations have been saved"
  When close all notifications
  Then the create Telegraf Wizard final step is loaded
  When click the Create Telegraf Wizard finish button
  Then there is a telegraf card for "<NAME>"
  Then the bucket of the telegraf card "<NAME>" is "<BUCKET>"


  Examples:
    | PLUGIN     | BUCKET  | NAME      | DESCR        | SUCCESS_MSG |
    | System     | DEFAULT |Strakonice | Lorem ipsum  | Successfully created dashboards for telegraf plugin: system. |
    | Docker     | Duchamp |Decin      | Lorem ipsum  | SKIP |
    | Kubernetes | DEFAULT |Kladno     | Lorem ipsum  | SKIP |
    | NGINX      | Duchamp |Nymburk    | Lorem ipsum  | SKIP |
    | Redis      | DEFAULT |Rakovnik   | Lorem ipsum  | SKIP |

Scenario: Sort Telegrafs by Name
  Then the telegraf sort order is "Decin,Kladno,Nymburk,Rakovnik,Strakonice"
  When click the sort type dropdown
  When click sort by item "Name Desc"
  #When click the telegraf sort by name button
  Then the telegraf sort order is "Strakonice,Rakovnik,Nymburk,Kladno,Decin"
  When click the sort type dropdown
  When click sort by item "Name Asc"
  #When click the telegraf sort by name button
  Then the telegraf sort order is "Decin,Kladno,Nymburk,Rakovnik,Strakonice"

Scenario: Filter Telegrafs
  When enter the value "Rak" into the Telegrafs filter
  Then the telegraf sort order is "Rakovnik,Strakonice"
  Then the telegraf cards "Decin,Kldano,Nymburk" are no longer present
  When clear the Telegrafs filter
  Then the telegraf sort order is "Decin,Kladno,Nymburk,Rakovnik,Strakonice"

Scenario: Verify setup instructions
  When click on setup instructions for the telegraf card "Decin"
  Then the telegraf setup instruction popup is loaded
  When dismiss the popup
  Then popup is not loaded

Scenario: Verify configuration
  When click on the name of the telegraf card "Nymburk"
  Then the telegraf configuration popup for "Nymburk" is loaded
  When dismiss the popup
  Then popup is not loaded

Scenario: Edit Telegraf Card
  When hover over the name of the telegraf card "Nymburk"
  When click the name edit icon of the telegraf card "Nymburk"
  When clear the name input of the telegraf card "Nymburk"
  When set the name input of the telegraf card "Nymburk" to "Norimberk"
  Then the Telegraf Card "Nymburk" can no longer be found
  Then there is a telegraf card for "Norimberk"
  When hover over the description of the telegraf Card "Norimberk"
  When click the description edit icon of the telegraf card "Norimberk"
  When clear the desrciption input of the telegraf card "Norimberk"
  When set the description input of the telegraf card "Norimberk" to "Hunt the Wumpus"
  Then the description of the telegraf card "Norimberk" is "Hunt the Wumpus"

  Scenario: Add labels to telegraf
    Then the Label Popup for the Telegraf Card "Kladno" is not present
    When click Add Label for Telegraf Card "Kladno"
    Then the Label Popup for the Telegraf Card "Kladno" is visible
    Then the item "Cesko" is in the Telegraf Card "Kladno" label select list
    Then the item "Mesto" is in the Telegraf Card "Kladno" label select list
    When filter the Telegraf Card "Kladno" label list with "Ce"
    Then the item "Cesko" is in the Telegraf Card "Kladno" label select list
    Then the item "Mesto" is NOT in the Telegraf Card "Kladno" label select list
    When clear the label filter of the Telegraf Card "Kladno"
    Then the item "Mesto" is in the Telegraf Card "Kladno" label select list
    When click the item "Cesko" is in the Telegraf Card "Kladno" label select list
    Then there is a label pill "Cesko" for the Telegraf Card "Kladno"
    Then the item "Cesko" is NOT in the Telegraf Card "Kladno" label select list
    When click the item "Mesto" is in the Telegraf Card "Kladno" label select list
    Then there is a label pill "Mesto" for the Telegraf Card "Kladno"
    Then the item "Mesto" is NOT in the Telegraf Card "Kladno" label select list
    Then the label select list for "Kladno" shows the empty state message
    When enter the value "Lidstvo" into the Telegraf Card "Kladno" label filter
    Then the create Label popup is loaded
    When dismiss the popup
    Then popup is not loaded

  Scenario: Delete label from telegraf
    When hover over the label pill "Cesko" for the Telegraf Card "Kladno"
    When click delete the label pill "Cesko" for the Telegraf Card "Kladno"
    Then the label pill "Cesko" for the Telegraf Card "Kladno" is NOT present
    When click Add Label for Telegraf Card "Kladno"
    # Affected by issue 16528
    Then the item "Cesko" is in the Telegraf Card "Kladno" label select list
    # lose focus
    When click telegraf card "Kladno"
    Then the Label Popup for the Telegraf Card "Kladno" is not present

Scenario Outline: Delete Telegraf Card
  When hover over telegraf card "<NAME>"
  When click delete for telegraf card "<NAME>"
  When click delete confirm for telegraf card "<NAME>"
  Then the Telegraf Card "<NAME>" can no longer be found

  Examples:
  |NAME|
  |Decin|
  |Strakonice|
  |Kladno    |
  |Norimberk |
  |Rakovnik  |

# N.B. can verify telegrafs at endpoint http://localhost:8086/api/v2/telegrafs
# TODO - Test installation of telegraf and instructions - check back end

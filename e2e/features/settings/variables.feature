@feature-settings
@settings-variables
Feature: Settings - Variables
  As a user I want to Read Create Update and Delete Variables
  So that I can eventually reuse them in alerts and dashboards in Influxdbv2

  Scenario: Open Variables Tab
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When clear browser storage
    When UI sign in user "DEFAULT"
    When click nav menu item "Settings"
    When click the settings tab "Variables"
    Then the variables Tab is loaded

  Scenario: Exercise Import Variable Popup
    When click create variable dropdown in header
    When click "import" variable dropdown item
    Then the import variable popup is loaded
    Then dismiss the popup
    Then popup is not loaded
    When click create variable dropdown empty
    When click "import" variable dropdown item
    Then the import variable popup is loaded
    When click the Import Variable popup paste JSON button
    Then the Import Variable JSON textarea is visible
    Then the Import JSON as variable button is enabled
    When click the Import Variable popup Upload File button
    Then the Import Variable JSON textarea is not visible
    Then the Import JSON as variable button is not enabled
    Then the Import JSON file upload area is present
    Then dismiss the popup
    Then popup is not loaded

@tested
  Scenario: Exercise Create Variable Popup
    When click create variable dropdown in header
    When click "new" variable dropdown item
    Then the create variable popup is loaded
    Then dismiss the popup
    Then popup is not loaded
    When click create variable dropdown empty
    When click "new" variable dropdown item
    Then the create variable popup is loaded
    Then the create variable popup selected type is "Query"
    Then the create variable popup create button is disabled
    Then the create variable popup script editor is visible
    When click the create variable popup type dropdown
    When click the create variable popup type dropdown item "Map"
    Then the create variable popup selected type is "Map"
    Then the create variable popup create button is disabled
    Then the create variable popup script editor is not visible
    Then the create variable popup textarea is visible
    Then the create variable popup default value dropdown is visible
    Then the create variable popup info line contains "0" items
    When click the create variable popup type dropdown
    When click the create variable popup type dropdown item "constant"
    Then the create variable popup selected type is "CSV"
    Then the create variable popup create button is disabled
    Then the create variable popup script editor is not visible
    Then the create variable popup textarea is visible
    Then the create variable popup default value dropdown is visible
    Then the create variable popup info line contains "0" items
    When click the create variable popup type dropdown
    When click the create variable popup type dropdown item "Query"
    Then the create variable popup selected type is "Query"
    Then the create variable popup create button is disabled
    Then the create variable popup script editor is visible
    Then the create variable popup textarea is not visible
    Then the create variable popup default value dropdown is not visible
    Then the create variable popup info line is not visible
    Then click popup cancel simple button
    Then popup is not loaded

  Scenario Outline: Import Variable
    When click create variable dropdown in header
    When click "import" variable dropdown item
    When upload the import variable file "<FILE>"
    Then the import variable drag and drop header contains success "<FILE>"
    When click the import variable import button
    Then popup is not loaded
    Then the success notification contains "Successfully created new variable: <NAME>"
    Then close all notifications
    Then there is a variable card for "<NAME>"

  Examples:
    |NAME|FILE|
    |Bucket|etc/test-data/variable-query-bucket.json|
    |Ryby|etc/test-data/variable-map-ryby.json|
    |Jehlicnany|etc/test-data/variable-map-jehlicnany.json|
    |Slavia|etc/test-data/variable-csv-slavia.json|
    |Arsenal|etc/test-data/variable-csv-arsenal.json|


  # TODO - failover on bad variable file
@tested
  Scenario: Import Bad Variable
    When click create variable dropdown in header
    When click "import" variable dropdown item
    When upload the import variable file "etc/test-data/variable-empty.json"
    Then the import variable drag and drop header contains success "etc/test-data/variable-empty.json"
    When click the import variable import button
    Then popup is not loaded
    Then the error notification contains "Failed to create variable:"
    Then close all notifications

  Scenario: Create Map Variable
    When click create variable dropdown in header
    When click "new" variable dropdown item
    When click the create variable popup type dropdown
    When click the create variable popup type dropdown item "map"
    When clear the create variable popup name input
    When enter the create variable popup name "Primaty"
    When enter the create variable popup values:
    """
    Human,homo sapiens
    Orangutan,pongo
    Chimpanzee,pan troglodytes
    Gorilla,gorilla
    Baboon,papio
    """
    When click the create variable popup title
    Then the create variable popup info line contains "5" items
    Then the selected default variable dropdown item is "Human"
    When click the create variable popup default dropdown
    When click the create variable popup default dropdown item "Chimpanzee"
    Then the selected default variable dropdown item is "Chimpanzee"
    When click the create variable popup create button
    Then popup is not loaded
    Then the success notification contains "Successfully created new variable: Primaty"
    Then close all notifications
    Then there is a variable card for "Primaty"

  Scenario: Create CSV Variable
    When click create variable dropdown in header
    When click "new" variable dropdown item
    When click the create variable popup type dropdown
    When click the create variable popup type dropdown item "constant"
    When clear the create variable popup name input
    When enter the create variable popup name "Obdobi"
    When enter the create variable popup values:
    """
    Antropocen,Holocen,Svrchni pleistocen,Stredni pleistocen,Spodni pleistocen
    """
    When click the create variable popup title
    Then the create variable popup info line contains "5" items
    Then the selected default variable dropdown item is "Antropocen"
    When click the create variable popup default dropdown
    When click the create variable popup default csv dropdown item "Holocen"
    Then the selected default variable dropdown item is "Holocen"
    When click the create variable popup create button
    Then popup is not loaded
    Then the success notification contains "Successfully created new variable: Obdobi"
    Then close all notifications
    Then there is a variable card for "Obdobi"

  Scenario: Create CSV Variable
    When click create variable dropdown in header
    When click "new" variable dropdown item
    When click the create variable popup type dropdown
    When click the create variable popup type dropdown item "constant"
    When clear the create variable popup name input
    When enter the create variable popup name "Reals"
    When enter the create variable popup values:
    """
    1.0,2.0,2.72,3.0,3.14
    """
    When click the create variable popup title
    Then the create variable popup info line contains "5" items
    Then the selected default variable dropdown item is "1.0"
    When click the create variable popup default dropdown
    When click the create variable popup default csv dropdown item "2.0"
    Then the selected default variable dropdown item is "2.0"
    When click the create variable popup create button
    Then popup is not loaded
    Then the success notification contains "Successfully created new variable: Reals"
    Then close all notifications
    Then there is a variable card for "Reals"


  Scenario: Create Query Variable
    When click create variable dropdown in header
    When click "new" variable dropdown item
    When click the create variable popup type dropdown
    When click the create variable popup type dropdown item "Query"
    When clear the create variable popup name input
    When enter the create variable popup name "Kybl"
    When enter the create variable popup Monaco Editor text:
    """
    buckets()
   |> filter(fn: (r) => r.name !~ /^_/)
   |> rename(columns: {name: '_value'})
   |> keep(columns: ['_value'])
    """
    When click the create variable popup create button
    Then popup is not loaded
    Then the success notification contains "Successfully created new variable: Kybl"
    Then close all notifications
    Then there is a variable card for "Kybl"

  Scenario: Filter Variables By Name
    When enter the value "yb" into the variables filter
    Then the variable cards "Kybl,Ryby" are visible
    Then the variable cards "Arsenal,Bucket,Jehlicnany,Obdobi,Primaty,Reals,Slavia" are not present
    Then the variable cards are sorted as "Kybl,Ryby"
    When click the sort type dropdown
    When click sort by item "Name Desc"
    Then the variable cards are sorted as "Ryby,Kybl"
    When click the sort type dropdown
    When click sort by item "Name Asc"
    #When click the variable sort by name button
    When clear the variables filter
    Then the variable cards "Arsenal,Bucket,Jehlicnany,Kybl,Obdobi,Primaty,Reals,Ryby,Slavia" are visible

  # Sort by type not working - TODO fix after Issue 15379 fixed

  Scenario: Sort Variables by name
    Then the variable cards are sorted as "Arsenal,Bucket,Jehlicnany,Kybl,Obdobi,Primaty,Reals,Ryby,Slavia"
    When click the sort type dropdown
    When click sort by item "Name Desc"
    #When click the variable sort by name button
    # has third neutral state -- not any more
    #When click the variable sort by name button
    Then the variable cards are sorted as "Slavia,Ryby,Reals,Primaty,Obdobi,Kybl,Jehlicnany,Bucket,Arsenal"
    When click the sort type dropdown
    When click sort by item "Name Asc"
    #When click the variable sort by name button
    Then the variable cards are sorted as "Arsenal,Bucket,Jehlicnany,Kybl,Obdobi,Primaty,Reals,Ryby,Slavia"

  Scenario: Rename Variable
    When hover over variable card named "Ryby"
    When click the context menu of the variable "Ryby"
    When click the context menu item "Rename" of the variable "Ryby"
    Then the variable name warning popup is visible
    Then dismiss the popup
    Then popup is not loaded
    When hover over variable card named "Ryby"
    When click the context menu of the variable "Ryby"
    When click the context menu item "Rename" of the variable "Ryby"
    Then the variable name warning popup is visible
    When click the rename variable warning popup understand button
    When click popup cancel simple button
    Then popup is not loaded
    When hover over variable card named "Ryby"
    When click the context menu of the variable "Ryby"
    When click the context menu item "Rename" of the variable "Ryby"
    When click the rename variable warning popup understand button
    When clear the rename variable popup name input
    Then the rename variable form warning states "Variable name cannot be empty"
    Then the rename variable from warning icon is visible
    Then the rename variable submit button is disabled
    When enter the new variable name "Kocky"
    When click rename variable popup submit button
    Then the success notification contains "Successfully updated variable: Kocky."
    Then close all notifications
    Then there is a variable card for "Kocky"

  Scenario: Edit Map Variable to CSV
    When click the variable card name "Kocky"
    Then the edit variable popup is loaded
    When click the edit variable popup type dropdown
    When click the edit variable popup type dropdown item "constant"
    Then the edit variable name input is disabled
    When enter the edit variable popup values:
    """
    Angora,Siamska,Barmska,Kartuzska,Ruska Modra
    """
    When click the edit variable popup title
    Then the create variable popup info line contains "5" items
    Then the selected default variable dropdown item is "Angora"
    When click the edit variable popup default dropdown
    When click the edit variable popup default csv dropdown item "Kartuzska"
    Then the selected default variable dropdown item is "Kartuzska"
    When click the edit variable popup submit button
    Then popup is not loaded
    Then the success notification contains "Successfully updated variable: Kocky."
    Then close all notifications
    Then there is a variable card for "Kocky"


  Scenario: Edit CSV Variable to Query
    When click the variable card name "Slavia"
    Then the edit variable popup is loaded
    When click the edit variable popup type dropdown
    When click the edit variable popup type dropdown item "query"
    Then the edit variable name input is disabled
    When enter the edit variable popup Query text:
    """
        buckets()
   |> filter(fn: (r) => r.name !~ /^_/)
   |> rename(columns: {name: '_value'})
   |> keep(columns: ['_value'])
    """
    When click the edit variable popup title
    When click the edit variable popup submit button
    Then popup is not loaded
    Then the success notification contains "Successfully updated variable: Slavia."
    Then close all notifications
    Then there is a variable card for "Slavia"


  Scenario: Edit Query Variable to Map
    When click the variable card name "Kybl"
    Then the edit variable popup is loaded
    When click the edit variable popup type dropdown
    When click the edit variable popup type dropdown item "map"
    Then the edit variable name input is disabled
    Then the edit variable popup textarea is cleared
    When enter the edit variable popup values:
    """
    kolac,tvarohovy
    kobliha,jahodova
    buchta,svestkova
    babovka,vanilkova
    loupak,cokoladovy
    pernik,domaci
    """
    When click the edit variable popup title
    Then the create variable popup info line contains "6" items
    Then the selected default variable dropdown item is "kolac"
    When click the edit variable popup default dropdown
    When click the edit variable popup default csv dropdown item "babovka"
    Then the selected default variable dropdown item is "babovka"
    When click the edit variable popup submit button
    Then popup is not loaded
    Then the success notification contains "Successfully updated variable: Kybl."
    Then close all notifications
    Then there is a variable card for "Kybl"

@tested
  Scenario Outline: Delete Variable
    When hover over variable card named "<NAME>"
    When click delete menu of variable card named "<NAME>"
    When click delete confirm of variable card named "<NAME>"
    Then the success notification contains "Successfully deleted the variable"
    Then the variable card "<NAME>" is not present

    Examples:
      |NAME|
      |Kybl|
      |Kocky |
      |Jehlicnany|
      |Slavia    |
      |Arsenal   |
      |Bucket    |
      |Obdobi    |
      |Primaty   |
      |Reals     |

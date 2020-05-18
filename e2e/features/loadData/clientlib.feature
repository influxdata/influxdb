@feature-loadData
@loadData-clientlib
Feature: Load Data - Client Libs
  As a user I want to Read Create Update and Delete Client Libraries
  So that I can manage the stores used with Influxdbv2

  Scenario: Load Initial Client Lib Tab
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When API sign in user "DEFAULT"
    When API create a bucket named "Duchamp" for user "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When click nav menu item "LoadData"
    When click load data tab "Client Libraries"
    Then the Client Libraries tab is loaded

  Scenario: Open C# Popup
    When click the "csharp" client library tile
    Then the csharp info popup is loaded
    Then click copy "Package Manager" to clipboard
    Then the success notification contains "has been copied to clipboard"
    #Then verify clipboard contains text of "Package Manager"
    When close all notifications
    Then verify the github repository link contains "influxdb-client-csharp"
    Then dismiss the popup

  Scenario: Open Go Popup
    When click the "go" client library tile
    Then the go info popup is loaded
    Then verify the github repository link contains "influxdb-client-go"
    Then dismiss the popup

  Scenario: Open Java Popup
    When click the "java" client library tile
    Then the java info popup is loaded
    Then verify the github repository link contains "influxdb-client-java"
    Then dismiss the popup

  Scenario: Open Node Popup
    When click the "javascript-node" client library tile
    Then the node info popup is loaded
    Then verify the github repository link contains "influxdb-client-js"
    Then dismiss the popup

  Scenario: Open Python Popup
    When click the "python" client library tile
    Then the python info popup is loaded
    Then verify the github repository link contains "influxdb-client-python"
    Then dismiss the popup

  # TODO - check copy to clipboard buttons - N.B. clipboard not available in chromedriver headless

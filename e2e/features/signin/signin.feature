@feature-signin
@signin-signin
Feature: Signin
  Use and abuse the signin page

  Scenario: Basic Signin
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When open the signin page
    When clear browser storage
    #Then the heading contains "InfluxData"
    Then the InfluxData heading is visible
    Then the version shown contains "DEFAULT"
    #Then the credits are valid
    When enter the username "DEFAULT"
    When enter the password "DEFAULT"
    When click the signin button
    Then the home page is loaded

@tested
   Scenario Outline: Signin Bad Credentials
     Given I reset the environment
     Given run setup over REST "DEFAULT"
     When open the signin page
     When enter the username "<USERNAME>"
     When enter the password "<PASSWORD>"
     When click the signin button
     Then the error notification contains "<MESSAGE>"

     Examples:
     | USERNAME | PASSWORD | MESSAGE |
     | wumpus   | DEFAULT  | Could not sign in |
     | DEFAULT  | wuumpuus | Could not sign in |


     # N.B. TODO - consider Security scenarios - Brute force password, injection attack

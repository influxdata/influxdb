@feature-loadData
@loadData-tokens
Feature: Load Data - Tokens

@tested
  Scenario: Load Initial Tokens tab
    Given I reset the environment
    Given run setup over REST "DEFAULT"
    When API sign in user "DEFAULT"
    When API create a bucket named "Duchamp" for user "DEFAULT"
    When API create a bucket named "Courbet" for user "DEFAULT"
    When API create a bucket named "Corot" for user "DEFAULT"
    When open the signin page
    When UI sign in user "DEFAULT"
    When click nav menu item "LoadData"
    When click load data tab "Tokens"
    Then the tokens tab is loaded
    Then the tokens list contains the token described as "admin's Token"

@tested
  Scenario: Exercise Create Read/Write Token Popup
    When click the generate token dropdown
    When click the generate token item "read-write"
    Then the generate read-write token popup is loaded
    When click the "Read" radio button "All Buckets"
    Then the "Read" panel shows the empty state text
    Then the bucket selector for the "Read" panel is not present
    When click the "Write" radio button "All Buckets"
    Then the "Write" panel shows the empty state text
    Then the bucket selector for the "Write" panel is not present
    When click the "Read" radio button "Scoped"
    Then the "Read" panel empty state text is not present
    Then the "Read" panel bucket selector is present
    When click the "Write" radio button "Scoped"
    Then the "Write" panel empty state text is not present
    Then the "Write" panel bucket selector is present
    Then the "Read" panel bucket list contains "DEFAULT,Corot,Courbet,Duchamp"
    Then the "Write" panel bucket list contains "DEFAULT,Corot,Courbet,Duchamp"
    When filter the "Read" panel bucket selector with "Co"
    Then the "Read" panel bucket list contains "Corot,Courbet"
    Then the "Read" panel bucket list does not contain "DEFAULT,Duchamp"
    When clear the "Read" panel bucket selector
    Then the "Read" panel bucket list contains "DEFAULT,Corot,Courbet,Duchamp"
    When filter the "Write" panel bucket selector with "Co"
    Then the "Write" panel bucket list contains "Corot,Courbet"
    Then the "Write" panel bucket list does not contain "DEFAULT,Duchamp"
    When clear the "Write" panel bucket selector
    Then the "Write" panel bucket list contains "DEFAULT,Corot,Courbet,Duchamp"
    When click "Read" panel select all buckets
    Then the "Read" panel buckets "DEFAULT,Corot,Courbet,Duchamp" are selected
    When click "Read" panel deselect all buckets
    Then the "Read" panel buckets "DEFAULT,Corot,Courbet,Duchamp" are not selected
    When click the "Read" panel bucket "Courbet"
    Then the "Read" panel buckets "Courbet" are selected
    Then the "Read" panel buckets "DEFAULT,Corot,Duchamp" are not selected
    When click the "Read" panel bucket "Courbet"
    Then the "Read" panel buckets "DEFAULT,Corot,Courbet,Duchamp" are not selected
    When click "Write" panel select all buckets
    Then the "Write" panel buckets "DEFAULT,Corot,Courbet,Duchamp" are selected
    When click "Write" panel deselect all buckets
    Then the "Write" panel buckets "DEFAULT,Corot,Courbet,Duchamp" are not selected
    When click the "Write" panel bucket "Courbet"
    Then the "Write" panel buckets "Courbet" are selected
    Then the "Write" panel buckets "DEFAULT,Corot,Duchamp" are not selected
    When click the "Write" panel bucket "Courbet"
    Then the "Write" panel buckets "DEFAULT,Corot,Courbet,Duchamp" are not selected
    When dismiss the popup
    Then popup is not loaded
    When click the generate token dropdown
    When click the generate token item "read-write"
    Then the generate read-write token popup is loaded
    When click popup cancel button
    Then popup is not loaded

@error-collateral
  Scenario: Exercise Create All Access Token Popup
    When click the generate token dropdown
    When click the generate token item "all-access"
    Then the generate all-access token popup is loaded
    When dismiss the popup
    Then popup is not loaded
    When click the generate token dropdown
    When click the generate token item "all-access"
    When click all-access token popup cancel
    Then popup is not loaded

@tested
  Scenario Outline: Create Token
    When click the generate token dropdown
    When select token type based on <PRIVILEGES> type
    When set token description for <PRIVILEGES> as <DESCR>
    When set token privileges for <BUCKET> as <PRIVILEGES>
    When click popup save based on <PRIVILEGES>
    Then the success notification contains "Token was created successfully"
    When close all notifications
    Then the tokens list contains the token described as "<DESCR>"

  Examples:
    |DESCR|BUCKET|PRIVILEGES|
    | Un enterrement a Ornans | Courbet | RW |
    | Nu descendant un escalier | Duchamp | R |
    | La Femme a la perle       | Corot     | RW |
    | Dismaland                 | DEFAULT   | R  |
    | Cambpells Soup            | All       | RW |
    | La Jocande   | ALL                    |ALL |

@tested
  Scenario Outline: Disable Token
    When disable the token described as <DESCR>
    Then the token described as <DESCR> is disabled

  Examples:
    |DESCR|
    | Dismaland |
    | Nu descendant un escalier |
    | Cambpells Soup            |

  # Scenario: Sort By Status # not working see issue 15301

@error-collateral
  Scenario: Sort By Name
    Then the first tokens are sorted by description as "admin's Token, Campbells Soup, Dismaland, La Femme a la perle, La Jocande"
    When click the tokens sort By Name button
    Then the first tokens are sorted by description as "Un enterrement a Ornans, Nu descendant un escalier, La Jocande, La Femme a la perle, Dismaland"
    When click the tokens sort By Name button
    Then the first tokens are sorted by description as "admin's Token, Campbells Soup, Dismaland, La Femme a la perle, La Jocande"

@error-collateral
  Scenario: Edit Description
    When hover over the token description "La Jocande"
    When click the token description toggle for "La Jocande"
    When clear the edit input for description "La Jocande"
    When set the new description of "La Jocande" to "La Dame a l hermine"
    Then the success notification contains "Token was updated successfully"
    Then the tokens list contains the token described as "La Dame a l hermine"
    Then the tokens list does not contain the token described as "La Jocande"
    When close all notifications

@tested
  Scenario: Enable Token
    When enable the token described as "Nu descendant un escalier"
    Then the token described as "Nu descendant un escalier" is enabled
    Then the success notification contains "Token was updated successfully"
    When close all notifications

@tested
  Scenario Outline: Review Token
    When click on the token described as "<DESCR>"
    Then the review token popup is loaded
    Then the review token popup matches "<BUCKETS>" and "<PRIVILEGES>"
    When dismiss the popup
    Then popup is not loaded

  Examples:
    |DESCR|BUCKETS|PRIVILEGES|
    | Un enterrement a Ornans | Courbet | read,write |
    | Nu descendant un escalier | Duchamp | read |
    | Cambpells Soup            | All       | read,write |
    | La Dame a l hermine   | ALL                    |ALL |

@tested
  Scenario Outline: Delete Token
    When hover over token card described as "<DESCR>"
    When click the delete button of the token card described as "<DESCR>"
    #When click delete confirm of the token card described as "<DESCR>"
    When click token card popover delete confirm
    Then the success notification contains "Token was deleted successfully"
    Then the tokens list does not contain the token described as "<DESCR>"
    Then close all notifications

  Examples:
  |DESCR|
  | Un enterrement a Ornans |
  | Nu descendant un escalier |
  | La Femme a la perle       |
  | Dismaland                 |
  | Cambpells Soup            |
  |  La Dame a l hermine   |




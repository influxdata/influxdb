@feature-settings
@settings-labels
Feature: Settings - Labels
  As a user I want to Read Create Update and Delete Labels
  So that I can manage the tag items used with Influxdbv2

@tested
Scenario: Open Labels Tab
  Given I reset the environment
  Given run setup over REST "DEFAULT"
  When open the signin page
  When UI sign in user "DEFAULT"
  When click nav menu item "Settings"
  When click the settings tab "Labels"
  Then the labels Tab is loaded

@tested
Scenario: Exercise Create Label Popup
  When I click the empty state Create Label button
  Then the create Label popup is loaded
  When dismiss the popup
  Then popup is not loaded
  When I click the header Create Label button
  Then the create Label popup is loaded
  When cancel the create label popup
  Then popup is not loaded
  When I click the header Create Label button
  Then the color input color matches the preview color
  Then the preview label pill contains "Name this Label"
  When enter the value "Etiketa" into the label popup name input
  Then the preview label pill contains "Etiketa"
  When clear the label popup color input
  Then the form element error message is "Please enter a hexcode"
  When enter the value "#" into the label popup color input
  Then the form element error message is "Must be 7 characters"
  When clear the label popup color input
  When enter the value "#FFC0CB" into the label popup color input
  Then the color input color matches the preview color
  When click the color select button I'm feeling lucky
  Then the value in the label popup color input is not "#FFC0CB"
  Then the color input color matches the preview color
  When click the label popup color swatch "Ruby"
  Then the value in the label popup color input is "#BF3D5E"
  Then the color input color matches the preview color
  Then the label popup preview text color is "#FFFFFF"
  When click the label popup color swatch "Onyx"
  Then the value in the label popup color input is "#31313d"
  Then the color input color matches the preview color
  Then the label popup preview text color is "#FFFFFF"
  When click the label popup color swatch "Laser"
  Then the value in the label popup color input is "#00C9FF"
  Then the color input color matches the preview color
  Then the label popup preview text color is "#202028"
  When dismiss the popup
  Then popup is not loaded

@tested
Scenario Outline: Create Label
  When I click the header Create Label button
  When clear the label popup name input
  When enter the value "<NAME>" into the label popup name input
  When clear the label popup description input
  When enter the value "<DESCRIPTION>" into the label popup description input
  When set the color in the label popup to "<COLOR>"
  When click the label popup Create Label button
  Then there is a label card named "<NAME>" in the labels list
  Then the label card "<NAME>" has a pill colored "<COLOR>"
  Then the label card "<NAME>" has description "<DESCRIPTION>"

  Examples:
  | NAME | DESCRIPTION | COLOR |
  | Buk | Fagus sylvatica | Topaz |
  | Habr | Carpinus betulus | #D2691E |
  | Jilm | Ulmus laevis     | Thunder |
  | Javor | Acer pseudoplatanus | #924544 |
  | Bouleau | Betula verrucosa | #F5EAD5 |

@tested
Scenario: Edit Label
  When I click the Label Card Pill "Bouleau"
  Then the edit label popup is loaded
  When clear the label popup name input
  When enter the value "Briza" into the label popup name input
  When clear the label popup description input
  When enter the value "Betula pendula" into the label popup description input
  When set the color in the label popup to "#ECFC31"
  Then the color input color matches the preview color
  When click the label popup Save Changes button
  Then there is a label card named "Briza" in the labels list
  Then the label card "Briza" has a pill colored "#ECFC31"
  Then the label card "Briza" has description "Betula pendula"

@tested
Scenario: Sort By Name
  Then the first labels are sorted as "Briza,Buk,Habr,Javor,Jilm"
  When click the sort type dropdown
  When click sort by item "Name Desc"
  Then the first labels are sorted as "Jilm,Javor,Habr,Buk,Briza"
  When click the sort type dropdown
  When click sort by item "Name Asc"
  Then the first labels are sorted as "Briza,Buk,Habr,Javor,Jilm"

@error-collateral
Scenario: Sort By Description
  When click the sort type dropdown
  When click sort by item "Properties.Description Asc"
  #When click sort label by description
  Then the first labels are sorted as "Javor,Briza,Habr,Buk,Jilm"
  When click the sort type dropdown
  When click sort by item "Properties.Description Desc"
  #When click sort label by description
  Then the first labels are sorted as "Jilm,Buk,Habr,Briza,Javor"


@tested
  Scenario: Filter Labels
  When clear the labels filter input
  When enter the value "J" into the label filter
  Then the first labels are sorted as "Jilm,Javor"
  Then the labels "Briza,Buk,Habr" are not present
  When enter the value "AV" into the label filter
  Then the first labels are sorted as "Javor"
  Then the labels "Briza,Buk,Habr,Jilm" are not present
  When clear the labels filter input
  When enter the value "betul" into the label filter
  Then the first labels are sorted as "Habr,Briza"
  Then the labels "Buk,Javor,Jilm" are not present
  When click the sort type dropdown
  When click sort by item "Name Desc"
  # There is a third neutral phase to the toggle - not anymore
  #When click sort label by name
  Then the first labels are sorted as "Habr,Briza"
  When click the sort type dropdown
  When click sort by item "Name Asc"
  Then the first labels are sorted as "Briza,Habr"
  When clear the labels filter input
  Then the first labels are sorted as "Briza,Buk,Habr,Javor,Jilm"

@tested
Scenario Outline: Delete Label
  When hover over label card "<NAME>"
  When click delete for the label card "<NAME>"
  When click delete confirm for the label card "<NAME>"
  Then the labels "<NAME>" are not present

  Examples:
  | NAME |
  | Briza |
  | Buk |
  | Habr |
  | Javor |
  | Jilm |

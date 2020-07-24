Feature: Cloud Signin - Func
  Use and abuse the cloud signin page

@cloud-test
Scenario: login to cloud password
  When I open the cloud login
  When log in to the cloud
  Then the home page is loaded

@cloud-test
Scenario: logout influx menu
  When click nav menu item "User"
  When click user nav item "Logout"
  # Need second click - 1st should lead to account info
  # TODO update when account info page loads
  When click nav menu item "User"
  When click user nav item "Logout"
  Then the cloud login page is loaded

#Scenario: excersize login page

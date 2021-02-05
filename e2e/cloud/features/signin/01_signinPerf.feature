Feature: Cloud Signin - Perf
  Check basic loading times of cloud login

  @perf-test
  @cloud-test
  Scenario: Perf - redirect to login
    When open the cloud page in "3000" milliseconds

  @perf-test
  @cloud-test
  Scenario: Perf: login to home
    When I open the cloud login
    When log in to the cloud in "3000" milliseconds

  @perf-test
  @cloud-test
  Scenario: Perf - logout to account
  # N.B. on free account first logout should load account info
    When I logout to account info in "3000" milliseconds

  @perf-test
  @cloud-test
  Scenario: Perf - logout to login
    When I logout to login page in "3000" milliseconds

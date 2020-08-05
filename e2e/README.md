## Selenium-Accept

Selenium Acceptance tests for the Influxdbv2 UI.  These tests were initially intended to support nightly testing of the open source (OSS) poduct.  They are currently (7. 2020) being leveraged to support synthetic testing of the cloud deployments. This leveraging has lead to some substantial changes and improvements in configuration.  

**Run cycle**

*original OSS*

```bash
npm install
npm run influx:setup
npm test
node src/utils/htmlReport.js
node src/utils/junitReport.js
```

Note that the final two reporting steps can be bundled into `package.json` scripts, or can be called as a part of the `package.json` *test* script.  They are shown here to show how third party components are used to generate reports.

To clean the reports and screenshots directories: 

```bash
npm run clean
```

### Configuration 

Tests are configured in the file `e2e.conf.json`.  The configuration to be used for a test run is specified
by the property `active` in this file.  This can be overridden on the commandline. 

**Command Line Arguments**

The following command line arguments are detected at load time. 

   * `headless` -  boolean.  Whether to run the tests headless or not. 
   * `sel_docker` or `selDocker` - boolean. Added for influxdata docker tests.  Chooses options needed for running tests in the influxdata docker container. 
   * `active_conf` or `activeConf` - string. Declare the configuration to be activated for the test run.  Must match a config declared in `e2e.conf.json`.
   
For example to run the dataexplorer feature `headless` against the `nightly` build configuration: 

```bash
npm test headless=true activeConf=nightly -- features/dataExplorer/dataExplorer.feature
```  

Another example to test the signin feature using _tags_, `headless` and against the `nighty` configuration. 

```bash
npm test -- headless=true activeConf=nightly -t "@feature-signin"
```

**Environment Variable Overrides**

Configuration properties can be overridden by `E2E` environment variables. The basic pattern for exporting an enviornment variable to be picked up for test configuration is the token `E2E` followed by an underscore, then the path to the property to be modified as defined by the configuration tree in `e2e.conf.json`.  Each node in the tree is declared in uppercase and separated by an underscore. 

For example, to declare the `influx_url` property in the `development` configuration export the environment variable `E2E_DEVELOPMENT_INFLUX_URL`.

e.g. 

`export E2E_DEVELOPMENT_INFLUX_URL="http://172.17.0.2:9999"`

This feature was added specifically to define passwords and usernames only via the test environment. However, it can be used with any configuration key value such as the INFLUX_URL endpoint. 

e.g. 

```bash
export E2E_NIGHTLY_DEFAULT_USER_USERNAME=hibou
export E2E_NIGHTLY_DEFAULT_USER_PASSWORD=HuluHulu
```

**Note** - if password or token values in the configuration file are declared with the value _"ENV"_ then they must be defined by an environment variable as shown above.  Failure to define the values in the environment will lead to 400 bad request and similar errors, for example when creating users at the start of a test feature. 

**User Generation**

In the configration file the key `create_method` defines the method to be used to create the user and associated artifacts. Four values for this key are recognized. 

   * `REST` - use the rest endpoint to create the user directly.  Recommended.
   * `CLI` - use the command line client to create the user.  Experimental. **_CAUTION_** Deletes the file `${USER_HOME}/.influxdbv2/configs` and regenerates it.  Depends on additional special configuration keys: 
      * `influx_path` - path to the `influx` client executable, used to create the user.   
   * `CLI_DOCKER` - use the command line client inside the docker container. Experimental. Depends on additional special configuration keys: 
      * `docker_name` - name of the docker container in which influxdbv2 is running.  
   * `SKIP` - skip user creation.  It is assumed the user account will already exist in the deployment under test. 
   
Note that in the Containerized version described below, the `CLI` and `CLI_DOCKER` user generation modes do not currently (7.2020) work.    

### Containerized test runs against Selenoid

In June 2020 scripts were prepared to containerize the tests using a standard node version 12 container from docker and Selenoid browser containers.  This approach should make it easier to port the test infrastructure into other CI pipelines as well as to expand browser coverage.

The entire process is defined in the script `./scripts/containerTests.sh`.  

This script does the following: 

   1. Stops and tears down any existing docker container containing these tests.
   1. Stops and tears down any running Selenoid containers.
   1. Restarts Selenoid via the script `./scripts/selenoid.sh`.
   1. Rebuilds the docker image containing these tests.  Note this is currently (7.2020) based on the standard docker nodejs v 12.16 image and uses `./scripts/Dokcerfile.tests`.
   1. Executes the tests based on tag definitions passed to the script using the argument `-t` or `--tags` (defaults to "@influx-influx") and using the configuration passed through the optional argument `-c` or `--config` (defaults to "development"). 
   
Examples

```bash
$ scripts/containerTests.sh --tags "not @download-file"
$ scripts/containerTests.sh --tags "@feature-signin"
```

**Mapping directories between containers**

Note that this script maps the `etc` directory containing test data and upload files by linking it to the system `/tmp` directory and then mapping that volume into the docker containers.  This solution will not work when selenoid and test containers are not hosted on the same machine.

**Debugging Selenoid**

Debugging Selenoid tests entails starting the `selenoid-ui` container and then accessing it at `http://localhost:8080/#/`.  

```bash
scripts/selenoid.sh -debug
```

_Do not_ run the `containerTests.sh` script.  Instead start the test as it is started in that script.  

e.g.
```bash
sudo docker run -it -v `pwd`/report:/home/e2e/report -v `pwd`/screenshots:/home/e2e/screenshots \
     -v /tmp/e2e/etc:/home/e2e/etc -v /tmp/e2e/downloads:/home/e2e/downloads \
     -e SELENIUM_REMOTE_URL="http://${SELENOID_HOST}:4444/wd/hub" \
     -e E2E_${ACTIVE_CONF^^}_INFLUX_URL="http://${INFLUX2_HOST}:9999" --detach \
     --name ${TEST_CONTAINER} e2e-${TEST_CONTAINER}:latest
``` 

Then run tests against it. 

```bash
sudo docker exec ${TEST_CONTAINER} npm test -- activeConf=${ACTIVE_CONF} --tags "$TAGS"
```   

Test runs can then be monitored through the Selenoid UI.  Open the active session and log and VNC windows will open.  

**Video with Selenoid**

It is also possible to record an MP4 of a debug test run.  In the script `selenoid.sh` uncomment the following line: 

```bash
sudo docker pull selenoid/video-recorder:latest-release
``` 

And for now, in `cucumber.js` uncomment the line: 

```javascript
caps.set('enableVideo', true);
```

Then rerun the script `selenoid.sh` with the argument `-debug`. 

### Light Weight Perfomance checks

For tests against the cloud a light weigh perfomance utility has been added. It currently exports only one method for tests: `execTimed( func, maxDelay, failMsg, successMsg)`.  This method will execute the passed function and expect it to resolve by `maxDelay` milliseconds.  Failures are thrown to cucumber and results are stored in a performance log buffer.  This log is then dumped to the console after all tests have been run.  They are also written to a CSV report file: `./report/performance.csv`.

For example, here is how it is used to check the redirect to the login page. 

```javascript
    async openCloudPage(maxDelay){
        await perfUtils.execTimed(async () => {
                await this.driver.get(__config.influx_url);
                await this.loginPage.waitToLoad(10000);
            },
            maxDelay, `Redirect failed to resolve in ${maxDelay} ms`);
    }

```
 
### Tips

Run only the feature under development

```bash
npm test -- features/onboarding/onboarding.feature:4
```

Number is line number where the target scenario starts. To run the whole featre it can be omitted. 

Run the same headless

```bash
npm test headless=true -- features/onboarding/onboarding.feature:4
``` 

### API Notes

Steps classes should directly or indirectly inherit from the `baseSteps` class in `baseSteps.js`.  This class contains some generic methods to streamline Selenium and Cucumber interactions.  More will be added as the test suite grows.  Here is a list as of 1.10.2019.

`assertNotPresent(selector)`

`assertNotVisible(element)`

`assertVisible(element)`

`clearInputText(input)`

`clickAndWait(element,
        wait = async () => { await this.driver.sleep((await this.driver.manage().getTimeouts()).implicit/20); })`

`clickPopupWizardContinue()`

`clickPopupWizardPrevious()`

`clickPopupWizardFinish()`

`clickPopupCancelBtn()`

`closeAllNotifications()`

`containsNotificationText(text)`

`containsErrorNotification(text)`

`delay(timeout)`

`dismissPopup()`

`hoverOver(element)`

`typeTextAndWait(input, text,
        wait = async () => { await this.driver.sleep((await this.driver.manage().getTimeouts()).implicit/20); })`

`verifyElementContainsText(element, text)`

`verifyElementContainsClass(element, clazz)`

`verifyElementDoesNotContainClass(element, clazz)`

`verifyElementDisabled(element)`

`verifyElementErrorMessage(msg, equal = true)`

`verifyElementText(element, text)`

`verifyInputErrorIcon()`

`verifyInputEqualsValue(input, value)`

`verifyInputContainsValue(input, value)`

`verifyInputDoesNotContainValue(input, value)`

`verifyNoElementErrorMessage()`

`verifyNoFormInputErrorIcon()`

`verifyPopupAlertContainsText(text)`

`verifyPopupAlertMatchesRegex(regex)`               

`verifyPopupNotPresent()`

`verifyWizardContinueButtonDisabled()`

`verifyWizardDocsLinkURL(url)`

Pages should inherit directly or indirectly from the `basePage` class in `basePage.js`.  This contains many common selectors and element getters found either in the over all framework or in common components such as wizards and popups.  

The file `commonStepDefs.js` contains a library of test statements leveraging `baseSteps` and covering functionality that will often need to be repeated.  It also contains steps for accessing methods from `influxUtils.js` for accessing the REST api through AXIOS, which is useful for setting up test data for a feature set.

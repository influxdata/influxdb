## Selenium-Accept

Selenium Acceptance tests for the Influxdbv2 UI.

**Run cycle**

```bash
npm install
npm run influx:setup
npm test
node src/utils/htmlReport.js
node src/utils/junitReport.js
```

Note that the final two reporting steps can be bundled into `package.json` scripts, or can be called as a part of the `package.json` *test* script.  They are shown here to show how third party components are used to generate reports.


### Tips

Run only the feature under development

```bash
npm test -- features/onboarding/onboarding.feature:4
```

Number is line number where the target scenario starts.


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



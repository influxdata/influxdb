import { Then, When } from 'cucumber';

const clientLibsSteps = require(__srcdir + '/steps/loadData/clientLibsSteps.js');
let clibTabSteps = new clientLibsSteps(__wdriver);


Then(/^the Client Libraries tab is loaded$/, async () => {
    await clibTabSteps.verifyClientLibsTabIsLoaded();
});

When(/^click the "(.*)" client library tile$/, async name => {
    await clibTabSteps.clickLibTileByName(name);
});

Then(/^the csharp info popup is loaded$/, async () => {
    await clibTabSteps.verifyCSharpPopupLoaded();
});

Then(/^the go info popup is loaded$/, async () => {
    await clibTabSteps.verifyGoPopupLoaded();
});

Then(/^the java info popup is loaded$/, async () => {
    await clibTabSteps.verifyJavaPopupLoaded();
});

Then(/^the node info popup is loaded$/, async () => {
    await clibTabSteps.verifyNodePopupLoaded();
});

Then(/^the python info popup is loaded$/, async () => {
    await clibTabSteps.verifyPythonPopupLoaded();
});

Then(/^click copy "(.*)" to clipboard$/, {timeout: 10000}, async  label => {
    await clibTabSteps.clickCopyToClipboardText(label);
});

Then(/^verify clipboard contains text of "(.*)"$/,  async label => {
    await clibTabSteps.verifyClipboardTextFrom(label);
});

Then(/^verify the github repository link contains "(.*)"$/, async token => {
    await clibTabSteps.verifyPopupGithubLink(token);
});

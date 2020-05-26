import { Then } from 'cucumber';
const dataExplorerSteps = require(__srcdir + '/steps/dataExplorer/dataExplorerSteps.js');

let deSteps = new dataExplorerSteps(__wdriver);

Then(/^the Data Explorer page is loaded$/, {timeout: 2 * 5000}, async () => {
    await deSteps.isLoaded();
    await deSteps.verifyIsLoaded();
    //await deSteps.verifyHeaderContains('Data Explorer');
});

const settingsPage = require(__srcdir + '/pages/settings/settingsPage.js');

const tabHeader = '//*[@data-testid=\'flex-box\'][.//h5]';
const warningHeader = '//*[@data-testid=\'flex-box\']//p';
const renameButton = '[data-testid=button][title=\'Rename\']';

const urlCtx = 'about';

class orgProfileTab extends settingsPage{

    constructor(driver){
        super(driver);
    }

    async isTabLoaded(){
        await super.isTabLoaded(urlCtx,
            [
                {type: 'xpath', selector: tabHeader},
                {type: 'xpath', selector: warningHeader},
                {type: 'css', selector: renameButton},
            ]
        );
    }

}

module.exports = orgProfileTab;

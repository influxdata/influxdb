const settingsPage = require(__srcdir + '/pages/settings/settingsPage.js');

const variablesFilter = '[data-testid=search-widget]';
//const addMemberButton = '[data-testid=flex-box] [data-testid=button]';

const urlCtx = 'members';

class membersTab extends settingsPage{

    constructor(driver){
        super(driver);
    }

    async isTabLoaded(){
        await super.isTabLoaded(urlCtx,
            [
                {type: 'css', selector: variablesFilter},
                //              {type: 'css', selector: addMemberButton},
            ]
        );
    }

}

module.exports = membersTab;

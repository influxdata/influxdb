const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const orgProfileTab = require(__srcdir + '/pages/settings/orgProfileTab.js');

class orgProfileSteps extends baseSteps{

    constructor(driver){
        super(driver);
        this.opTab = new orgProfileTab(driver);
    }

    async isLoaded(){
        await this.opTab.isTabLoaded();
    }

}

module.exports = orgProfileSteps;

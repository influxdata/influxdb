const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const membersTab = require(__srcdir + '/pages/settings/membersTab.js');

class membersSteps extends baseSteps{

    constructor(driver){
        super(driver);
        this.memTab = new membersTab(driver);
    }

    async isLoaded(){
        await this.memTab.isTabLoaded();
    }

}

module.exports =  membersSteps;

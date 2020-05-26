const baseSteps = require(__srcdir + '/steps/baseSteps.js');
const createOrgPage = require(__srcdir + '/pages/createOrgPage.js');

class createOrgSteps extends baseSteps {

    constructor(driver){
        super(driver);
        this.createOrgPage = new createOrgPage(driver);
    }

    //for driver sync
    async isLoaded(){
        await this.createOrgPage.isLoaded();
    }

    //for assrtions
    async verifyIsLoaded(){
        this.assertVisible(await this.createOrgPage.getInputOrgName());
        this.assertVisible(await this.createOrgPage.getInputBucketName());
        this.assertVisible(await this.createOrgPage.getbuttonCancel());
        this.assertVisible(await this.createOrgPage.getbuttonCreate());
    }
}

module.exports = createOrgSteps;

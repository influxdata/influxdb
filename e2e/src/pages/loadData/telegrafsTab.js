const { By } = require('selenium-webdriver');
const loadDataPage = require(__srcdir + '/pages/loadData/loadDataPage.js');
const basePage = require(__srcdir + '/pages/basePage.js');

const telegrafsFilter = '[data-testid=search-widget]';
const createConfigInHeader = '//div[@data-testid=\'tabbed-page--header\']//*[contains(@title, \'Create\')]';
const nameSort = '[data-testid=name-sorter]';
const bucketSort = '[data-testid=bucket-sorter]';
const createConfigInBody = '[data-testid=resource-list] [data-testid=button]';
const telegrafCardTemplate = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]';
const telegrafCards = '[data-testid=resource-card]';
const telegrafCardSetupInstructions = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'setup-instructions-link\']';
const telegrafCardName = '//*[@data-testid=\'collector-card--name\'][span[text()=\'%NAME%\']]';
const telegrafCardNameEditBtn = '//*[./*[@data-testid=\'collector-card--name\'][span[text()=\'%NAME%\']]]//*[@data-testid=\'collector-card--name-button\']';
const telegrafCardNameInput = '[data-testid=\'collector-card--input\']'; //should only be one active
const telegrafCardDescr = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'resource-list--editable-description\']';
const telegrafCardDescrEditBtn = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'resource-list--editable-description\']//*[@data-testid=\'icon\']';
const telegrafCardDescrInput = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//div[@data-testid=\'resource-list--editable-description\']//input';
const telegrafCardDelete = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'telegraf-delete-menu\']';
const telegrafCardDeleteConfirm = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//button[@data-testid=\'telegraf-delete-button\']';
const telegrafCardAddLabelBtn = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'inline-labels--add\']';
const telegrafCardLabelPopup = '[data-testid=inline-labels--popover--dialog]';
const telegrafCardLabelPopupListItem = '[data-testid=\'inline-labels--popover--dialog\'] [data-testid=\'label-list--item %ITEM%\']';
const telegrafCardLabelPillItem = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'label--pill %ITEM%\']';
const telegrafCardLabelPopupFilter = '[data-testid=\'inline-labels--popover--dialog\'] [data-testid=\'inline-labels--popover-field\']';
const telegrafCardLabelEmptyState = '[data-testid=\'inline-labels--popover--dialog\'] [data-testid=\'empty-state--text\']';
const telegrafCardLabelPillDelete = '//*[@data-testid=\'resource-card\'][.//*[text()=\'%NAME%\']]//*[@data-testid=\'label--pill--delete %ITEM%\']';

const urlCtx = 'telegrafs';

// Telegraf wizard
const bucketDropdownBtn = '[data-testid=bucket-dropdown--button] ';
const pluginFilter = '[data-testid=input-field][placeholder*=\'Plugins\']';
const pluginTileTemplate = '[data-testid=telegraf-plugins--%TILE_NAME%]';

// Telegraf wizard step 2
const configurationNameInput = '[data-testid=input-field][title*=\'Configuration Name\']';
const configurationDescrInput = '[data-testid=input-field][title*=\'Configuration Descr\']';
const configurationPluginsSideBar = '//*[*[text()=\'Plugins\']]//div[contains(@class,\'side-bar--tabs\')]';

//Telegraf wizard edit plugin
const pluginDockerEditEndpoint = '//*[.//*[text()=\'endpoint\']][@data-testid=\'form--element\']//*[@data-testid=\'input-field\']';
const pluginK8SEditEndpoint = '//*[.//*[text()=\'url\']][@data-testid=\'form--element\']//*[@data-testid=\'input-field\']';
const pluginNGINXEditEndpoint = '//*[.//*[text()=\'urls\']][@data-testid=\'form--element\']//*[@data-testid=\'input-field\']';
const pluginNGINXAddUrlButton = '[data-testid=button][title=\'Add to list of urls\']';
const pluginNGINXDeleteFirstURL = '[data-testid=confirmation-button--button][title=\'Delete\']:nth-of-type(1)';
const pluginNGINXDeleteURLConfirmButton = '[data-testid=confirmation-button--confirm-button]';
const pluginNGINXURLListItems = '[data-testid=overlay--body] [data-testid=\'grid--column\'] [data-testid=index-list]';
const pluginRedisServersEditEndpoint = '//*[.//*[text()=\'servers\']][@data-testid=\'form--element\']//*[@data-testid=\'input-field\']';
const pluginRedisPasswordEditEndpoint = '//*[.//*[text()=\'password\']][@data-testid=\'form--element\']//*[@data-testid=\'input-field\']';

//Telegraf wizard step 3
const codeToken = '//pre[contains(text(), \'TOKEN\')]';
const codeCliTelegraf = '//code[contains(text(), \'telegraf\')]';
const copyToClipboardToken = '//*[@class=\'code-snippet\'][.//*[contains(text(),\'TOKEN\')]]//*[@data-testid=\'button-copy\']';
const copyToClipboardCommand = '//*[@class=\'code-snippet\'][.//*[contains(text(),\'telegraf\')]]//*[@data-testid=\'button-copy\']';

//Config Popup
const downloadConfigButton = '//*[@data-testid=\'button\'][span[text()=\'Download Config\']]';



class telegrafsTab extends loadDataPage{

    constructor(driver){
        super(driver);
    }

    async isTabLoaded(){
        await super.isTabLoaded(urlCtx,
            [
                {type: 'css', selector: telegrafsFilter},
                {type: 'xpath', selector: createConfigInHeader},
                //{type: 'css', selector: nameSort},
                basePage.getSortTypeButtonSelector()
                //{type: 'css', selector: bucketSort},
            ]
        );
    }

    async getTelegraphCardByName(name){
        return await this.driver.findElement(By.xpath(`//*[@data-testid='resource-card'][//span[text()='${name}']]`));
    }

    async getTelegrafsFilter(){
        return await this.driver.findElement(By.css(telegrafsFilter));
    }

    async getCreateConfigInHeader(){
        return await this.driver.findElement(By.xpath(createConfigInHeader));
    }

    async getNameSort(){
        return await this.driver.findElement(By.css(nameSort));
    }

    async getBucketSort(){
        return await this.driver.findElement(By.css(bucketSort));
    }

    async getCreateConfigInBody(){
        return await this.driver.findElement(By.css(createConfigInBody));
    }

    // Telegraf Wizard

    async getBucketDropdownBtn(){
        return await this.driver.findElement(By.css(bucketDropdownBtn));
    }

    async getPluginFilter(){
        return await this.driver.findElement(By.css(pluginFilter));
    }

    async getPluginTileByName(name){
        return await this.driver.findElement(By.css(pluginTileTemplate.replace('%TILE_NAME%', name)));
    }

    static getPluginTitleSelectorByName(name){
        return { type: 'css', selector: pluginTileTemplate.replace('%TILE_NAME%', name)};
    }

    // Telegraf Wizard step 2
    async getPluginItemByName(name){
        return await this.driver.findElement(By.xpath(`//*[contains(@class, 'side-bar--tab')]/div[.//*[text() = '${name.toLowerCase()}']]`));
    }

    async getConfigurationNameInput(){
        return await this.driver.findElement(By.css(configurationNameInput));
    }

    static getConfigurationNameInputSelector(){
        return { type: 'css', selector: configurationNameInput};
    }

    async getConfigurationDescrInput(){
        return await this.driver.findElement(By.css(configurationDescrInput));
    }

    static getConfigurationDescrInputSelector(){
        return { type: 'css', selector: configurationDescrInput};
    }

    async getConfigurationPluginsSideBar(){
        return await this.driver.findElement(By.xpath(configurationPluginsSideBar));
    }

    static configurationPluginsSideBarSelector(){
        return { type: 'css', selector: configurationPluginsSideBar};
    }

    //Telegraf wizard edit plugin
    async getPluginDockerEditEndpoint(){
        return await this.driver.findElement(By.xpath(pluginDockerEditEndpoint));
    }

    async getPluginK8SEditEndpoint(){
        return await this.driver.findElement(By.xpath(pluginK8SEditEndpoint));
    }

    async getPluginNGINXEditEndpoint(){
        return await this.driver.findElement(By.xpath(pluginNGINXEditEndpoint));
    }

    async getPluginNGINXAddUrlButton(){
        return await this.driver.findElement(By.css(pluginNGINXAddUrlButton));
    }

    async getPluginNGINXDeleteFirstURL(){
        return await this.driver.findElement(By.css(pluginNGINXDeleteFirstURL));
    }

    async getPluginNGINXDeleteURLConfirmButton(){
        return await this.driver.findElement(By.css(pluginNGINXDeleteURLConfirmButton));
    }

    async getPluginNGINXURLListItems(){
        return await this.driver.findElements(By.css(pluginNGINXURLListItems));
    }

    static getPluginNGINXURLListItemsSelector(){
        return { type: 'css', selector: pluginNGINXURLListItems };
    }

    async getPluginRedisServersEditEndpoint(){
        return await this.driver.findElement(By.xpath(pluginRedisServersEditEndpoint));
    }

    async getPluginRedisPasswordEditEndpoint(){
        return await this.driver.findElement(By.xpath(pluginRedisPasswordEditEndpoint));
    }

    async getCodeToken(){
        return await this.driver.findElement(By.xpath(codeToken));
    }

    async getCodeCliTelegraf(){
        return await this.driver.findElement(By.xpath(codeCliTelegraf));
    }

    // Config popup

    async getDownloadConfigButton(){
        return await this.driver.findElement(By.xpath(downloadConfigButton));
    }

    // Telegraf Card List

    async getTelegrafCardByName(name){
        return await this.driver.findElement(By.xpath(telegrafCardTemplate.replace('%NAME%', name)));
    }

    static getTelegrafCardSelectorByName(name){
        return { type: 'xpath', selector: telegrafCardTemplate.replace('%NAME%', name) };
    }

    async getTelegrafCards(){
        return await this.driver.findElements(By.css(telegrafCards));
    }

    async getCopyToClipboardToken(){
        return await this.driver.findElement(By.xpath(copyToClipboardToken));
    }

    async getCopyToClipboardCommand(){
        return await this.driver.findElement(By.xpath(copyToClipboardCommand));
    }

    async getTelegrafCardSetupInstructions(card){
        return await this.driver.findElement(By.xpath(telegrafCardSetupInstructions.replace('%NAME%', card)));
    }

    async getTelegrafCardName(name){
        return await this.driver.findElement(By.xpath(telegrafCardName.replace('%NAME%', name)));
    }

    async getTelegrafCardNameEditBtn(name){
        return await this.driver.findElement(By.xpath(telegrafCardNameEditBtn.replace('%NAME%', name)));
    }

    async getTelegrafCardNameInput(name){
        return await this.driver.findElement(By.css(telegrafCardNameInput.replace('%NAME%', name)));
    }

    async getTelegrafCardDescr(name){
        return await this.driver.findElement(By.xpath(telegrafCardDescr.replace('%NAME%', name)));
    }

    async getTelegrafCardDescrEditBtn(name){
        return await this.driver.findElement(By.xpath(telegrafCardDescrEditBtn.replace('%NAME%', name)));
    }

    async getTelegrafCardDescrInput(name){
        return await this.driver.findElement(By.xpath(telegrafCardDescrInput.replace('%NAME%', name)));
    }

    async getTelegrafCardDelete(name){
        return await this.driver.findElement(By.xpath(telegrafCardDelete.replace('%NAME%', name)));
    }

    async getTelegrafCardDeleteConfirm(name){
        return await this.driver.findElement(By.xpath(telegrafCardDeleteConfirm.replace('%NAME%', name)));
    }

    async getTelegrafCardAddLabelBtn(name){
        return await this.driver.findElement(By.xpath(telegrafCardAddLabelBtn.replace('%NAME%', name)));
    }

    async getTelegrafCardLabelPopup(name){
        return await this.driver.findElement(By.css(telegrafCardLabelPopup.replace('%NAME%', name)));
    }

    static getTelegrafCardLabelPopupSelector(name){
        return { type: 'css', selector: telegrafCardLabelPopup.replace('%NAME%', name)};
    }

    async getTelegrafCardLabelPopupListItem(name, item){

        return await this.driver.findElement(By.css(telegrafCardLabelPopupListItem
            .replace('%ITEM%', item)));

    }

    static getTelegrafCardLabelPopupListItemSelector(name, item){
        return { type: 'css', selector: telegrafCardLabelPopupListItem
            .replace('%ITEM%', item)};
    }

    async getTelegrafCardLabelPillItem(name, item){
        return await this.driver.findElement(By.xpath(telegrafCardLabelPillItem
            .replace('%NAME%', name)
            .replace('%ITEM%', item)));
    }

    static getTelegrafCardLabelPillItemSelector(name, item){
        return { type: 'xpath', selector: telegrafCardLabelPillItem
            .replace('%NAME%', name)
            .replace('%ITEM%', item)};
    }

    async getTelegrafCardLabelPopupFilter(name){
        return await this.driver.findElement(By.css(telegrafCardLabelPopupFilter.replace('%NAME%', name)));
    }

    async getTelegrafCardLabelEmptyState(name){
        return await this.driver.findElement(By.css(telegrafCardLabelEmptyState.replace('%NAME%', name)));
    }

    async getTelegrafCardLabelPillDelete(name, item){
        return await this.driver.findElement(By.xpath(telegrafCardLabelPillDelete
            .replace('%NAME%', name)
            .replace('%ITEM%', item)));
    }

}

module.exports = telegrafsTab;

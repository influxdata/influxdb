const loadDataPage = require(__srcdir + '/pages/loadData/loadDataPage.js');
const { By } = require('selenium-webdriver');

const bucketCards = '[data-testid^=\'bucket--card \']';
const createBucketBtn = 'button[data-testid=\'Create Bucket\']';
const filterInput = '[data-testid=search-widget]';
const nameSorter = '[data-testid=\'name-sorter\']';
const policySorter = '[data-testid=\'retention-sorter\']';
const bucketCardByName = '[data-testid=\'bucket--card--name %NAME%\']';
const bucketCardSettingsByName = `[data-testid='bucket-card %NAME%'] [data-testid=bucket-settings]`

// Create Bucket Popup
const popupContainer = '[data-testid=overlay--container]';
const popupTitle = '[data-testid=overlay--header] div';
const popupInputName = '[data-testid=bucket-form-name]';
const popupRetentionNever = '[data-testid=retention-never--button]';
const popupRetentionIntervals = '[data-testid=retention-intervals--button]';
const popupCancelButton = '[data-testid=overlay--body] button[title=Cancel]';
const popupDismissButton = '[data-testid=overlay--header] button[class*=dismiss]';
const popupCreateButton = '[data-testid=overlay--body] button[title*=Create]';
const popupRPIntervalControls = '[data-testid=form--element] [data-testid=grid--row]';
const popupRPDurationSelectorButton = '[data-testid=duration-selector--button]';
const popupRPDaysInput = popupRPIntervalControls + ' [data-testid=grid--column]:nth-of-type(1) input';
const popupRPHoursInput = popupRPIntervalControls + ' [data-testid=grid--column]:nth-of-type(2) input';
const popupRPMinutesInput = popupRPIntervalControls + ' [data-testid=grid--column]:nth-of-type(3) input';
const popupRPSecondsInput = popupRPIntervalControls + ' [data-testid=grid--column]:nth-of-type(4) input';
const popupFormError = '[data-testid=form--element-error]';

//Edit Bucket Popup
const popupSaveChanges = '[data-testid=bucket-form-submit]';
const popupHelpText = '[data-testid=form--help-text]';

//Add data line protocol Popup Wizard
//reuse popup title above
//reuse dismiss above
const wizardStepTitle = '.cf-overlay--title';
const wizardStepSubTitle = '[data-testid=form-container] [class*=sub-title]';
const wizardRadioUploadFile = '[data-testid=\'Upload File\']';
const wizardRadioManual = '[data-testid=\'Enter Manually\']';
const wizardPrecisionDropdown = '[data-testid=\'wizard-step--lp-precision--dropdown\']';
const wizardDragAndDrop = 'div.drag-and-drop';
const wizardContinueButton = '[data-testid=next]';
const wizardWriteDataButton = '[data-testid*=\'write-data--button\']';
const wizardTextArea = '[data-testid=\'line-protocol--text-area\']';
const wizardFinishButton = '[data-testid=next][title=\'Finish\']';
const wizardCloseButton = '[data-testid=lp-close--button]';
const wizardCancelButton = '[data-testid^=lp-cancel--button]';
const wizardDismissButton = '.cf-overlay--dismiss';
const wizardStepStateText = '[data-testid=\'line-protocol--status\']';
const wizardSparkleSpinner = '[data-testid=sparkle-spinner]';
const dataWizardPreviousButton = '[data-testid=overlay--footer] [data-testid=back]';

const popoverItem = '//*[@data-testid=\'popover--contents\']//*[text() = \'%ITEM%\']';


const urlCtx = 'buckets';

class bucketsTab extends loadDataPage {

    constructor(driver){
        super(driver);
    }

    async isTabLoaded(){
        await super.isTabLoaded(urlCtx);
    }

    async getBucketCards(){
        return await this.driver.findElements(By.css(bucketCards));
    }

    async getCreateBucketBtn(){
        return await this.driver.findElement(By.css(createBucketBtn));
    }

    async getFilterInput(){
        return await this.driver.findElement(By.css(filterInput));
    }

    async getNameSorter(){
        return await this.driver.findElement(By.css(nameSorter));
    }

    static async getNameSorterSelector(){
        return {type: 'css', selector: nameSorter};
    }

    async getPolicySorter(){
        return await this.driver.findElement(By.css(policySorter));
    }

    //Create Bucket Popup
    async getPopupContainer(){
        return await this.driver.findElement(By.css(popupContainer));
    }

    static getPopupContainerSelector(){
        return { type: 'css',  selector: popupContainer };
    }

    async getPopupTitle(){
        return await this.driver.findElement(By.css(popupTitle));
    }

    static getPopupTitleSelector(){
        return { type: 'css', selector: popupTitle};
    }

    async getPopupInputName(){
        return await this.driver.findElement(By.css(popupInputName));
    }

    async getPopupRetentionNever(){
        return await this.driver.findElement(By.css(popupRetentionNever));
    }

    async getPopupRetentionIntervals(){
        return await this.driver.findElement(By.css(popupRetentionIntervals));
    }

    async getPopupCancelButton(){
        return await this.driver.findElement(By.css(popupCancelButton));
    }

    async getPopupRPIntevalControls(){
        return await this.driver.findElement(By.css(popupRPIntervalControls));
    }

    static getPopupRPIntervalControlsSelector(){
        return { type: 'css',  selector: popupRPIntervalControls };
    }

    async getPopupRPDurationSelectorButton(){
        return await this.driver.findElement(By.css(popupRPDurationSelectorButton));
    }

    static getPopupRPDurationSelectorButtonSelector(){
        return {type: 'css', selector: popupRPDurationSelectorButton};
    }

    async getPopupRPDaysInput(){
        return await this.driver.findElement(By.css(popupRPDaysInput));
    }

    async getPopupRPHoursInput(){
        return await this.driver.findElement(By.css(popupRPHoursInput));
    }

    async getPopupRPMinutesInput(){
        return await this.driver.findElement(By.css(popupRPMinutesInput));
    }

    async getPopupRPSecondsInput(){
        return await this.driver.findElement(By.css(popupRPSecondsInput));
    }

    async getPopupFormError(){
        return await this.driver.findElement(By.css(popupFormError));
    }

    static getPopupFormErrorSelector(){
        return { type: 'css',  selector: popupFormError };
    }

    async getPopupDismissButton(){
        //return await this.driver.findElement(By.css(popupDismissButton));
        return await this.smartGetElement({type: 'css', selector: popupDismissButton});
    }

    static async getPopupDismissButtonSelector(){
        return popupDismissButton;
    }

    async getPopupRPDurationSelectorItem(duration){
        return await this.driver.findElement(By.css(`[data-testid=duration-selector--${duration}]`));
    }

    async getPopupHelpText(){
        return await this.driver.findElement(By.css(popupHelpText));
    }

    async getPopupCreateButton(){
        return await this.driver.findElement(By.css(popupCreateButton));
    }

    //get just the name link
    async getBucketCardName(name){
        return await this.driver.findElement(By.css(`[data-testid='bucket--card--name ${name}']`));
    }

    //get the whole card
    async getBucketCardByName(name){
        return await this.driver.findElement(By.css(bucketCardByName.replace('%NAME%', name)));
        //return await this.driver.findElement(By.xpath(`//div[div/div[@data-testid='bucket--card ${name}']]`));
    }

    static async getBucketCardDeleteSelectorByName(name){
        return {type: 'xpath', selector: `//div[div/div/div[@data-testid='bucket--card ${name}'] ]//*[@data-testid='context-delete-menu']`};
    }

    async getBucketCardDeleteByName(name){
        //   return await this.driver.findElement(By.xpath(`//div[div/div/div[@data-testid='bucket--card ${name}'] ]//*[@data-testid='context-delete-menu']`));
        return await this.driver.findElement(By.css(`[data-testid='context-delete-menu ${name}']`));
    }

    async getBucketCardRetentionByName(name){
        //        return await this.driver.findElement(By.xpath(`//div[div/div[@data-testid='bucket--card ${name}']]//div[contains(text(), 'Retention')]`));
        //return await this.driver.findElement(By.xpath(`//*[@data-testid='bucket--card ${name}']//*[@data-testid='cf-resource-card--meta-item'][contains(text(),"Retention")]`));
        //return await this.driver.findElement(By.xpath(`//*[@data-testid='bucket-card'][.//*[@data-testid='bucket--card--name ${name}']]//*[@data-testid='cf-resource-card--meta-item'][contains(text(), 'Retention')]`));
        return await this.driver.findElement(By.xpath(`//*[@data-testid='bucket-card ${name}']//*[@data-testid='resource-list--meta']//*[contains(text(), 'Retention')]`));
    }

    async getBucketCardPopover(){
        //return await this.driver.findElement(By.xpath(`//div[div/div[@data-testid='bucket--card ${name}']]//div[@data-testid='popover--contents']`));
        return await this.driver.findElement(By.css('[data-testid=popover--contents]'));
    }

    static async getPopoverSelector(){
        return { type: 'css', selector: '[data-testid=popover--contents]'};
    }

    async getBucketCardPopoverItemByName(name, item){
        return await this.driver.findElement(By.xpath(`//div[div/div[@data-testid='bucket--card ${name}']]//div[@data-testid='popover--contents']//div[contains(text(), '${item}')]`));
    }

    static async getBucketCardSelectorByName(name){
        return {type: 'css', selector: `[data-testid='bucket--card ${name}']`};
    }

    async getBucketCardDeleteConfirmByName(name){
        //return await this.smartGetElement({type: 'xpath', selector: `//div[div/div/div[@data-testid='bucket--card ${name}'] ]//*[@data-testid='context-delete-menu']/..//button[text() = 'Confirm']`});
        //return await this.driver.findElement(By.xpath(`//div[div/div/div[@data-testid='bucket--card ${name}'] ]//*[@data-testid='context-delete-menu']/..//button[text() = 'Confirm']`));
        return await this.driver.findElement(By.css(`[data-testid='context-delete-bucket ${name}'] `));
    }

    async getBucketCardAddDataByName(name){

        //return await this.driver.findElement(By.xpath(`//*[@data-testid='bucket-card'][.//*[@data-testid='bucket--card--name ${name}']]//button[@title='Add Data']`));
        return await this.driver.findElement(By.xpath(`//*[@data-testid='bucket-card ${name}']//button[@title='Add Data']`))
        //return await this.smartGetElement({type: 'xpath', selector: `//div[div/div/div[@data-testid='bucket--card ${name}']]//button[@title = 'Add Data']`});
    }

    async getPopupSaveChanges(){
        return await this.driver.findElement(By.css(popupSaveChanges));
    }

    async getWizardStepTitle(){
        return await this.driver.findElement(By.css(wizardStepTitle));
    }

    static async getWizardStepTitleSelector(){
        return {type: 'css', selector: wizardStepTitle };
    }

    async getWizardStepSubTitle(){
        return await this.driver.findElement(By.css(wizardStepSubTitle));
    }

    static async getWizardStepSubTitleSelector(){
        return {type: 'css', selector: wizardStepSubTitle };
    }


    async getWizardRadioUploadFile(){
        return await this.driver.findElement(By.css(wizardRadioUploadFile));
    }

    async getWizardRadioManual(){
        return await this.driver.findElement(By.css(wizardRadioManual));
    }

    async getWizardPrecisionDropdown(){
        return await this.driver.findElement(By.css(wizardPrecisionDropdown));
    }

    async getWizardDragAndDrop(){
        return await this.driver.findElement(By.css(wizardDragAndDrop));
    }

    async getWizardContinueButton(){
        return await this.driver.findElement(By.css(wizardContinueButton));
    }

    static async getWizardContinueButtonSelector(){
        return {type: 'css', selector: wizardContinueButton };
    }

    async getWizardWriteDataButton(){
        return await this.driver.findElement(By.css(wizardWriteDataButton));
    }


    async getWizardTextArea(){
        return await this.driver.findElement(By.css(wizardTextArea));
    }

    async getWizardDropdownPrecisionItem(prec){
        return await this.driver.findElement(By.css(`[data-testid='wizard-step--lp-precision-${prec}']`));
    }

    async getWizardFinishButton(){
        return await this.driver.findElement(By.css(wizardFinishButton));
    }

    async getWizardCloseButton(){
        return await this.driver.findElement(By.css(wizardCloseButton));
    }

    async getWizardCancelButton(){
        return await this.driver.findElement(By.css(wizardCancelButton));
    }

    async getWizardDismissButton(){
        return await this.driver.findElement(By.css(wizardDismissButton));
    }

    async getWizardStepStateText(){
        return await this.driver.findElement(By.css(wizardStepStateText));
    }

    async getWizardSparkleSpinner(){
        return await this.driver.findElement(By.css(wizardSparkleSpinner));
    }

    async getPopoverItem(item){
        return await this.driver.findElement(By.xpath(popoverItem.replace('%ITEM%', item)));
    }

    async getDataWizardPreviousButton(){
        return await this.driver.findElement(By.css(dataWizardPreviousButton));
    }

    async getBucketCardSettingsByName(name){
        return await this.driver.findElement(By.css(bucketCardSettingsByName.replace('%NAME%', name)));
    }

}

module.exports = bucketsTab;

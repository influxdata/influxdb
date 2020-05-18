const chrome = require('selenium-webdriver/chrome');
const ffox = require('selenium-webdriver/firefox');
const fs = require('fs');
const {Builder, Capabilities, By, Key, logging, PageLoadStrategy, promise, until} = require('selenium-webdriver');
//following provides cleaner paths in require statements
global.__basedir = __dirname;
global.__srcdir = __dirname + "/src";
global.__runtime = new Date();
global.__runtimeStr = __runtime.getFullYear().toString() +
    (__runtime.getMonth() + 1).toString().padStart(2, '0') +
    __runtime.getDate().toString().padStart(2, '0') + "-" +
    __runtime.getHours().toString().padStart(2, '0') +
    __runtime.getMinutes().toString().padStart(2, '0') +
    __runtime.getSeconds().toString().padStart(2, '0');


const { flush, config, defaultUser } = require(__srcdir + '/utils/influxUtils');

global.__screenShotDir = process.cwd() + "/" + __config.screenshot_dir + "/" + __runtimeStr;
global.__dataBuffer = {};

fs.mkdirSync(__screenShotDir,  { recursive: true });

var common = '--require "src/step_definitions/**/*.js" --require hooks.js --require-module babel-core/register ';

let caps = new Capabilities();

let chromeUserPreferences = { 'download.prompt_for_download': false, "download.default_directory": __basedir };
let windowSize = { "width": 1024, "height": 768 };

if(__config.window_size){
    windowSize.width = parseInt(__config.window_size.width);
    windowSize.height = parseInt(__config.window_size.height);
}

console.log("DEBUG windowSize " + JSON.stringify(windowSize));

let logPrefs =  new logging.Preferences();
logPrefs.setLevel(logging.Type.BROWSER, logging.Level.ALL);
logPrefs.setLevel(logging.Type.DRIVER, logging.Level.INFO);

let chromeArguments = ['--no-sandbox'];

if(__config.sel_docker){
    chromeArguments.push('--disable-dev-shm-usage')
}

if(__config.headless) {
    caps.set('applicationCacheEnabled', false);
    caps.set('pageLoadStrategy', 'none');

    switch (__config.browser.toLowerCase()) {
        case "chrome":
        global.__wdriver = new Builder()
            .withCapabilities(caps)
            .forBrowser(__config.browser)
            .setChromeOptions(new chrome.Options().headless()
            .addArguments(chromeArguments)
                .setUserPreferences(chromeUserPreferences)
                .setLoggingPrefs(logPrefs)
                .windowSize({width: windowSize.width, height: windowSize.height}))
            .build();
            break;
        case "firefox":
            global.__wdriver = new Builder()
                .forBrowser(__config.browser)
                .setFirefoxOptions(new ffox.Options().headless().windowSize({width: windowSize.width,
                    height: windowSize.height}))
                .build();
            break;

    }
}else{
    switch (__config.browser.toLowerCase()) {
        case "chrome":
            global.__wdriver = new Builder()
                .withCapabilities(caps)
                .forBrowser(__config.browser)
                .setChromeOptions(new chrome.Options().addArguments("--incognito")
                    .addArguments(chromeArguments)
                    .setUserPreferences(chromeUserPreferences)
                    .setLoggingPrefs(logPrefs)
                    .windowSize({width: windowSize.width, height: windowSize.height}))
                .build();
            break;
        case "firefox":
            global.__wdriver = new Builder()
                .withCapabilities(caps)
                .forBrowser(__config.browser)
                .build();
            break;
    }
}

__wdriver.manage().setTimeouts({implicit: 3000});
__wdriver.executor_.w3c = true;
console.log("DEBUG __wdriver: " + JSON.stringify(__wdriver));

module.exports = {
    'default': common + '--format summary --format node_modules/cucumber-pretty --format json:report/cucumber_report.json',
    dry: common + '--dry-run',
    progress: common + '--format progress'
};

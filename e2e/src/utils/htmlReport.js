#!/usr/bin/env node
var reporter = require('cucumber-html-reporter');

var options = {
    theme: 'bootstrap',
    jsonFile: 'report/cucumber_report.json',
    output: 'report/cucumber_report.html',
    reportSuiteAsScenarios: true,
    launchReport: false,
    metadata: {
        'App Version':'2.0.0-alpha',
        'Test Environment': 'Deveolop',
        'Browser': 'Chrome  74',
        'Platform': 'Ubuntu 16.04',
        'Parallel': 'Scenarios',
        'Executed': 'local'
    }
};

reporter.generate(options);

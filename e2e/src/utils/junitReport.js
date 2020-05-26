#!/usr/bin/env node
const cucumberJunitConvert = require('cucumber-junit-convert');

const options = {
    inputJsonFile: 'report/cucumber_report.json',
    outputXmlFile: 'report/cucumber_junit.xml'
};

cucumberJunitConvert.convert(options);

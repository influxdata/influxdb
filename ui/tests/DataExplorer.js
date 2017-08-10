const src = process.argv.find(s => s.includes('--src=')).replace('--src=', '')
const dataExplorerUrl = `http://localhost:8888/sources/${src}/chronograf/data-explorer`
const dataTest = s => `[data-test="${s}"]`

module.exports = {
  'Data Explorer (functional) - SHOW DATABASES'(browser) {
    browser
      // Navigate to the Data Explorer
      .url(dataExplorerUrl)
      // Open a new query tab
      .waitForElementVisible(dataTest('add-query-button'), 1000)
      .click(dataTest('add-query-button'))
      .waitForElementVisible(dataTest('query-editor-field'), 1000)
      // Drop any existing testing database
      .setValue(dataTest('query-editor-field'), 'DROP DATABASE "testing"\n')
      .click(dataTest('query-editor-field'))
      .pause(500)
      // Create a new testing database
      .clearValue(dataTest('query-editor-field'))
      .setValue(dataTest('query-editor-field'), 'CREATE DATABASE "testing"\n')
      .click(dataTest('query-editor-field'))
      .pause(2000)
      .refresh()
      .waitForElementVisible(dataTest('query-editor-field'), 1000)
      .clearValue(dataTest('query-editor-field'))
      .setValue(dataTest('query-editor-field'), 'SHOW DATABASES\n')
      .click(dataTest('query-editor-field'))
      .pause(1000)
      .waitForElementVisible(
        dataTest('query-builder-list-item-database-testing'),
        5000
      )
      .assert.containsText(
        dataTest('query-builder-list-item-database-testing'),
        'testing'
      )
      .end()
  },
  'Query Builder'(browser) {
    browser
      // Navigate to the Data Explorer
      .url(dataExplorerUrl)
      // Check to see that there are no results displayed
      .waitForElementVisible(dataTest('data-explorer-no-results'), 5000)
      .assert.containsText(dataTest('data-explorer-no-results'), 'No Results')
      // Open a new query tab
      .waitForElementVisible(dataTest('new-query-button'), 1000)
      .click(dataTest('new-query-button'))
      // Select the testing database
      .waitForElementVisible(
        dataTest('query-builder-list-item-database-testing'),
        1000
      )
      .click(dataTest('query-builder-list-item-database-testing'))
      // Open up the Write Data dialog
      .click(dataTest('write-data-button'))
      // Set the dialog to manual entry mode
      .waitForElementVisible(dataTest('manual-entry-button'), 1000)
      .click(dataTest('manual-entry-button'))
      // Enter some time-series data
      .setValue(
        dataTest('manual-entry-field'),
        'testing,test_measurement=1,test_measurement2=2 value=3,value2=4'
      )
      // Pause, then click the submit button
      .pause(500)
      .click(dataTest('write-data-submit-button'))
      .pause(2000)
      // Start building a query
      // Select the testing measurement
      .waitForElementVisible(
        dataTest('query-builder-list-item-measurement-testing'),
        2000
      )
      .click(dataTest('query-builder-list-item-measurement-testing'))
      // Select both test measurements
      .waitForElementVisible(
        dataTest('query-builder-list-item-tag-test_measurement'),
        1000
      )
      .click(dataTest('query-builder-list-item-tag-test_measurement'))
      .click(dataTest('query-builder-list-item-tag-test_measurement2'))
      .pause(500)
      // Select both tag values
      .waitForElementVisible(
        dataTest('query-builder-list-item-tag-value-1'),
        1000
      )
      .click(dataTest('query-builder-list-item-tag-value-1'))
      .click(dataTest('query-builder-list-item-tag-value-2'))
      .pause(500)
      // Select both field values
      .waitForElementVisible(
        dataTest('query-builder-list-item-field-value'),
        1000
      )
      .click(dataTest('query-builder-list-item-field-value'))
      .click(dataTest('query-builder-list-item-field-value2'))
      .pause(500)
      // Assert the built query string
      .assert.containsText(
        dataTest('query-editor-field'),
        'SELECT mean("value") AS "mean_value", mean("value2") AS "mean_value2" FROM "testing"."autogen"."testing" WHERE time > now() - 1h AND "test_measurement"=\'1\' AND "test_measurement2"=\'2\' GROUP BY time(10s)'
      )
      .click(dataTest('data-table'))
      .click(dataTest('query-builder-list-item-function-value'))
      .waitForElementVisible(dataTest('function-selector-item-mean'), 1000)
      .click(dataTest('function-selector-item-mean'))
      .click(dataTest('function-selector-apply'))
      .end()
  },
}

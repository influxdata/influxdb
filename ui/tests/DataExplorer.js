const src = process.argv.find(s => s.includes('--src=')).replace('--src=', '')
const url = `http://localhost:8888/sources/${src}/chronograf/data-explorer`

module.exports = {
  'Data Explorer (functional) - SHOW DATABASES'(browser) {
    browser
      .url(url)
      .waitForElementVisible('[data-test="add-query-button"]', 1000)
      .click('[data-test="add-query-button"]')
      .pause(500)
      .waitForElementVisible('[data-test="query-editor-field"]', 1000)
      .setValue('[data-test="query-editor-field"]', 'SHOW DATABASES\n')
      .pause(1000)
      .waitForElementVisible('[data-test="data-table"]', 5000)
      .assert.containsText('[data-test="data-table"]', '_internal')
      .end()
  },
}

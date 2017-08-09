const src = process.argv.find(s => s.includes('--src=')).replace('--src=', '')
const url = `http://localhost:8888/sources/${src}/chronograf/data-explorer`

module.exports = {
  'Data Explorer (functional) - SHOW DATABASES'(browser) {
    browser
      .url(url)
      .useXpath()
      .waitForElementVisible(
        '//div[@class="btn btn-primary"][contains(.,"Add a Query")]',
        1000
      )
      .click('//div[@class="btn btn-primary"][contains(.,"Add a Query")]')
      .pause(500)
      .waitForElementVisible(
        '//textarea[contains(@class,"query-editor--field")]',
        1000
      )
      .setValue(
        '//textarea[contains(@class,"query-editor--field")]',
        'SHOW DATABASES\n'
      )
      .pause(1000)
      .waitForElementVisible(
        '*//div[@class="fixedDataTableCellLayout_main public_fixedDataTableCell_main"]/span[contains(.,"_internal")]',
        5000
      )
      .assert.containsText(
        '*//div[@class="fixedDataTableCellLayout_main public_fixedDataTableCell_main"]/span[contains(.,"_internal")]',
        '_internal'
      )
      .end()
  },
}

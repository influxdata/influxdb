describe('Scrapers', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/scrapers_tab`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a scraper', () => {
      const newScraper = 'Scraper'
      const newURL = 'http://google.com'

      cy.getByTestID('table-row').should('have.length', 0)

      cy.contains('Create').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('name')
          .clear()
          .type(newScraper)
        cy.getByInputName('url')
          .clear()
          .type(newURL)
        cy.get('.button')
          .contains('Finish')
          .click()
      })

      cy.getByTestID('table-row')
        .should('have.length', 1)
        .and('contain', newScraper)
    })
  })
})

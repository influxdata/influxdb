import {Organization, Bucket} from '@influxdata/influx'

describe('Scrapers', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
        bucket,
      } = body
      cy.wrap(body.org).as('org')
      cy.wrap(bucket).as('bucket')

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

    it('can update scrapers name', () => {
      const newScraperName = 'This is new name'

      const scraperName = 'New Scraper'
      const url = 'http://google.com'
      const type = 'Prometheus'

      cy.get<Organization>('@org').then(({id}) => {
        let orgID = id
        cy.get<Bucket>('@bucket').then(({id}) => {
          cy.createScraper(scraperName, url, type, orgID, id)
        })
      })

      cy.getByTestID('table-cell').within(() => {
        cy.getByTestID('editable-name').click()
        cy.getByTestID('input-field').type(`${newScraperName}{enter}`)
      })
    })

    it('can delete a scraper', () => {
      const scraperName = 'New Scraper'
      const url = 'http://google.com'
      const type = 'Prometheus'

      cy.get<Organization>('@org').then(({id}) => {
        let orgID = id
        cy.get<Bucket>('@bucket').then(({id}) => {
          cy.createScraper(scraperName, url, type, orgID, id)
          cy.createScraper(scraperName, url, type, orgID, id)
        })
      })

      cy.getByTestID('table-row').should('have.length', 2)

      cy.getByTestID('confirmation-button')
        .last()
        .click({force: true})

      cy.getByTestID('table-row').should('have.length', 1)
    })
  })
})

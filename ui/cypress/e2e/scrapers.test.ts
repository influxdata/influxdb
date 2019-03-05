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
        cy.visit(`${orgs}/${id}/scrapers`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a scraper from the create button in the page header', () => {
      const newScraper = '🐍craper'
      const newURL = 'http://google.com'

      cy.getByTestID('table-row').should('have.length', 0)

      cy.getByTestID('create-scraper-button-header').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('name')
          .clear()
          .type(newScraper)
        cy.getByInputName('url')
          .clear()
          .type(newURL)
        cy.getByTestID('create-scraper--submit').click()
      })

      cy.getByTestID('table-row').should('have.length', 1)
    })

    it('can create a scraper from the create button in the empty state', () => {
      const newScraper = '🐍craper'
      const newURL = 'http://google.com'

      cy.getByTestID('table-row').should('have.length', 0)

      cy.getByTestID('create-scraper-button-empty').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('name')
          .clear()
          .type(newScraper)
        cy.getByInputName('url')
          .clear()
          .type(newURL)
        cy.getByTestID('create-scraper--submit').click()
      })

      cy.getByTestID('table-row').should('have.length', 1)
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

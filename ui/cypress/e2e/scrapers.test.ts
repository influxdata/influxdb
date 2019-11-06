import {Organization, Bucket} from '../../src/types'

// a generous commitment to delivering this page in a loaded state
const PAGE_LOAD_SLA = 10000

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
        cy.visit(`${orgs}/${id}/load-data/scrapers`)
      })
    })
    cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})
  })

  describe('from the org settings', () => {
    it('can create a scraper from the create button in the page header', () => {
      const newScraper = '🐍craper'
      const newURL = 'http://google.com'

      cy.getByTestID('resource-card').should('have.length', 0)

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

      cy.getByTestID('resource-card').should('have.length', 1)
    })

    it('can create a scraper from the create button in the empty state', () => {
      const newScraper = '🐍craper'
      const newURL = 'http://google.com'

      cy.getByTestID('resource-card').should('have.length', 0)

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

      cy.getByTestID('resource-card').should('have.length', 1)
    })

    describe('Scrapers list', () => {
      beforeEach(() => {
        const scraperName = 'New Scraper'
        const url = 'http://google.com'
        const type = 'Prometheus'

        cy.get<Organization>('@org').then((org: Organization) => {
          cy.get<Bucket>('@bucket').then((bucket: Bucket) => {
            cy.createScraper(scraperName, url, type, org.id, bucket.id)
            cy.createScraper(scraperName, url, type, org.id, bucket.id)
          })
        })

        cy.fixture('routes').then(({orgs}) => {
          cy.get<Organization>('@org').then(({id}: Organization) => {
            cy.visit(`${orgs}/${id}/load-data/scrapers`)
          })
        })
        cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})
      })

      it('can update scrapers name', () => {
        const newScraperName = 'This is new name'
        cy.getByTestID('resource-card').within(() => {
          cy.getByTestID('editable-name')
            .first()
            .click()
          cy.getByTestID('input-field').type(`${newScraperName}{enter}`)
        })

        cy.getByTestID('resource-card').contains(newScraperName)
      })

      it('can delete a scraper', () => {
        cy.getByTestID('resource-card').should('have.length', 2)

        cy.getByTestID('confirmation-button')
          .last()
          .click({force: true})

        cy.getByTestID('resource-card').should('have.length', 1)
      })
    })
  })
})

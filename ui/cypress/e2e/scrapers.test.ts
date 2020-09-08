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

  describe('Filtering and sorting', () => {
    beforeEach(() => {
      const type = 'TypeTest'

      cy.get<Organization>('@org').then((org: Organization) => {
        cy.get<Bucket>('@bucket').then((bucket: Bucket) => {
          cy.createScraper(
            'Michigan',
            'http://Michigan.com',
            type,
            org.id,
            bucket.id
          )
          cy.createScraper(
            'Minnesota',
            'http://Minnesota.com',
            type,
            org.id,
            bucket.id
          )
          cy.createScraper('Iowa', 'http://Iowa.com', type, org.id, bucket.id)
          cy.createScraper(
            'Nebraska',
            'http://Nebraska.com',
            type,
            org.id,
            bucket.id
          )
        })
      })

      cy.fixture('routes').then(({orgs}) => {
        cy.get<Organization>('@org').then(({id}: Organization) => {
          cy.visit(`${orgs}/${id}/load-data/scrapers`)
        })
      })
      cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})
    })

    it('can filter scrapers', () => {
      //filter scrapers starting with "Mi"
      cy.getByTestID('resource-card').should('have.length', 4)
      cy.getByTestID('search-widget')
        .type('Mi')
        .then(() => {
          cy.getByTestID('resource-card').should('have.length', 2)
          cy.getByTestID('resource-card').should('contain', 'Michigan')
          cy.getByTestID('resource-card').should('contain', 'Minnesota')
        })

      //clear and assert all scrapers are visible
      cy.getByTestID('search-widget')
        .clear()
        .then(() => {
          cy.getByTestID('resource-card').should('have.length', 4)
        })

      //filter non-existing scrapers
      cy.getByTestID('search-widget')
        .type('Illinois')
        .then(() => {
          cy.getByTestID('empty-state').should('exist')
        })
    })

    it('can sort scrapers', () => {
      //sort by name - descending
      cy.getByTestID('resource-sorter--button')
        .click()
        .then(() => {
          cy.getByTestID('resource-sorter--name-desc')
            .click()
            .then(() => {
              cy.getByTestID('resource-card')
                .eq(0)
                .should('contain', 'Nebraska')
              cy.getByTestID('resource-card')
                .eq(3)
                .should('contain', 'Iowa')
            })
        })

      //sort by name - ascending
      cy.getByTestID('resource-sorter--button')
        .click()
        .then(() => {
          cy.getByTestID('resource-sorter--name-asc')
            .click()
            .then(() => {
              cy.getByTestID('resource-card')
                .eq(0)
                .should('contain', 'Iowa')
              cy.getByTestID('resource-card')
                .eq(3)
                .should('contain', 'Nebraska')
            })
        })

      //sort by url - descending
      cy.getByTestID('resource-sorter--button')
        .click()
        .then(() => {
          cy.getByTestID('resource-sorter--url-desc')
            .click()
            .then(() => {
              cy.getByTestID('resource-card')
                .eq(0)
                .should('contain', 'Nebraska')
              cy.getByTestID('resource-card')
                .eq(3)
                .should('contain', 'Iowa')
            })
        })

      //sort by url - ascending
      cy.getByTestID('resource-sorter--button')
        .click()
        .then(() => {
          cy.getByTestID('resource-sorter--url-asc')
            .click()
            .then(() => {
              cy.getByTestID('resource-card')
                .eq(0)
                .should('contain', 'Iowa')
              cy.getByTestID('resource-card')
                .eq(3)
                .should('contain', 'Nebraska')
            })
        })

      //create new buckets to assert sorting by buckets
      cy.getByTestID('tabs--tab')
        .contains('Buckets')
        .click()
      cy.getByTestID('Create Bucket')
        .click()
        .then(() => {
          cy.getByTestID('bucket-form-name').type('Arkansas')
          cy.getByTestID('bucket-form-submit').click()
        })
      cy.getByTestID('Create Bucket')
        .click()
        .then(() => {
          cy.getByTestID('bucket-form-name').type('Wisconsin')
          cy.getByTestID('bucket-form-submit').click()
        })

      cy.getByTestID('tabs--tab')
        .contains('Scrapers')
        .click()

      //create new scrapers
      cy.getByTestID('create-scraper-button-header').click()
      cy.getByTitle('Name')
        .clear()
        .type('Arkansas')
      cy.getByTestID('bucket-dropdown--button')
        .click()
        .then(() => {
          cy.getByTestID('dropdown-item')
            .contains('Arkansas')
            .click()
            .then(() => {
              cy.getByTestID('create-scraper--submit')
                .click()
                .then(() => {
                  cy.getByTestID('resource-card').should('contain', 'Arkansas')
                })
            })
        })

      cy.getByTestID('create-scraper-button-header').click()
      cy.getByTitle('Name')
        .clear()
        .type('Wisconsin')
      cy.getByTestID('bucket-dropdown--button')
        .click()
        .then(() => {
          cy.getByTestID('dropdown-item')
            .contains('Wisconsin')
            .click()
            .then(() => {
              cy.getByTestID('create-scraper--submit')
                .click()
                .then(() => {
                  cy.getByTestID('resource-card').should('contain', 'Wisconsin')
                })
            })
        })

      //sort by buckets - descending
      cy.getByTestID('resource-sorter--button')
        .click()
        .then(() => {
          cy.getByTestID('resource-sorter--bucket-desc')
            .click()
            .then(() => {
              cy.getByTestID('resource-card')
                .eq(0)
                .should('contain', 'Wisconsin')
              cy.getByTestID('resource-card')
                .eq(5)
                .should('contain', 'Arkansas')
            })
        })

      //sort by buckets - ascending
      cy.getByTestID('resource-sorter--button')
        .click()
        .then(() => {
          cy.getByTestID('resource-sorter--bucket-asc')
            .click()
            .then(() => {
              cy.getByTestID('resource-card')
                .eq(0)
                .should('contain', 'Arkansas')
              cy.getByTestID('resource-card')
                .eq(5)
                .should('contain', 'Wisconsin')
            })
        })
    })
  })
})

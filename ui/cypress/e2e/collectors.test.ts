import {Organization} from '../../src/types'
import {bucket} from '../fixtures/user.json'

describe('Collectors', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/load-data/telegrafs`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a telegraf config', () => {
      const newConfig = 'New Config'
      const configDescription = 'This is a new config testing'

      cy.getByTestID('table-row').should('have.length', 0)

      cy.contains('Create Configuration').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByTestID('telegraf-plugins--System').click()
        cy.getByTestID('next').click()
        cy.getByInputName('name')
          .clear()
          .type(newConfig)
        cy.getByInputName('description')
          .clear()
          .type(configDescription)
        cy.get('.cf-button')
          .contains('Create and Verify')
          .click()
        cy.getByTestID('streaming').within(() => {
          cy.get('.cf-button')
            .contains('Listen for Data')
            .click()
        })
        cy.get('.cf-button')
          .contains('Finish')
          .click()
      })

      cy.getByTestID('resource-card')
        .should('have.length', 1)
        .and('contain', newConfig)
        .and('contain', bucket)
    })

    it('can update configuration name', () => {
      const newConfigName = 'This is new name'

      const telegrafConfigName = 'New Config'
      const description = 'Config Description'

      cy.get('@org').then(({id}: Organization) => {
        cy.createTelegraf(telegrafConfigName, description, id, bucket)
      })

      cy.getByTestID('collector-card--name')
        .first()
        .trigger('mouseover')

      cy.getByTestID('collector-card--name-button')
        .first()
        .click()

      cy.getByTestID('collector-card--input')
        .type(newConfigName)
        .type('{enter}')

      cy.getByTestID('collector-card--name').should('contain', newConfigName)
    })

    describe('when a config already exists', () => {
      beforeEach(() => {
        const telegrafConfigName = 'New Config'
        const description = 'Config Description'
        cy.get('@org').then(({id}: Organization) => {
          cy.createTelegraf(telegrafConfigName, description, id)
        })
      })

      it('can delete a telegraf config', () => {
        cy.getByTestID('resource-card').should('have.length', 1)

        cy.getByTestID('context-menu')
          .last()
          .click({force: true})

        cy.getByTestID('context-menu-item')
          .last()
          .click({force: true})

        cy.getByTestID('empty-state').should('exist')
      })

      it('can view setup instructions for a config', () => {
        cy.getByTestID('resource-card').should('have.length', 1)

        cy.getByTestID('setup-instructions-link').click()

        cy.getByTestID('setup-instructions').should('exist')

        cy.getByTestID('overlay--header')
          .find('button')
          .click()

        cy.getByTestID('setup-instructions').should('not.exist')
      })
    })

    it('can filter telegraf configs correctly', () => {
      // fixes issue #15246:
      // https://github.com/influxdata/influxdb/issues/15246
      const firstTelegraf = 'test1'
      const secondTelegraf = 'test2'
      const thirdTelegraf = 'unicorn'
      const description = 'Config Description'
      const secondBucket = 'bucket2'

      cy.get('@org').then(({id}: Organization) => {
        cy.createTelegraf(firstTelegraf, description, id, bucket)
        cy.createTelegraf(secondTelegraf, description, id, bucket)
        cy.createTelegraf(thirdTelegraf, description, id, secondBucket)
      })

      cy.reload()

      cy.getByTestID('search-widget').type(firstTelegraf)

      cy.getByTestID('resource-card').should('have.length', 1)
      cy.getByTestID('resource-card').should('contain', firstTelegraf)

      cy.getByTestID('search-widget')
        .clear()
        .type(secondTelegraf)

      cy.getByTestID('resource-card').should('have.length', 1)
      cy.getByTestID('resource-card').should('contain', secondTelegraf)

      cy.getByTestID('search-widget')
        .clear()
        .type(thirdTelegraf)

      cy.getByTestID('resource-card').should('have.length', 1)
      cy.getByTestID('resource-card')
        .should('contain', thirdTelegraf)
        .and('contain', secondBucket)

      cy.getByTestID('search-widget')
        .clear()
        .type('should have no results')

      cy.getByTestID('resource-card').should('have.length', 0)
      cy.getByTestID('empty-state').should('exist')

      cy.getByTestID('search-widget')
        .clear()
        .type('test')

      cy.getByTestID('resource-card').should('have.length', 2)
      cy.getByTestID('resource-card').should('contain', firstTelegraf)
      cy.getByTestID('resource-card').should('contain', secondTelegraf)
      cy.getByTestID('resource-card').should('not.contain', thirdTelegraf)
    })

    it('can sort telegraf configs by name', () => {
      const telegrafs = ['b', 'a', 'c']
      const [firstTelegraf, secondTelegraf, thirdTelegraf] = telegrafs
      const description = 'Config Description'

      cy.get('@org').then(({id}: Organization) => {
        cy.createTelegraf(firstTelegraf, description, id, bucket)
        cy.createTelegraf(secondTelegraf, description, id, bucket)
        cy.createTelegraf(thirdTelegraf, description, id, bucket)
      })

      cy.reload()

      // test the order
      // cy.getByTestID('resource-card').should('have.length', 3)
      cy.getByTestID('collector-card--name')
        .not('a')
        .should('have.length', 3)

      // test to see if telegrafs are initially sorted by name
      telegrafs.sort()

      cy.getByTestID('collector-card--name')
        .each((val, index) => {
          expect(val.text()).to.equal(telegrafs[index])
        })
        .then(() => {
          cy.getByTestID('resource-list--sorter')
            .contains('Name')
            .click()
          telegrafs.reverse()
        })
        .then(() => {
          cy.getByTestID('collector-card--name').each((val, index) => {
            expect(val.text()).to.equal(telegrafs[index])
          })
        })
    })

    it.only('can sort telegraf configs by bucket', () => {
      const telegrafs = ['telegraf_a', 'telegraf_b', 'telegraf_c']
      const [firstTelegraf, secondTelegraf, thirdTelegraf] = telegrafs
      const description = 'Config Description'
      const bucketz = ['MO_buckets', 'EZ_buckets', 'Bucky']
      const [firstBucket, secondBucket, thirdBucket] = bucketz

      cy.get('@org').then(({id}: Organization) => {
        cy.createTelegraf(firstTelegraf, description, id, firstBucket)
        cy.createTelegraf(secondTelegraf, description, id, secondBucket)
        cy.createTelegraf(thirdTelegraf, description, id, thirdBucket)
      })

      cy.reload()

      // test the order
      // find all the cf-resource-card--meta-item without children
      cy.getByTestID('cf-resource-card--meta-item').should('have.length', 6)

      cy.getByTestID('cf-resource-card--meta-item')
        .each((val, index) => {
          const text = val.text()
          const i = Math.floor(index / 2)
          if (text.includes('Bucket: ')) {
            expect(text).to.equal(`Bucket: ${bucketz[i]}`)
          }
        })
        .then(() => {
          cy.getByTestID('resource-list--sorter')
            .contains('Bucket')
            .click()
          bucketz.sort()
          cy.getByTestID('cf-resource-card--meta-item').each((val, index) => {
            const text = val.text()
            const i = Math.floor(index / 2)
            if (text.includes('Bucket: ')) {
              expect(text).to.equal(`Bucket: ${bucketz[i]}`)
            }
          })
        })
        .then(() => {
          cy.getByTestID('resource-list--sorter')
            .contains('Bucket')
            .click()
          bucketz.reverse()
          cy.getByTestID('cf-resource-card--meta-item').each((val, index) => {
            const text = val.text()
            const i = Math.floor(index / 2)
            if (text.includes('Bucket: ')) {
              expect(text).to.equal(`Bucket: ${bucketz[i]}`)
            }
          })
        })
    })
  })
})

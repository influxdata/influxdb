import {Organization} from '../../src/types'

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
    })

    it('can update configuration name', () => {
      const newConfigName = 'This is new name'

      const telegrafConfigName = 'New Config'
      const description = 'Config Description'

      cy.get('@org').then(({id}: Organization) => {
        cy.createTelegraf(telegrafConfigName, description, id)
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

      cy.get('@org').then(({id}: Organization) => {
        cy.createTelegraf(firstTelegraf, description, id)
        cy.createTelegraf(secondTelegraf, description, id)
        cy.createTelegraf(thirdTelegraf, description, id)
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
      cy.getByTestID('resource-card').should('contain', thirdTelegraf)

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
  })
})

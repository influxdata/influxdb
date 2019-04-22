import {Organization} from '@influxdata/influx'

describe('Collectors', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/telegrafs`)
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
        cy.getByInputName('System').click({force: true})
        cy.get('.button')
          .contains('Continue')
          .click()
        cy.getByInputName('name')
          .clear()
          .type(newConfig)
        cy.getByInputName('description')
          .clear()
          .type(configDescription)
        cy.get('.button')
          .contains('Create and Verify')
          .click()
        cy.getByTestID('streaming').within(() => {
          cy.get('.button')
            .contains('Listen for Data')
            .click()
        })
        cy.get('.button')
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

      cy.get<Organization>('@org').then(({id}) => {
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

    it.skip('can delete a telegraf config', () => {
      const telegrafConfigName = 'New Config'
      const description = 'Config Description'

      cy.get<Organization>('@org').then(({id}) => {
        cy.createTelegraf(telegrafConfigName, description, id)
        cy.createTelegraf(telegrafConfigName, description, id)
      })

      cy.getByTestID('table-row').should('have.length', 2)

      cy.getByTestID('confirmation-button')
        .last()
        .click({force: true})

      cy.getByTestID('table-row').should('have.length', 1)
    })
  })
})

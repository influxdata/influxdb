import {Organization} from '@influxdata/influx'

describe('Dashboards', () => {
  beforeEach(() => {
    cy.flush()

    cy.setupUser().then(({body}) => {
      cy.wrap(body.org).as('org')
    })

    cy.get<Organization>('@org').then(org => {
      cy.signin(org.id)
    })

    cy.fixture('routes').then(({dashboards}) => {
      cy.visit(dashboards)
    })
  })

  it('can create a dashboard from empty state', () => {
    cy.getByDataTest('empty-state')
      .contains('Create')
      .click()

    cy.visit('/dashboards')

    cy.getByDataTest('resource-card')
      .its('length')
      .should('be.eq', 1)
  })

  it('can create a dashboard from the header', () => {
    cy.get('.page-header--container')
      .contains('Create')
      .click()

    cy.getByDataTest('dropdown--item New Dashboard').click()

    cy.visit('/dashboards')

    cy.getByDataTest('resource-card')
      .its('length')
      .should('be.eq', 1)
  })

  it('can delete a dashboard', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id)
      cy.createDashboard(id)
    })

    cy.getByDataTest('resource-card')
      .its('length')
      .should('eq', 2)

    cy.getByDataTest('resource-card')
      .first()
      .trigger('mouseover')
      .within(() => {
        cy.getByDataTest('context-delete-menu').click()

        cy.getByDataTest('context-delete-dashboard').click()
      })

    cy.getByDataTest('resource-card')
      .its('length')
      .should('eq', 1)
  })

  it.only('can edit a dashboards name', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id)
    })

    const newName = 'new 🅱️ashboard'

    cy.getByDataTest('resource-card').within(() => {
      cy.getByDataTest('dashboard-card--name').trigger('mouseover')

      cy.getByDataTest('dashboard-card--name-button').click()

      cy.get('.input-field')
        .type(newName)
        .type('{enter}')
    })

    cy.visit('/dashboards')

    cy.getByDataTest('resource-card').should('contain', newName)
  })
})

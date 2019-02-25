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
    cy.getByTestID('empty-state')
      .contains('Create')
      .click()

    cy.visit('/dashboards')

    cy.getByTestID('resource-card')
      .its('length')
      .should('be.eq', 1)
  })

  it('can create a dashboard from the header', () => {
    cy.get('.page-header--container')
      .contains('Create')
      .click()

    cy.getByTestID('dropdown--item New Dashboard').click()

    cy.visit('/dashboards')

    cy.getByTestID('resource-card')
      .its('length')
      .should('be.eq', 1)
  })

  it('can delete a dashboard', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id)
      cy.createDashboard(id)
    })

    cy.getByTestID('resource-card')
      .its('length')
      .should('eq', 2)

    cy.getByTestID('resource-card')
      .first()
      .trigger('mouseover')
      .within(() => {
        cy.getByTestID('context-delete-menu').click()

        cy.getByTestID('context-delete-dashboard').click()
      })

    cy.getByTestID('resource-card')
      .its('length')
      .should('eq', 1)
  })

  it('can edit a dashboards name', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id)
    })

    const newName = 'new 🅱️ashboard'

    cy.getByTestID('resource-card').within(() => {
      cy.getByTestID('dashboard-card--name').trigger('mouseover')

      cy.getByTestID('dashboard-card--name-button').click()

      cy.get('.input-field')
        .type(newName)
        .type('{enter}')
    })

    cy.visit('/dashboards')

    cy.getByTestID('resource-card').should('contain', newName)
  })
})

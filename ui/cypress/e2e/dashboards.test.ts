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
    cy.get('.empty-state')
      .contains('Create')
      .click()

    cy.visit('/dashboards')

    cy.get('.index-list--row')
      .its('length')
      .should('be.eq', 1)
  })

  it('can create a dashboard from the header', () => {
    cy.get('.page-header--container')
      .contains('Create')
      .click()

    cy.getByDataTest('dropdown--item New Dashboard').click()

    cy.visit('/dashboards')

    cy.get('.index-list--row')
      .its('length')
      .should('be.eq', 1)
  })

  it('can delete a dashboard', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id)
      cy.createDashboard(id)
    })

    cy.get('.index-list--row').then(rows => {
      const numDashboards = rows.length

      cy.get('.button-danger')
        .first()
        .click()

      cy.contains('Confirm')
        .first()
        .click({force: true})

      cy.get('.index-list--row')
        .its('length')
        .should('eq', numDashboards - 1)
    })
  })

  it('can edit a dashboards name', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id).then(({body}) => {
        cy.visit(`/dashboards/${body.id}`)
      })
    })

    const newName = 'new 🅱️ashboard'

    cy.get('.renamable-page-title--title').click()
    cy.get('.input-field')
      .type(newName)
      .type('{enter}')

    cy.visit('/dashboards')

    cy.get('.index-list--row').should('contain', newName)
  })
})

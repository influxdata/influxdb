import {Organization} from '@influxdata/influx'

describe('Dashboards', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
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

  describe('labeling', () => {
    it('can click to filter dashboard labels', () => {
      const newLabelName = 'click-me'

      cy.get<Organization>('@org').then(({id}) => {
        cy.createDashboard(id).then(({body}) => {
          cy.createLabel('dashboards', body.id, newLabelName)
        })

        cy.createDashboard(id).then(({body}) => {
          cy.createLabel('dashboards', body.id, 'bar')
        })
      })

      cy.visit('/dashboards')

      cy.getByTestID('resource-card').should('have.length', 2)

      cy.getByTestID(`label--pill ${newLabelName}`).click()

      cy.getByTestID('resource-card').should('have.length', 1)
    })
  })

  describe('searching', () => {
    it('can search dashboards by labels', () => {
      const widgetSearch = 'searchME'

      cy.get<Organization>('@org').then(({id}) => {
        cy.createDashboard(id).then(({body}) => {
          cy.createLabel('dashboards', body.id, widgetSearch)
        })

        cy.createDashboard(id).then(({body}) => {
          cy.createLabel('dashboards', body.id, 'bar')
        })
      })

      cy.visit('/dashboards')

      cy.getByTestID('resource-card').should('have.length', 2)

      cy.getByTestID('search-widget').type(widgetSearch)

      cy.getByTestID('resource-card').should('have.length', 1)

      cy.getByTestID('resource-card')
        .first()
        .get('.label')
        .should('contain', widgetSearch)
    })

    it('can search by clicking label', () => {
      const clicked = 'click-me'

      cy.get<Organization>('@org').then(({id}) => {
        cy.createDashboard(id).then(({body}) => {
          cy.createLabel('dashboards', body.id, clicked)
        })

        cy.createDashboard(id).then(({body}) => {
          cy.createLabel('dashboards', body.id, 'bar')
        })
      })

      cy.visit('/dashboards')

      cy.getByTestID('resource-card').should('have.length', 2)

      cy.getByTestID(`label--pill ${clicked}`).click()

      cy.getByTestID('search-widget').should('have.value', clicked)

      cy.getByTestID('resource-card').should('have.length', 1)
    })

    it('can search by dashboard name', () => {
      const searchName = 'beepBoop'
      cy.get<Organization>('@org').then(({id}) => {
        cy.createDashboard(id, searchName)
        cy.createDashboard(id)
      })

      cy.visit('/dashboards')

      cy.getByTestID('search-widget').type('bEE')

      cy.getByTestID('resource-card').should('have.length', 1)
      cy.getByTestID('dashboard-card--name').contains('span', searchName)
    })
  })
})

import {Organization} from '@influxdata/influx'

const newLabelName = 'click-me'

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

    cy.getByTestID('resource-card').should('have.length', 1)
  })

  it('can create a dashboard from the header', () => {
    cy.get('.page-header--container')
      .contains('Create')
      .click()

    cy.getByTestID('dropdown--item New Dashboard').click()

    cy.visit('/dashboards')

    cy.getByTestID('resource-card').should('have.length', 1)
  })

  describe('Dashboard List', () => {
    beforeEach(() => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.createDashboard(id).then(({body}) => {
          cy.createAndAddLabel('dashboards', body.id, newLabelName)
        })

        cy.createDashboard(id).then(({body}) => {
          cy.createAndAddLabel('dashboards', body.id, 'bar')
        })
      })

      cy.visit('/dashboards')
    })

    it('can delete a dashboard', () => {
      cy.getByTestID('resource-card').should('have.length', 2)

      cy.getByTestID('resource-card')
        .first()
        .trigger('mouseover')
        .within(() => {
          cy.getByTestID('context-delete-menu').click()
          cy.getByTestID('context-delete-dashboard').click()
        })

      cy.getByTestID('resource-card').should('have.length', 1)
    })

    it('can edit a dashboards name', () => {
      const newName = 'new 🅱️ashboard'

      cy.getByTestID('resource-card').within(() => {
        cy.getByTestID('dashboard-card--name')
          .first()
          .trigger('mouseover')

        cy.getByTestID('dashboard-card--name-button')
          .first()
          .click()

        cy.get('.input-field')
          .type(newName)
          .type('{enter}')
      })

      cy.getByTestID('resource-card').should('contain', newName)
    })

    describe('Labeling', () => {
      it('can click to filter dashboard labels', () => {
        cy.getByTestID('resource-card').should('have.length', 2)

        cy.getByTestID(`label--pill ${newLabelName}`).click()

        cy.getByTestID('resource-card').should('have.length', 1)
      })

      it('can delete a label from a dashboard', () => {
        cy.getByTestID('resource-card').should('have.length', 2)

        cy.getByTestID('resource-card')
          .first()
          .within(() => {
            const pillID = `label--pill ${newLabelName}`

            cy.getByTestID(pillID).should('have.length', 1)

            cy.getByTestID(`label--pill--delete ${newLabelName}`).click({
              force: true,
            })

            cy.getByTestID(pillID).should('have.length', 0)
            cy.getByTestID(`inline-labels--empty`).should('have.length', 1)
          })
      })

      it('can add an existing label to a dashboard', () => {
        const labelName = 'swogglez'

        cy.createLabel(labelName).then(() => {
          cy.getByTestID('resource-card').should('have.length', 2)

          cy.getByTestID(`inline-labels--add`)
            .first()
            .click()

          cy.getByTestID('inline-labels--popover').within(() => {
            cy.getByTestID(`label--pill ${labelName}`).click()
          })

          cy.visit('/dashboards')

          cy.getByTestID(`label--pill ${labelName}`).should('have.length', 1)
        })
      })
    })

    describe('Searching', () => {
      it('can search dashboards by labels', () => {
        cy.getByTestID('resource-card').should('have.length', 2)

        cy.getByTestID('search-widget').type(newLabelName)

        cy.getByTestID('resource-card').should('have.length', 1)

        cy.getByTestID('resource-card')
          .first()
          .get('.label')
          .should('contain', newLabelName)
      })

      it('can search by clicking label', () => {
        const clicked = 'click-me'

        cy.getByTestID('resource-card').should('have.length', 2)

        cy.getByTestID(`label--pill ${clicked}`).click()

        cy.getByTestID('search-widget').should('have.value', clicked)

        cy.getByTestID('resource-card').should('have.length', 1)
      })

      it('can search by dashboard name', () => {
        cy.getByTestID('search-widget').type('bEE')

        cy.getByTestID('resource-card').should('have.length', 1)
        cy.getByTestID('dashboard-card--name').contains('span', 'beEE')
      })
    })
  })
})

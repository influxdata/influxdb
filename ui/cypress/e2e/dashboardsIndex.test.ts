import {Organization} from '@influxdata/influx'

const newLabelName = 'click-me'
const dashboardName = 'Bee Happy'
const dashSearchName = 'bEE'

describe('Dashboards', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      cy.wrap(body.org).as('org')
    })

    cy.fixture('routes').then(({orgs}) => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}/dashboards`)
      })
    })
  })

  it('can create a dashboard from empty state', () => {
    cy.getByTestID('empty-state')
      .contains('Create')
      .click()

    cy.getByTestID('dropdown--item New Dashboard').click()

    cy.fixture('routes').then(({orgs}) => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}/dashboards`)
      })
    })

    cy.getByTestID('dashboard-card').should('have.length', 1)
  })

  it('can create a dashboard from the header', () => {
    cy.get('.page-header--container')
      .contains('Create')
      .click()

    cy.getByTestID('dropdown--item New Dashboard').click()

    cy.fixture('routes').then(({orgs}) => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}/dashboards`)
      })
    })

    cy.getByTestID('dashboard-card').should('have.length', 1)
  })

  describe('Dashboard List', () => {
    beforeEach(() => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.createDashboard(id, dashboardName).then(({body}) => {
          cy.createAndAddLabel('dashboards', id, body.id, newLabelName)
        })

        cy.createDashboard(id).then(({body}) => {
          cy.createAndAddLabel('dashboards', id, body.id, 'bar')
        })
      })

      cy.fixture('routes').then(({orgs}) => {
        cy.get<Organization>('@org').then(({id}) => {
          cy.visit(`${orgs}/${id}/dashboards`)
        })
      })
    })

    it('can delete a dashboard', () => {
      cy.getByTestID('dashboard-card').should('have.length', 2)

      cy.getByTestID('dashboard-card')
        .first()
        .trigger('mouseover')
        .within(() => {
          cy.getByTestID('context-delete-menu').click()
          cy.getByTestID('context-delete-dashboard').click()
        })

      cy.getByTestID('dashboard-card').should('have.length', 1)
    })

    it('can edit a dashboards name', () => {
      const newName = 'new 🅱️ashboard'

      cy.getByTestID('dashboard-card').within(() => {
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

      cy.getByTestID('dashboard-card').should('contain', newName)
    })

    describe('Labeling', () => {
      it('can click to filter dashboard labels', () => {
        cy.getByTestID('dashboard-card').should('have.length', 2)

        cy.getByTestID(`label--pill ${newLabelName}`).click()

        cy.getByTestID('dashboard-card')
          .should('have.length', 1)
          .and('contain', newLabelName)
      })

      it('can delete a label from a dashboard', () => {
        cy.getByTestID('dashboard-card')
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

        cy.get<Organization>('@org').then(({id}) => {
          cy.createLabel(labelName, id).then(() => {
            cy.getByTestID(`inline-labels--add`)
              .first()
              .click()

            cy.getByTestID('inline-labels--popover').within(() => {
              cy.getByTestID(`label--pill ${labelName}`).click()
            })

            cy.getByTestID('dashboard-card')
              .first()
              .within(() => {
                cy.getByTestID(`label--pill ${labelName}`).should('be.visible')
              })
          })
        })
      })

      it('can create a label and add to a dashboard', () => {
        const label = 'plerps'
        cy.getByTestID(`inline-labels--add`)
          .first()
          .click()

        cy.getByTestID('inline-labels--popover').within(() => {
          cy.getByTestID('inline-labels--popover-field').type(label)
          cy.getByTestID('inline-labels--create-new').click()
        })

        cy.getByTestID('overlay--container').within(() => {
          cy.getByTestID('create-label-form--name').should('have.value', label)
          cy.getByTestID('create-label-form--submit').click()
        })

        cy.getByTestID('dashboard-card')
          .first()
          .within(() => {
            cy.getByTestID(`label--pill ${label}`).should('be.visible')
          })
      })
    })

    describe('Searching', () => {
      it('can search dashboards by labels', () => {
        cy.getByTestID('dashboard-card').should('have.length', 2)

        cy.getByTestID('search-widget').type(newLabelName)

        cy.getByTestID('dashboard-card').should('have.length', 1)

        cy.getByTestID('dashboard-card')
          .first()
          .get('.label')
          .should('contain', newLabelName)
      })

      it('can search by clicking label', () => {
        const clicked = 'click-me'

        cy.getByTestID('dashboard-card').should('have.length', 2)

        cy.getByTestID(`label--pill ${clicked}`).click()

        cy.getByTestID('search-widget').should('have.value', clicked)

        cy.getByTestID('dashboard-card').should('have.length', 1)
      })

      it('can search by dashboard name', () => {
        cy.getByTestID('search-widget').type(dashSearchName)

        cy.getByTestID('dashboard-card').should('have.length', 1)
        cy.getByTestID('dashboard-card--name').contains('span', dashboardName)
      })
    })
  })
})

import {Organization} from '@influxdata/influx'

describe('Dashboard', () => {
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

    cy.getByTestID('resource-card').should('contain', newName)
  })

  it('can create a cell', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id).then(({body}) => {
        cy.visit(`/dashboards/${body.id}`)
      })
    })

    cy.getByTestID('add-cell--button').click()
    cy.getByTestID('save-cell--button').click()
    cy.getByTestID('cell--view-empty').should('have.length', 1)
  })
})

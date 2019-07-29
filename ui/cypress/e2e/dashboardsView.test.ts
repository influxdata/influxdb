import {Organization} from '../../src/types'

describe('Dashboard', () => {
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

  it('can edit a dashboards name', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id).then(({body}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${id}/dashboards/${body.id}`)
        })
      })
    })

    const newName = 'new 🅱️ashboard'

    cy.get('.renamable-page-title--title').click()
    cy.get('.cf-input-field')
      .type(newName)
      .type('{enter}')

    cy.fixture('routes').then(({orgs}) => {
      cy.get<Organization>('@org').then(({id}) => {
        cy.visit(`${orgs}/${id}/dashboards`)
      })
    })

    cy.getByTestID('dashboard-card').should('contain', newName)
  })

  it('can create a cell', () => {
    cy.get<Organization>('@org').then(({id}) => {
      cy.createDashboard(id).then(({body}) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.visit(`${orgs}/${id}/dashboards/${body.id}`)
        })
      })
    })

    cy.getByTestID('add-cell--button').click()
    cy.getByTestID('save-cell--button').click()
    cy.getByTestID('cell--view-empty').should('have.length', 1)
  })
})

describe('Load Data Sources', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/load-data/sources`)
      })
    })
  })

  it('navigate to Client Library details view and render it with essentials', () => {
    cy.getByTestID('write-data--section client-libraries').within(() => {
      cy.getByTestID('square-grid').within(() => {
        cy.getByTestIDSubStr('load-data-item')
          .first()
          .click()
      })
    })

    const contentContainer = cy.getByTestID('load-data-details-content')
    contentContainer.should('exist')
    contentContainer.children().should('exist')

    const logoElement = cy.getByTestID('load-data-details-thumb')
    logoElement.should('exist')
  })

  it('navigate to Telegraf Plugin details view and render it with essentials', () => {
    cy.getByTestID('write-data--section telegraf-plugins').within(() => {
      cy.getByTestID('square-grid').within(() => {
        cy.getByTestIDSubStr('load-data-item')
          .first()
          .click()
      })
    })

    const contentContainer = cy.getByTestID('load-data-details-content')
    contentContainer.should('exist')
    contentContainer.children().should('exist')

    const logoElement = cy.getByTestID('load-data-details-thumb')
    logoElement.should('exist')
  })
})

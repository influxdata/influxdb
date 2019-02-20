describe('DataExplorer', () => {
  beforeEach(() => {
    cy.flush()

    cy.setupUser().then(({body}) => {
      cy.signin(body.org.id)
    })

    cy.fixture('routes').then(({explorer}) => {
      cy.visit(explorer)
    })
  })

  it('renders the page', () => {
    cy.get('.data-explorer').should('exist')
  })
})

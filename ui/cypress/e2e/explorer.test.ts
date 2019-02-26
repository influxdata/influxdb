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

  it('typing a custom query enables the submit button', () => {
    cy.getByTestID('switch-to-script-editor').click()

    cy.getByTestID('time-machine-submit-button').should('be.disabled')

    cy.getByTestID('flux-editor').within(() => {
      cy.get('textarea').type('yo', {force: true})
      cy.getByTestID('time-machine-submit-button').should('not.be.disabled')
    })
  })
})

describe('DataExplorer', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin()

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

  it('deleting query from script editor disables submit', () => {
    cy.getByTestID('switch-to-script-editor').click()

    cy.getByTestID('flux-editor').within(() => {
      cy.get('textarea').type('from(bucket: "foo")', {force: true})
      cy.getByTestID('time-machine-submit-button').should('not.be.disabled')

      cy.get('textarea').type('{selectall} {backspace}', {force: true})
    })

    cy.getByTestID('time-machine-submit-button').should('be.disabled')
  })
})

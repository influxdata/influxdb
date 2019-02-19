// currently getting unauthorized errors for task creation
describe.skip('Tasks', () => {
  let orgID: string = ''
  beforeEach(() => {
    cy.flush()

    cy.setupUser().then(({body}) => {
      orgID = body.org.id
    })

    cy.signin()

    cy.visit('/tasks')
  })

  it('can create a task', () => {
    cy.get('.empty-state').within(() => {
      cy.contains('Create').click()
    })

    cy.getByInputName('name').type('🅱️ask')
    cy.getByInputName('interval').type('1d')
    cy.getByInputName('offset').type('20m')

    cy.getByDataTest('flux-editor').within(() => {
      cy.get('textarea').type('{}', {force: true})
    })

    cy.contains('Save').click()
  })
})

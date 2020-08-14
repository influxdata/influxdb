describe('Client Libraries', () => {
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

  it('navigate to arduino details page', () => {
    cy.getByTestID('client-libraries-cards--arduino').click()
    cy.getByTestID('page-title').contains('Arduino')
  })

  it('navigate to csharp details page', () => {
    cy.getByTestID('client-libraries-cards--csharp').click()
    cy.getByTestID('page-title').contains('C#')
  })

  it('navigate to go details page', () => {
    cy.getByTestID('client-libraries-cards--go').click()
    cy.getByTestID('page-title').contains('GO')
  })

  it('navigate to java details page', () => {
    cy.getByTestID('client-libraries-cards--java').click()
    cy.getByTestID('page-title').contains('Java')
  })

  it('navigate to javascript details page', () => {
    cy.getByTestID('client-libraries-cards--javascript-node').click()
    cy.getByTestID('page-title').contains('JavaScript/Node.js')
  })

  it('navigate to Kotlin details page', () => {
    cy.getByTestID('client-libraries-cards--kotlin').click()
    cy.getByTestID('page-title').contains('Kotlin')
  })

  it('navigate to php details page', () => {
    cy.getByTestID('client-libraries-cards--php').click()
    cy.getByTestID('page-title').contains('PHP')
  })

  it('navigate to python details page', () => {
    cy.getByTestID('client-libraries-cards--python').click()
    cy.getByTestID('page-title').contains('Python')
  })

  it('navigate to ruby details page', () => {
    cy.getByTestID('client-libraries-cards--ruby').click()
    cy.getByTestID('page-title').contains('Ruby')
  })

  it('navigate to scala details page', () => {
    cy.getByTestID('client-libraries-cards--scala').click()
    cy.getByTestID('page-title').contains('Scala')
  })
})

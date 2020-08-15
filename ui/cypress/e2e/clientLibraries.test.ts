describe('Client Libraries', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/load-data/client-libraries`)
      })
    })
  })

  it('open arduino popup', () => {
    cy.getByTestID('client-libraries-cards--arduino').click()
    cy.getByTestID('overlay--header').contains('Arduino Client Library')
  })

  it('open csharp popup', () => {
    cy.getByTestID('client-libraries-cards--csharp').click()
    cy.getByTestID('overlay--header').contains('C# Client Library')
  })

  it('open go popup', () => {
    cy.getByTestID('client-libraries-cards--go').click()
    cy.getByTestID('overlay--header').contains('GO Client Library')
  })

  it('open java popup', () => {
    cy.getByTestID('client-libraries-cards--java').click()
    cy.getByTestID('overlay--header').contains('Java Client Library')
  })

  it('open javascript popup', () => {
    cy.getByTestID('client-libraries-cards--javascript-node').click()
    cy.getByTestID('overlay--header').contains(
      'JavaScript/Node.js Client Library'
    )
  })

  it('open Kotlin popup', () => {
    cy.getByTestID('client-libraries-cards--kotlin').click()
    cy.getByTestID('overlay--header').contains('Kotlin Client Library')
  })

  it('open php popup', () => {
    cy.getByTestID('client-libraries-cards--php').click()
    cy.getByTestID('overlay--header').contains('PHP Client Library')
  })

  it('open python popup', () => {
    cy.getByTestID('client-libraries-cards--python').click()
    cy.getByTestID('overlay--header').contains('Python Client Library')
  })

  it('open ruby popup', () => {
    cy.getByTestID('client-libraries-cards--ruby').click()
    cy.getByTestID('overlay--header').contains('Ruby Client Library')
  })

  it('open scala popup', () => {
    cy.getByTestID('client-libraries-cards--scala').click()
    cy.getByTestID('overlay--header').contains('Scala Client Library')
  })
})

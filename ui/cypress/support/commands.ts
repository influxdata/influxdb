declare namespace Cypress {
  interface Chainable<Subject> {
    login: typeof login
    getByDataTest: typeof getByDataTest
    getByInputName: typeof getByInputName
    createUser: typeof createUser
    flush: typeof flush
  }
}

const login = () => {
  cy.fixture('user').then(user => {
    cy.request({
      method: 'POST',
      url: '/api/v2/signin',
      auth: {user: user.username, pass: user.password},
    })
  })
}

// TODO: have to go through setup because we cannot create a user w/ a password via the user API
const createUser = () => {
  cy.fixture('user').then(({username, password, org, bucket}) => {
    cy.request({
      method: 'POST',
      url: '/api/v2/setup',
      body: {username, password, org, bucket},
    })
  })
}

const flush = () => {
  cy.request({
    method: 'GET',
    url: '/debug/flush',
  })
}

// DOM node getters
const getByDataTest = (dataTest: string): Cypress.Chainable => {
  return cy.get(`[data-test="${dataTest}"]`)
}

const getByInputName = (name: string): Cypress.Chainable => {
  return cy.get(`input[name=${name}]`)
}

Cypress.Commands.add('login', login)
Cypress.Commands.add('getByDataTest', getByDataTest)
Cypress.Commands.add('getByInputName', getByInputName)
Cypress.Commands.add('createUser', createUser)
Cypress.Commands.add('flush', flush)

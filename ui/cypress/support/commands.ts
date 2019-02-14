declare namespace Cypress {
  interface Chainable<Subject> {
    signin: typeof signin
    setupUser: typeof setupUser
    createDashboard: typeof createDashboard
    flush: typeof flush
    getByDataTest: typeof getByDataTest
    getByInputName: typeof getByInputName
  }
}

const signin = (): Cypress.Chainable => {
  return cy.fixture('user').then(user => {
    cy.request({
      method: 'POST',
      url: '/api/v2/signin',
      auth: {user: user.username, pass: user.password},
    })
  })
}

// createDashboard relies on an org fixture to be set
const createDashboard = (orgID: string): Cypress.Chainable => {
  return cy.request({
    method: 'POST',
    url: '/api/v2/dashboards',
    body: {
      name: 'test dashboard',
      orgID,
    },
  })
}

// TODO: have to go through setup because we cannot create a user w/ a password via the user API
const setupUser = (): Cypress.Chainable => {
  return cy.fixture('user').then(({username, password, org, bucket}) => {
    return cy.request({
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
  return cy.get(`[data-testid="${dataTest}"]`)
}

const getByInputName = (name: string): Cypress.Chainable => {
  return cy.get(`input[name=${name}]`)
}

// getters
Cypress.Commands.add('getByDataTest', getByDataTest)
Cypress.Commands.add('getByInputName', getByInputName)

// auth flow
Cypress.Commands.add('signin', signin)

// setup
Cypress.Commands.add('createUser', setupUser)

// dashboards
Cypress.Commands.add('createDashboard', createDashboard)

// general
Cypress.Commands.add('flush', flush)

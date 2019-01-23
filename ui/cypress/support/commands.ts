// ***********************************************
// This example commands.js shows you how to
// create various custom commands and overwrite
// existing commands.
//
// For more comprehensive examples of custom
// commands please read more here:
// https://on.cypress.io/custom-commands
// ***********************************************
//
//
// -- This is a parent command --
// Cypress.Commands.add("login", (email, password) => { ... })
//
//
// -- This is a child command --
// Cypress.Commands.add("drag", { prevSubject: 'element'}, (subject, options) => { ... })
//
//
// -- This is a dual command --
// Cypress.Commands.add("dismiss", { prevSubject: 'optional'}, (subject, options) => { ... })
//
//
// -- This is will overwrite an existing command --
// Cypress.Commands.overwrite("visit", (originalFn, url, options) => { ... })
// ***********************************************

declare namespace Cypress {
  interface Chainable<Subject> {
    login: typeof login
    getByDataTest: typeof getByDataTest
    getByInputName: typeof getByInputName
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

const getByDataTest = (dataTest: string): Cypress.Chainable => {
  return cy.get(`[data-test="${dataTest}"]`)
}

const getByInputName = (name: string): Cypress.Chainable => {
  return cy.get(`input[name=${name}]`)
}

Cypress.Commands.add('login', login)
Cypress.Commands.add('getByDataTest', getByDataTest)
Cypress.Commands.add('getByInputName', getByInputName)

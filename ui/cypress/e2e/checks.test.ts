import {Organization} from '../../src/types'

const measurement = 'my_meas'
const field = 'my_field'
describe('Checks', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.writeData([`${measurement} ${field}=0`, `${measurement} ${field}=1`])
      cy.wrap(body.org).as('org')

      // visit the alerting index
      cy.fixture('routes').then(({orgs, alerting}) => {
        cy.visit(`${orgs}/${id}${alerting}`)
      })
    })
  })

  it('can validate a threshold check', () => {
    cy.getByTestID('create-check').click()

    cy.getByTestID('create-threshold-check').click()

    cy.getByTestID(`selector-list ${measurement}`).click()

    cy.getByTestID('save-cell--button').should('be.disabled')

    cy.getByTestID(`selector-list ${field}`).click()

    cy.getByTestID('save-cell--button').should('be.disabled')

    cy.getByTestID('checkeo--header alerting-tab').click()

    cy.getByTestID('add-threshold-condition-WARN').click()

    cy.getByTestID('save-cell--button').should('be.enabled')
  })

  describe('When a check does not exist', () => {
    it('should route the user to the alerting index page', () => {
      const nonexistentID = '046cd86a2030f000'

      // visitng the check edit overlay
      cy.get('@org').then(({id}: Organization) => {
        cy.fixture('routes').then(({orgs, alerting, checks}) => {
          cy.visit(`${orgs}/${id}${alerting}${checks}/${nonexistentID}/edit`)
          cy.url().should(
            'eq',
            `${Cypress.config().baseUrl}${orgs}/${id}${alerting}`
          )
        })
      })
    })
  })

  describe('Check should be viewable once created', () => {
    beforeEach(() => {
      // creates a check before each iteration
      // TODO: refactor into a request
      cy.getByTestID('create-check').click()
      cy.getByTestID('create-threshold-check').click()
      cy.getByTestID(`selector-list ${measurement}`).click()
      cy.getByTestID('save-cell--button').should('be.disabled')
      cy.getByTestID(`selector-list ${field}`).click()
      cy.getByTestID('save-cell--button').should('be.disabled')
      cy.getByTestID('checkeo--header alerting-tab').click()
      cy.getByTestID('add-threshold-condition-WARN').click()
      cy.getByTestID('save-cell--button').click()
      cy.getByTestID('check-card').should('have.length', 1)
    })

    it('should allow created checks to be selected and routed to the edit page', () => {
      cy.getByTestID('check-card--name').should('have.length', 1)
      cy.getByTestID('check-card--name').click()
      cy.get('@org').then(({id}: Organization) => {
        cy.fixture('routes').then(({orgs, alerting, checks}) => {
          cy.url().then(url => {
            Cypress.minimatch(
              url,
              `
                ${
                  Cypress.config().baseUrl
                }${orgs}/${id}${alerting}${checks}/*/edit
              `
            )
          })
        })
      })
    })
  })
})

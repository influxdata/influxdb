import {Organization} from '../../src/types'

// a generous commitment to delivering this page in a loaded state
const PAGE_LOAD_SLA = 10000

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
    cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})
  })

  it('can validate a threshold check', () => {
    cy.getByTestID('create-check').click()

    cy.getByTestID('create-threshold-check').click()
    // added test to disable group on check query builder
    cy.getByTestID('dropdown--button')
      .should('be.disabled')
      .and('not.contain', 'Group')
      .contains('Filter')
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
      cy.getByTestID('notification-error').should('not.exist')
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

    it('can edit the check card', () => {
      // toggle on / off
      cy.get('.cf-resource-card__disabled').should('not.exist')
      cy.getByTestID('check-card--slide-toggle').click()
      cy.getByTestID('notification-error').should('not.exist')
      cy.get('.cf-resource-card__disabled').should('exist')
      cy.getByTestID('check-card--slide-toggle').click()
      cy.getByTestID('notification-error').should('not.exist')
      cy.get('.cf-resource-card__disabled').should('not.exist')

      // last run status
      cy.getByTestID('last-run-status--icon').should('exist')
      cy.getByTestID('last-run-status--icon').trigger('mouseover')
      cy.getByTestID('popover--dialog')
        .should('exist')
        .contains('Last Run Status:')

      // create a label
      cy.getByTestID('check-card').within(() => {
        cy.getByTestID('inline-labels--add').click()
      })

      const labelName = 'l1'
      cy.getByTestID('inline-labels--popover--contents').type(labelName)
      cy.getByTestID('inline-labels--create-new').click()
      cy.getByTestID('create-label-form--submit').click()

      // delete the label
      cy.getByTestID(`label--pill--delete ${labelName}`).click({force: true})
      cy.getByTestID('inline-labels--empty').should('exist')
    })
  })
})

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

    // User can only see all panels at once on large screens
    cy.getByTestID('alerting-tab--checks').click({force: true})
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
      cy.getByTestID('input-field')
        .clear()
        .type('0')
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

    it('should allow created checks edited checks to persist changes (especially if the value is 0)', () => {
      const checkName = 'Check it out!'
      // Selects the check to edit
      cy.getByTestID('check-card--name').should('have.length', 1)
      cy.getByTestID('check-card--name').click()
      // ensures that the check WARN value is set to 0
      cy.getByTestID('input-field')
        .should('have.value', '0')
        .clear()
        .type('7')
      // renames the check
      cy.getByTestID('page-title')
        .contains('Name this Check')
        .type(checkName)
      cy.getByTestID('save-cell--button').click()
      // checks that the values persisted
      cy.getByTestID('check-card--name').contains(checkName)
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
        // Need to trigger mouseout else the popover obscures the other buttons
        .trigger('mouseout')

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

  describe('Access alert history page', () => {
    it('After check creation confirm history page has graph', () => {
      // creates a check before each iteration
      // TODO: refactor into a request with other before each
      cy.getByTestID('create-check').click()
      cy.getByTestID('create-threshold-check').click()
      cy.getByTestID(`selector-list ${measurement}`).click()
      cy.getByTestID('save-cell--button').should('be.disabled')
      cy.getByTestID(`selector-list ${field}`).click()
      cy.getByTestID('save-cell--button').should('be.disabled')
      cy.getByTestID('checkeo--header alerting-tab').click()
      cy.getByTestID('add-threshold-condition-CRIT').click()
      cy.getByTestID('input-field')
        .clear()
        .type('0')
      cy.getByTestID('add-threshold-condition-WARN').click()
      cy.getByTestID('schedule-check')
        .clear()
        .type('5s')
      cy.getByTestID('offset-options')
        .clear()
        .type('1s')
      cy.getByTestID('save-cell--button').click()
      cy.getByTestID('check-card').should('have.length', 1)
      cy.getByTestID('notification-error').should('not.exist')
      cy.getByTestID('context-history-menu').click()
      cy.getByTestID('context-history-task').click()
      cy.getByTestID('giraffe-axes').should('be.visible')

      //Clicking the check status input results in dropdown and clicking outside removes dropdown
      cy.getByTestID('check-status-input').click()
      cy.getByTestID('check-status-dropdown').should('be.visible')
      cy.getByTestID('alert-history-title').click()
      cy.getByTestID('check-status-dropdown').should('not.exist')

      //Minimize the graph by dragging
      cy.get('.threshold-marker--area.threshold-marker--crit')
        .trigger('mousedown', {which: 1, pageX: 600, pageY: 100})
        .trigger('mousemove', {which: 1, pageX: 700, pageY: 100})
        .trigger('mouseup', {force: true})
    })

    it('accepts keyboard tabs as navigation', () => {
      // have to make the viewport huge to get it not to switch to tablet size
      cy.viewport(1800, 980)

      cy.get('body').tab()
      cy.getByTestID('filter--input checks').should('have.focus')

      cy.focused()
        .tab()
        .tab()
      cy.getByTestID('filter--input endpoints').should('have.focus')

      cy.focused().tab()
      cy.getByTestID('filter--input rules').should('have.focus')
    })
  })
})

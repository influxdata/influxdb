import {SlackNotificationEndpoint, Organization} from '../../src/types'

describe('NotificationRules', () => {
  const name1 = 'Slack 1'
  const name2 = 'Slack 2'
  const name3 = 'Slack 3'

  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      // create the notification endpoints
      cy.fixture('endpoints').then(({slack}) => {
        cy.createEndpoint({...slack, name: name1, orgID: id})
        cy.createEndpoint({...slack, name: name2, orgID: id}).then(({body}) => {
          cy.wrap(body).as('selectedEndpoint')
        })
        cy.createEndpoint({...slack, name: name3, orgID: id})
      })

      // visit the alerting index
      cy.fixture('routes').then(({orgs, alerting}) => {
        cy.visit(`${orgs}/${id}${alerting}`)

        // User can only see all panels at once on large screens
        cy.getByTestID('alerting-tab--rules').click()
      })
    })
  })

  describe('When a rule does not exist', () => {
    it('should route the user to the alerting index page', () => {
      const nonexistentID = '04984be058066088'

      // visitng the rules edit overlay
      cy.get('@org').then(({id}: Organization) => {
        cy.fixture('routes').then(({orgs, alerting, rules}) => {
          cy.visit(`${orgs}/${id}${alerting}${rules}/${nonexistentID}/edit`)
          cy.url().should(
            'eq',
            `${Cypress.config().baseUrl}${orgs}/${id}${alerting}`
          )
        })
      })
    })
  })

  describe('numeric input validation in Theshold Checks', () => {
    beforeEach(() => {
      cy.getByTestID('page-contents').within(() => {
        cy.getByTestID('dropdown').click()
        cy.getByTestID('create-threshold-check').click()
      })
    })

    describe('when threshold is above', () => {
      it('should put input field in error status and stay in error status when input is invalid or empty', () => {
        cy.getByTestID('checkeo--header alerting-tab').click()
        cy.getByTestID('add-threshold-condition-CRIT').click()
        cy.getByTestID('builder-conditions').within(() => {
          cy.getByTestID('panel').within(() => {
            cy.getByTestID('input-field')
              .click()
              .type('{backspace}{backspace}')
              .invoke('attr', 'type')
              .should('equal', 'text')
              .getByTestID('input-field--error')
              .should('have.length', 1)
              .and('have.value', '')
            cy.getByTestID('input-field')
              .click()
              .type('somerangetext')
              .invoke('val')
              .should('equal', '')
              .getByTestID('input-field--error')
              .should('have.length', 1)
          })
        })
      })

      it('should allow "20" to be deleted and then allow numeric input to get out of error status', () => {
        cy.getByTestID('checkeo--header alerting-tab').click()
        cy.getByTestID('add-threshold-condition-CRIT').click()
        cy.getByTestID('builder-conditions').within(() => {
          cy.getByTestID('panel').within(() => {
            cy.getByTestID('input-field')
              .click()
              .type('{backspace}{backspace}9')
              .invoke('val')
              .should('equal', '9')
              .getByTestID('input-field--error')
              .should('have.length', 0)
          })
        })
      })
    })

    describe('when threshold is inside range', () => {
      it('should put input field in error status and stay in error status when input is invalid or empty', () => {
        cy.getByTestID('checkeo--header alerting-tab').click()
        cy.getByTestID('add-threshold-condition-CRIT').click()
        cy.getByTestID('builder-conditions').within(() => {
          cy.getByTestID('panel').within(() => {
            cy.getByTestID('dropdown--button').click()
            cy.get(
              '.cf-dropdown-item--children:contains("is inside range")'
            ).click()
            cy.getByTestID('input-field')
              .first()
              .click()
              .type('{backspace}{backspace}')
              .invoke('attr', 'type')
              .should('equal', 'text')
              .getByTestID('input-field--error')
              .should('have.length', 1)
              .and('have.value', '')
            cy.getByTestID('input-field')
              .first()
              .click()
              .type('hhhhhhhhhhhh')
              .invoke('val')
              .should('equal', '')
              .getByTestID('input-field--error')
              .should('have.length', 1)
          })
        })
      })

      it('should allow "20" to be deleted and then allow numeric input to get out of error status', () => {
        cy.getByTestID('checkeo--header alerting-tab').click()
        cy.getByTestID('add-threshold-condition-CRIT').click()
        cy.getByTestID('builder-conditions').within(() => {
          cy.getByTestID('panel').within(() => {
            cy.getByTestID('dropdown--button').click()
            cy.get(
              '.cf-dropdown-item--children:contains("is inside range")'
            ).click()
            cy.getByTestID('input-field')
              .first()
              .click()
              .type('{backspace}{backspace}7')
              .invoke('val')
              .should('equal', '7')
              .getByTestID('input-field--error')
              .should('have.length', 0)
          })
        })
      })
    })
  })

  it('can create a notification rule', () => {
    const ruleName = 'my-new-rule'
    cy.getByTestID('create-rule').click()

    cy.getByTestID('rule-name--input').type(ruleName)

    cy.getByTestID('rule-schedule-every--input')
      .clear()
      .type('20m')
      .should('have.value', '20m')

    cy.getByTestID('rule-schedule-offset--input')
      .clear()
      .type('1m')
      .should('have.value', '1m')

    // Editing a Status Rule
    cy.getByTestID('status-rule').within(() => {
      cy.getByTestID('status-change--dropdown')
        .click()
        .within(() => {
          cy.getByTestID('status-change--dropdown-item is equal to').click()
          cy.getByTestID('status-change--dropdown--button').within(() => {
            cy.contains('equal')
          })
        })

      cy.getByTestID('levels--dropdown previousLevel').should('not.exist')
      cy.getByTestID('levels--dropdown currentLevel').should('exist')

      cy.getByTestID('status-change--dropdown')
        .click()
        .within(() => {
          cy.getByTestID('status-change--dropdown-item changes from').click()
          cy.getByTestID('status-change--dropdown--button').within(() => {
            cy.contains('changes from')
          })
        })

      cy.getByTestID('levels--dropdown previousLevel').click()
      cy.getByTestID('levels--dropdown-item INFO').click()
      cy.getByTestID('levels--dropdown--button previousLevel').within(() => {
        cy.contains('INFO')
      })

      cy.getByTestID('levels--dropdown currentLevel').click()
      cy.getByTestID('levels--dropdown-item CRIT').click()
      cy.getByTestID('levels--dropdown--button currentLevel').within(() => {
        cy.contains('CRIT')
      })
    })

    cy.getByTestID('endpoint--dropdown--button')
      .within(() => {
        cy.contains(name1)
      })
      .click()

    cy.get<SlackNotificationEndpoint>('@selectedEndpoint').then(({id}) => {
      cy.getByTestID(`endpoint--dropdown-item ${id}`).click()
      cy.getByTestID('endpoint--dropdown--button')
        .within(() => {
          cy.contains(name2)
        })
        .click()
    })

    const message = `
      Have you ever wanted to interrupt all your co-workers, but don't
      want to struggle with the hassle of typing @here in #general? Well,
      do we have the notification for you!
    `

    cy.getByTestID('slack-message-template--textarea')
      .clear()
      .type(message)
      .should('contain', message)

    cy.getByTestID('rule-overlay-save--button').click()

    // Add a label
    cy.getByTestID(`rule-card ${ruleName}`).within(() => {
      cy.getByTestID('inline-labels--add').click()
    })

    const labelName = 'l1'
    cy.getByTestID('inline-labels--popover--contents').type(labelName)
    cy.getByTestID('inline-labels--create-new').click()
    cy.getByTestID('create-label-form--submit').click()

    // Delete the label
    cy.getByTestID(`label--pill--delete ${labelName}`).click({force: true})
    cy.getByTestID('inline-labels--empty').should('exist')

    // Filter for the new rule
    cy.getByTestID('filter--input rules').type(ruleName)

    cy.getByTestID('rule-card--name')
      .contains(ruleName)
      .click()

    const editedName = ruleName + '!'

    // Edit the rule
    cy.getByTestID('rule-name--input')
      .clear()
      .type(editedName)

    cy.getByTestID('rule-schedule-every--input')
      .clear()
      .type('21m')
      .should('have.value', '21m')

    cy.getByTestID('rule-schedule-offset--input')
      .clear()
      .type('2m')
      .should('have.value', '2m')

    cy.getByTestID('rule-overlay-save--button').click()

    // Open overlay
    cy.getByTestID('rule-card--name')
      .contains(editedName)
      .click()

    // Close overlay
    cy.getByTestID('dismiss-overlay')
      .find('button')
      .click()

    // Delete the rule
    cy.getByTestID('rules--column').within(() => {
      cy.getByTestID(`context-delete-menu`).click()
      cy.getByTestID(`context-delete-task`).click()
    })

    // Remove the filter
    cy.getByTestID('filter--input rules').clear()
    cy.getByTestID('rule-card--name').should('have.length', 0)
  })
})

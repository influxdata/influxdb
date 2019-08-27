import {SlackNotificationEndpoint} from '../../src/types'

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
      })
    })
  })

  // TODO(desa): this needs to be skipped until https://github.com/influxdata/influxdb/issues/14799
  it.skip('can create a notification rule', () => {
    const ruleName = 'my-new-rule'
    cy.getByTestID('alert-column--header create-rule').click()

    cy.getByTestID('rule-name--input').type(ruleName)

    // Rule schedule section
    cy.getByTestID('rule-schedule-cron').click()
    cy.getByTestID('rule-schedule-cron--input')
      .type('2 0 * * *')
      .should('have.value', '2 0 * * *')
    cy.getByTestID('rule-schedule-every').click()
    cy.getByTestID('rule-schedule-every--input')
      .type('20m')
      .should('have.value', '20m')
    cy.getByTestID('rule-schedule-offset--input')
      .type('1m')
      .should('have.value', '1m')

    // Editing a Status Rule
    cy.getByTestID('status-rule').within(() => {
      cy.getByTestID('status-change--dropdown')
        .click()
        .within(() => {
          cy.getByTestID('status-change--dropdown-item equal').click()
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

    cy.getByTestID('slack-channel--input').type('interrupt.your.coworkers')
    cy.getByTestID('slack-message-template--textarea').type(`
      Have you ever wanted to interrupt all your co-workers, but don't
      want to struggle with the hassle of typing @here in #general? Well,
      do we have the notification for you!
    `)

    cy.getByTestID('rule-overlay-save--button').click()

    cy.getByTestID(`rule-card--name`).contains(ruleName)
  })
})

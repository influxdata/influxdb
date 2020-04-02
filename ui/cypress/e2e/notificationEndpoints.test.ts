import {
  NotificationEndpoint,
  SlackNotificationEndpoint,
  Organization,
} from '../../src/types'

describe('Notification Endpoints', () => {
  const endpoint: NotificationEndpoint = {
    orgID: '',
    name: 'Pre-Created Endpoint',
    userID: '',
    description: 'interrupt everyone at work',
    status: 'active',
    type: 'slack',
    url: 'insert.slack.url.here',
    token: 'plerps',
  }

  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')
      cy.fixture('routes').then(({orgs, alerting}) => {
        cy.createEndpoint({...endpoint, orgID: id}).then(({body}) => {
          cy.wrap(body).as('endpoint')
        })
        cy.visit(`${orgs}/${id}${alerting}`)

        // User can only see all panels at once on large screens
        cy.getByTestID('alerting-tab--endpoints').click({force: true})
      })
    })
  })

  it('can create a notification endpoint', () => {
    const name = 'An Endpoint Has No Name'
    const description =
      'A minute, an hour, a month. Notification Endpoint is certain. The time is not.'

    cy.getByTestID('create-endpoint').click()

    cy.getByTestID('endpoint-name--input')
      .clear()
      .type(name)
      .should('have.value', name)

    cy.getByTestID('endpoint-description--textarea')
      .clear()
      .type(description)
      .should('have.value', description)

    cy.getByTestID('endpoint-change--dropdown')
      .click()
      .within(() => {
        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('Slack')
        })

        cy.getByTestID('endpoint--dropdown-item http').click()

        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('HTTP')
        })
      })

    cy.getByTestID('http-url')
      .clear()
      .type('http.url.us')
      .should('have.value', 'http.url.us')

    cy.getByTestID('endpoint-change--dropdown')
      .click()
      .within(() => {
        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('HTTP')
        })

        cy.getByTestID('endpoint--dropdown-item slack').click()

        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('Slack')
        })
      })

    cy.getByTestID('slack-url')
      .clear()
      .type('slack.url.us')
      .should('have.value', 'slack.url.us')

    cy.getByTestID('endpoint-save--button').click()

    cy.getByTestID(`endpoint-card ${name}`).should('exist')
    cy.getByTestID('endpoint--overlay').should('not.be.visible')
  })

  it('can create a notification endpoint pager duty with client url', () => {
    const name = 'Pagerduty example'

    cy.getByTestID('create-endpoint').click()

    cy.getByTestID('endpoint-name--input')
      .clear()
      .type(name)
      .should('have.value', name)

    cy.getByTestID('endpoint-change--dropdown')
      .click()
      .within(() => {
        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('Slack')
        })

        cy.getByTestID('endpoint--dropdown-item pagerduty').click()

        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('Pagerduty')
        })
      })

    cy.getByTestID('pagerduty-url')
      .clear()
      .type('many-faced-god.gov')
      .should('have.value', 'many-faced-god.gov')

    cy.getByTestID('pagerduty-routing-key')
      .type('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9')
      .should('have.value', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9')

    cy.getByTestID('endpoint-save--button').click()

    cy.getByTestID(`endpoint-card ${name}`).should('exist')
    cy.getByTestID('endpoint--overlay').should('not.be.visible')
  })

  it('can create a notification endpoint pager duty without client url', () => {
    const name = 'Pagerduty url none'

    cy.getByTestID('create-endpoint').click()

    cy.getByTestID('endpoint-name--input')
      .clear()
      .type(name)
      .should('have.value', name)

    cy.getByTestID('endpoint-change--dropdown')
      .click()
      .within(() => {
        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('Slack')
        })

        cy.getByTestID('endpoint--dropdown-item pagerduty').click()

        cy.getByTestID('endpoint--dropdown--button').within(() => {
          cy.contains('Pagerduty')
        })
      })

    cy.getByTestID('pagerduty-url').clear()

    cy.getByTestID('pagerduty-routing-key')
      .type('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9')
      .should('have.value', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9')

    cy.getByTestID('endpoint-save--button').click()

    cy.getByTestID(`endpoint-card ${name}`).should('exist')
    cy.getByTestID('endpoint--overlay').should('not.be.visible')
  })

  it('can edit a notification endpoint', () => {
    cy.get<SlackNotificationEndpoint>('@endpoint').then(endpoint => {
      const {name, description, url} = endpoint
      const newName = 'An Endpoint Has No Name'
      const newDescription =
        'A minute, an hour, a month. Notification Endpoint is certain. The time is not.'
      const newURL = 'many-faced-god.gov'

      cy.getByTestID(`endpoint-card--name ${name}`).click()

      cy.getByTestID('endpoint-name--input')
        .should('have.value', name)
        .clear()
        .type(newName)
        .should('have.value', newName)

      cy.getByTestID('endpoint-description--textarea')
        .should('have.value', description)
        .clear()
        .type(newDescription)
        .should('have.value', newDescription)

      cy.getByTestID('slack-url').should('have.value', url)

      cy.getByTestID('endpoint-change--dropdown')
        .click()
        .within(() => {
          cy.getByTestID('endpoint--dropdown--button').within(() => {
            cy.contains('Slack')
          })

          cy.getByTestID('endpoint--dropdown-item pagerduty').click()

          cy.getByTestID('endpoint--dropdown--button').within(() => {
            cy.contains('Pagerduty')
          })
        })

      cy.getByTestID('pagerduty-url')
        .clear()
        .type(newURL)
        .should('have.value', newURL)

      cy.getByTestID('pagerduty-routing-key')
        .clear()
        .type('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9')
        .should('have.value', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9')

      cy.getByTestID('endpoint-save--button').click()

      cy.getByTestID('endpoint--overlay').should('not.be.visible')

      // Create a label
      cy.getByTestID(`endpoint-card ${newName}`).within(() => {
        cy.getByTestID('inline-labels--add').click()
      })

      const labelName = 'l1'
      cy.getByTestID('inline-labels--popover--contents').type(labelName)
      cy.getByTestID('inline-labels--create-new').click()
      cy.getByTestID('create-label-form--submit').click()

      // Delete the label
      cy.getByTestID(`label--pill--delete ${labelName}`).click({force: true})
      cy.getByTestID('inline-labels--empty').should('exist')
    })
  })

  it('can view history of endpoint', () => {
    cy.get<SlackNotificationEndpoint>('@endpoint').then(() => {
      cy.getByTestID(`context-history-menu`).click()
      cy.getByTestID(`context-history-task`).click()
      cy.getByTestID(`alert-history-title`).should('exist')
    })
  })

  it('can delete endpoint', () => {
    cy.get<SlackNotificationEndpoint>('@endpoint').then(() => {
      cy.getByTestID(`context-delete-menu`).click()
      cy.getByTestID(`context-delete-task`).click()
      cy.getByTestID(`endpoint-card--name ${name}`).should('not.exist')
    })
  })

  describe('When a endpoint does not exist', () => {
    it('should route the user to the alerting index page', () => {
      const nonexistentID = '0495f0d1247ab600'

      // visitng the endpoint edit overlay
      cy.get('@org').then(({id}: Organization) => {
        cy.fixture('routes').then(({orgs, alerting, endpoints}) => {
          cy.visit(`${orgs}/${id}${alerting}${endpoints}/${nonexistentID}/edit`)
          cy.url().should(
            'eq',
            `${Cypress.config().baseUrl}${orgs}/${id}${alerting}`
          )
        })
      })
    })
  })
})

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
        cy.getByTestID('dropdown')
          .click()
          .then(() => {
            cy.getByTestID('create-threshold-check').click()
          })
      })
    })
    describe('when threshold is above', () => {
      it('should allow "20" to be deleted to temporarily become a text input field in error status', () => {
        cy.getByTestID('checkeo--header alerting-tab')
          .click()
          .then(() => {
            cy.getByTestID('add-threshold-condition-CRIT')
              .click()
              .then(() => {
                cy.getByTestID('builder-conditions').within(() => {
                  cy.getByTestID('panel').within(() => {
                    cy.getByTestID('input-field')
                      .click()
                      .type('{backspace}{backspace}')
                      .invoke('attr', 'type')
                      .should('equal', 'text')
                      .getByTestID('input-field--error')
                      .should('have.length', 1)
                  })
                })
              })
          })
      })
      it('should allow "20" to be deleted to become a blank input field in error status', () => {
        cy.getByTestID('checkeo--header alerting-tab')
          .click()
          .then(() => {
            cy.getByTestID('add-threshold-condition-CRIT')
              .click()
              .then(() => {
                cy.getByTestID('builder-conditions').within(() => {
                  cy.getByTestID('panel').within(() => {
                    cy.getByTestID('input-field')
                      .click()
                      .type('{backspace}{backspace}')
                      .invoke('val')
                      .should('equal', '')
                      .getByTestID('input-field--error')
                      .should('have.length', 1)
                  })
                })
              })
          })
      })
      it('should allow "20" to be deleted to temporarily become a text input field but does NOT allow text input and remains in error status', () => {
        cy.getByTestID('checkeo--header alerting-tab')
          .click()
          .then(() => {
            cy.getByTestID('add-threshold-condition-CRIT')
              .click()
              .then(() => {
                cy.getByTestID('builder-conditions').within(() => {
                  cy.getByTestID('panel').within(() => {
                    cy.getByTestID('input-field')
                      .click()
                      .type('{backspace}{backspace}somerange')
                      .invoke('val')
                      .should('equal', '')
                      .getByTestID('input-field--error')
                      .should('have.length', 1)
                  })
                })
              })
          })
      })
      it('should allow "20" to be deleted and then allow numeric input to get out of error status', () => {
        cy.getByTestID('checkeo--header alerting-tab')
          .click()
          .then(() => {
            cy.getByTestID('add-threshold-condition-CRIT')
              .click()
              .then(() => {
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
      })
    })
    describe('when threshold is inside range', () => {
      it('should allow "20" to be deleted to temporarily become a text input field in error status', () => {
        cy.getByTestID('checkeo--header alerting-tab')
          .click()
          .then(() => {
            cy.getByTestID('add-threshold-condition-CRIT')
              .click()
              .then(() => {
                cy.getByTestID('builder-conditions').within(() => {
                  cy.getByTestID('panel').within(() => {
                    cy.getByTestID('dropdown--button')
                      .click()
                      .then(() => {
                        cy.get(
                          '.cf-dropdown-item--children:contains("is inside range")'
                        )
                          .click()
                          .then(() => {
                            cy.getByTestID('input-field')
                              .first()
                              .click()
                              .type('{backspace}{backspace}')
                              .invoke('attr', 'type')
                              .should('equal', 'text')
                              .getByTestID('input-field--error')
                              .should('have.length', 1)
                          })
                      })
                  })
                })
              })
          })
      })
      it('should allow "20" to be deleted to become a blank input field in error status', () => {
        cy.getByTestID('checkeo--header alerting-tab')
          .click()
          .then(() => {
            cy.getByTestID('add-threshold-condition-CRIT')
              .click()
              .then(() => {
                cy.getByTestID('builder-conditions').within(() => {
                  cy.getByTestID('panel').within(() => {
                    cy.getByTestID('dropdown--button')
                      .click()
                      .then(() => {
                        cy.get(
                          '.cf-dropdown-item--children:contains("is inside range")'
                        )
                          .click()
                          .then(() => {
                            cy.getByTestID('input-field')
                              .first()
                              .click()
                              .type('{backspace}{backspace}')
                              .invoke('val')
                              .should('equal', '')
                              .getByTestID('input-field--error')
                              .should('have.length', 1)
                          })
                      })
                  })
                })
              })
          })
      })
      it('should allow "20" to be deleted to temporarily become a text input field but does NOT allow text input and remains in error status', () => {
        cy.getByTestID('checkeo--header alerting-tab')
          .click()
          .then(() => {
            cy.getByTestID('add-threshold-condition-CRIT')
              .click()
              .then(() => {
                cy.getByTestID('builder-conditions').within(() => {
                  cy.getByTestID('panel').within(() => {
                    cy.getByTestID('dropdown--button')
                      .click()
                      .then(() => {
                        cy.get(
                          '.cf-dropdown-item--children:contains("is inside range")'
                        )
                          .click()
                          .then(() => {
                            cy.getByTestID('input-field')
                              .first()
                              .click()
                              .type('{backspace}{backspace}hhhhhhhhhhhh')
                              .invoke('val')
                              .should('equal', '')
                              .getByTestID('input-field--error')
                              .should('have.length', 1)
                          })
                      })
                  })
                })
              })
          })
      })
      it('should allow "20" to be deleted and then allow numeric input to get out of error status', () => {
        cy.getByTestID('checkeo--header alerting-tab')
          .click()
          .then(() => {
            cy.getByTestID('add-threshold-condition-CRIT')
              .click()
              .then(() => {
                cy.getByTestID('builder-conditions').within(() => {
                  cy.getByTestID('panel').within(() => {
                    cy.getByTestID('dropdown--button')
                      .click()
                      .then(() => {
                        cy.get(
                          '.cf-dropdown-item--children:contains("is inside range")'
                        )
                          .click()
                          .then(() => {
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
          })
      })
    })
  })

  // TODO(desa): this needs to be skipped until https://github.com/influxdata/influxdb/issues/14799
  it.skip('can create a notification rule', () => {
    const ruleName = 'my-new-rule'
    cy.getByTestID('alert-column--header create-rule').click()

    cy.getByTestID('rule-name--input').type(ruleName)

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

    describe('can edit a notification rule', () => {
      cy.get<SlackNotificationEndpoint>('@endpoint').then(() => {
        cy.getByTestID(`context-history-menu`).click()
        cy.getByTestID(`context-history-task`).click()
        cy.getByTestID(`alert-history-title`).should('exist')
      })

      it('can delete endpoint', () => {
        cy.get<SlackNotificationEndpoint>('@endpoint').then(() => {
          cy.getByTestID(`context-delete-menu`).click()
          cy.getByTestID(`context-delete-task`).click()
          //then finally check to make sure the endpoint is gone
        })
      })
    })
  })
})

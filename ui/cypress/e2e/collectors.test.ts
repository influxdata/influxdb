import {Organization} from '../../src/types'

describe('Collectors', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/load-data/telegrafs`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a telegraf config', () => {
      const newConfig = 'New Config'
      const configDescription = 'This is a new config testing'

      cy.getByTestID('table-row')
        .should('have.length', 0)
        .then(() => {
          cy.contains('Create Configuration')
            .click()
            .then(() => {
              cy.getByTestID('overlay--container').within(() => {
                cy.getByTestID('telegraf-plugins--System')
                  .click()
                  .then(() => {
                    cy.getByTestID('next')
                      .click()
                      .then(() => {
                        cy.getByInputName('name')
                          .clear()
                          .type(newConfig)
                        cy.getByInputName('description')
                          .clear()
                          .type(configDescription)
                        cy.get('.cf-button')
                          .contains('Create and Verify')
                          .click()
                        cy.getByTestID('streaming').within(() => {
                          cy.get('.cf-button')
                            .contains('Listen for Data')
                            .click()
                        })
                        cy.get('.cf-button')
                          .contains('Finish')
                          .click()
                      })
                  })
              })
            })
        })

      cy.fixture('user').then(({bucket}) => {
        cy.getByTestID('resource-card')
          .should('have.length', 1)
          .and('contain', newConfig)
          .and('contain', bucket)
      })
    })

    describe('when a config already exists', () => {
      beforeEach(() => {
        const telegrafConfigName = 'New Config'
        const description = 'Config Description'
        cy.get('@org').then(({id}: Organization) => {
          cy.fixture('user').then(({bucket}) => {
            cy.createTelegraf(telegrafConfigName, description, id, bucket)
          })
        })

        cy.reload()
      })

      it('can update configuration name and delete a configuration', () => {
        const newConfigName = 'This is new name'

        cy.getByTestID('collector-card--name')
          .first()
          .trigger('mouseover')
          .then(() => {
            cy.getByTestID('collector-card--name-button')
              .first()
              .click()
              .then(() => {
                cy.getByTestID('collector-card--input')
                  .type(newConfigName)
                  .type('{enter}')
                  .then(() => {
                    cy.getByTestID('collector-card--name').should(
                      'contain',
                      newConfigName
                    )
                  })
              })
          })

        cy.getByTestID('resource-card').should('have.length', 1)

        cy.getByTestID('context-menu')
          .last()
          .click()
          .then(() => {
            cy.getByTestID('context-menu-item')
              .last()
              .click()
              .then(() => {
                cy.getByTestID('empty-state').should('exist')
              })
          })
      })

      it('can view setup instructions for a config', () => {
        cy.getByTestID('resource-card').should('have.length', 1)

        cy.getByTestID('setup-instructions-link').click()

        cy.getByTestID('setup-instructions').should('exist')

        cy.getByTestID('overlay--header')
          .find('button')
          .click()

        cy.getByTestID('setup-instructions').should('not.exist')
      })
    })

    describe('sorting & filtering', () => {
      const telegrafs = ['bad', 'apple', 'cookie']
      const bucketz = ['MO_buckets', 'EZ_buckets', 'Bucky']
      const [firstTelegraf, secondTelegraf, thirdTelegraf] = telegrafs
      beforeEach(() => {
        const description = 'Config Description'
        const [firstBucket, secondBucket, thirdBucket] = bucketz
        cy.get('@org').then(({id}: Organization) => {
          cy.createTelegraf(firstTelegraf, description, id, firstBucket)
          cy.createTelegraf(secondTelegraf, description, id, secondBucket)
          cy.createTelegraf(thirdTelegraf, description, id, thirdBucket)
        })
        cy.reload()
      })
      // filter by name
      it('can filter telegraf configs and sort by bucket and name', () => {
        // fixes https://github.com/influxdata/influxdb/issues/15246
        cy.getByTestID('search-widget')
          .type(firstTelegraf)
          .then(() => {
            cy.getByTestID('resource-card').should('have.length', 1)
            cy.getByTestID('resource-card').should('contain', firstTelegraf)

            cy.getByTestID('search-widget')
              .clear()
              .type(secondTelegraf)
              .then(() => {
                cy.getByTestID('resource-card').should('have.length', 1)
                cy.getByTestID('resource-card').should(
                  'contain',
                  secondTelegraf
                )

                cy.getByTestID('search-widget')
                  .clear()
                  .type(thirdTelegraf)
                  .then(() => {
                    cy.getByTestID('resource-card').should('have.length', 1)
                    cy.getByTestID('resource-card').should(
                      'contain',
                      thirdTelegraf
                    )

                    cy.getByTestID('search-widget')
                      .clear()
                      .type('should have no results')
                      .then(() => {
                        cy.getByTestID('resource-card').should('have.length', 0)
                        cy.getByTestID('empty-state').should('exist')

                        cy.getByTestID('search-widget')
                          .clear()
                          .type('a')
                          .then(() => {
                            cy.getByTestID('resource-card').should(
                              'have.length',
                              2
                            )
                            cy.getByTestID('resource-card').should(
                              'contain',
                              firstTelegraf
                            )
                            cy.getByTestID('resource-card').should(
                              'contain',
                              secondTelegraf
                            )
                            cy.getByTestID('resource-card').should(
                              'not.contain',
                              thirdTelegraf
                            )
                          })
                      })
                  })
              })
          })

        // sort by buckets test here
        cy.reload() // clear out filtering state from the previous test
        cy.getByTestID('bucket-sorter')
          .click()
          .then(() => {
            bucketz.sort()
            cy.getByTestID('bucket-name')
              .should('have.length', 3)
              .each((val, index) => {
                const text = val.text()
                expect(text).to.include(bucketz[index])
              })
              .then(() => {
                cy.getByTestID('bucket-sorter').click()
                bucketz.reverse()
                cy.getByTestID('bucket-name').each((val, index) => {
                  const text = val.text()
                  expect(text).to.include(bucketz[index])
                })
              })
          })

        // sort by name test here
        cy.reload() // clear out sorting state from previous test
        cy.getByTestID('collector-card--name').should('have.length', 3)

        // test to see if telegrafs are initially sorted by name
        telegrafs.sort()

        cy.getByTestID('collector-card--name')
          .each((val, index) => {
            expect(val.text()).to.include(telegrafs[index])
          })
          .then(() => {
            telegrafs.reverse()
            cy.getByTestID('name-sorter')
              .click()
              .then(() => {
                cy.getByTestID('collector-card--name').each((val, index) => {
                  expect(val.text()).to.include(telegrafs[index])
                })
              })
          })
      })
    })
  })
})

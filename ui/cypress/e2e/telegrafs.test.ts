import {Organization} from '../../src/types'

// a generous commitment to delivering this page in a loaded state
const PAGE_LOAD_SLA = 10000

describe('Collectors', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
      } = body
      cy.wrap(body.org).as('org')

      cy.fixture('routes').then(({orgs, telegrafs}) => {
        cy.visit(`${orgs}/${id}${telegrafs}`)
      })
    })

    cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})
  })

  describe('from the org view', () => {
    it('can create a telegraf config', () => {
      const newConfig = 'New Config'
      const configDescription = 'This is a new config testing'

      cy.getByTestID('table-row').should('have.length', 0)
      cy.contains('Create Configuration').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByTestID('telegraf-plugins--System').click()
        cy.getByTestID('next').click()
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

      cy.fixture('user').then(({bucket}) => {
        cy.getByTestID('resource-card')
          .should('have.length', 1)
          .and('contain', newConfig)
          .and('contain', bucket)
      })
    })

    it('allows the user to view just the output', () => {
      const bucketz = ['MO_buckets', 'EZ_buckets', 'Bucky']
      const [firstBucket, secondBucket, thirdBucket] = bucketz

      cy.get<Organization>('@org').then(({id, name}: Organization) => {
        cy.createBucket(id, name, firstBucket)
        cy.createBucket(id, name, secondBucket)
        cy.createBucket(id, name, thirdBucket)
      })

      cy.reload()
      cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})

      cy.getByTestID('button--output-only').click()
      cy.getByTestID('overlay--container')
        .should('be.visible')
        .within(() => {
          const buckets = bucketz.slice(0).sort((a, b) => {
            const _a = a.toLowerCase()
            const _b = b.toLowerCase()
            return _a > _b ? 1 : _a < _b ? -1 : 0
          })

          cy.get('code').should($el => {
            const text = $el.text()

            expect(text.includes('[[outputs.influxdb_v2]]')).to.be.true
            //expect a default sort to be applied
            expect(text.includes(`bucket = "${buckets[0]}"`)).to.be.true
          })

          cy.getByTestID('bucket-dropdown').within(() => {
            cy.getByTestID('bucket-dropdown--button').click()
            cy.getByTestID('dropdown-item')
              .eq(2)
              .click()
          })

          cy.get('code').should($el => {
            const text = $el.text()

            // NOTE: this index is off because there is a default
            // defbuck bucket in there (alex)
            expect(text.includes(`bucket = "${buckets[1]}"`)).to.be.true
          })
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
        cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})
      })

      it('can update configuration name', () => {
        const newConfigName = 'This is new name'

        cy.getByTestID('collector-card--name')
          .first()
          .trigger('mouseover')
        cy.getByTestID('collector-card--name-button')
          .first()
          .click()
        cy.getByTestID('collector-card--input')
          .type(newConfigName)
          .type('{enter}')
        cy.getByTestID('collector-card--name').should('contain', newConfigName)

        cy.getByTestID('resource-card').should('have.length', 1)
      })

      it('can view setup instructions & config text', () => {
        cy.getByTestID('resource-card').should('have.length', 1)

        cy.getByTestID('setup-instructions-link').click()

        cy.getByTestID('setup-instructions').should('exist')

        cy.getByTestID('overlay--header')
          .find('button')
          .click()

        cy.getByTestID('setup-instructions').should('not.exist')

        cy.getByTestID('collector-card--name').click()

        cy.getByTestID('toml-editor').should('exist')
      })

      it('can delete a label from config', () => {
        cy.getByTestID('resource-card').should('have.length', 1)

        cy.getByTestID('inline-labels--add').click()
        cy.getByTestID('inline-labels--popover-field').type('zoe')
        cy.getByTestID('inline-labels--create-new').click()
        cy.getByTestID('overlay--container').should('exist')
        cy.getByTestID('create-label-form--submit').click()
        cy.getByTestID('label--pill zoe').should('exist')
        cy.getByTestID('label--pill--delete zoe').click({force: true})

        cy.getByTestID('inline-labels--empty').should('exist')
      })

      it('can delete a config', () => {
        cy.getByTestID('resource-card').should('have.length', 1)
        cy.getByTestID('telegraf-delete-menu').click({force: true})
        cy.getByTestID('telegraf-delete-button').click()
        cy.getByTestID('empty-state').should('exist')
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
        cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})
      })
      // filter by name
      it('can filter telegraf configs and sort by name', () => {
        // fixes https://github.com/influxdata/influxdb/issues/15246
        cy.getByTestID('search-widget').type(firstTelegraf)
        cy.getByTestID('resource-card').should('have.length', 1)
        cy.getByTestID('resource-card').should('contain', firstTelegraf)

        cy.getByTestID('search-widget')
          .clear()
          .type(secondTelegraf)
        cy.getByTestID('resource-card').should('have.length', 1)
        cy.getByTestID('resource-card').should('contain', secondTelegraf)

        cy.getByTestID('search-widget')
          .clear()
          .type(thirdTelegraf)
        cy.getByTestID('resource-card').should('have.length', 1)
        cy.getByTestID('resource-card').should('contain', thirdTelegraf)

        cy.getByTestID('search-widget')
          .clear()
          .type('should have no results')
        cy.getByTestID('resource-card').should('have.length', 0)
        cy.getByTestID('empty-state').should('exist')

        cy.getByTestID('search-widget')
          .clear()
          .type('a')
        cy.getByTestID('resource-card').should('have.length', 2)
        cy.getByTestID('resource-card').should('contain', firstTelegraf)
        cy.getByTestID('resource-card').should('contain', secondTelegraf)
        cy.getByTestID('resource-card').should('not.contain', thirdTelegraf)

        // sort by buckets test here
        cy.reload() // clear out filtering state from the previous test
        cy.get('[data-testid="resource-list--body"]', {timeout: PAGE_LOAD_SLA})

        cy.getByTestID('collector-card--name').should('have.length', 3)

        // test to see if telegrafs are initially sorted by name
        telegrafs.sort()

        cy.wait(0).then(() => {
          // NOTE: this then is just here to let me scope this variable (alex)
          const teletubbies = telegrafs.slice(0).sort()
          cy.getByTestID('collector-card--name').each((val, index) => {
            expect(val.text()).to.include(teletubbies[index])
          })
        })

        cy.getByTestID('resource-sorter--button')
          .click()
          .then(() => {
            cy.getByTestID('resource-sorter--name-desc').click()
          })
          .then(() => {
            // NOTE: this then is just here to let me scope this variable (alex)
            const teletubbies = telegrafs
              .slice(0)
              .sort()
              .reverse()
            cy.getByTestID('collector-card--name').each((val, index) => {
              expect(val.text()).to.include(teletubbies[index])
            })
          })
      })
    })
  })

  describe('configuring plugins', () => {
    // fix for https://github.com/influxdata/influxdb/issues/15500
    describe('configuring nginx', () => {
      beforeEach(() => {
        // These clicks launch move through configuration modals rather than navigate to new pages
        cy.contains('Create Configuration')
          .click()
          .then(() => {
            cy.contains('NGINX')
              .click()
              .then(() => {
                cy.contains('Continue')
                  .click()
                  .then(() => {
                    cy.contains('nginx').click()
                  })
              })
          })
      })

      it('can add and delete urls', () => {
        cy.getByTestID('input-field').type('http://localhost:8086')
        cy.contains('Add').click()

        cy.contains('http://localhost:8086').should('exist', () => {
          cy.getByTestID('input-field').type('http://example.com')
          cy.contains('Add').click()

          cy.contains('http://example.com')
            .should('exist')
            .then($example => {
              $example.contains('Delete').click()
              $example.contains('Confirm').click()

              cy.contains('http://example').should('not.exist')

              cy.contains('Done')
                .click()
                .then(() => {
                  cy.get('.cf-icon.checkmark').should('exist')
                })
            })
        })
      })

      it('handles busted input', () => {
        // do nothing when clicking done with no urls
        cy.contains('Done').click()
        cy.contains('nginx').should('exist')
        cy.get('.cf-icon.circle-thick').should('exist')

        cy.contains('nginx').click()
        cy.getByTestID('input-field').type('youre mom')
        cy.contains('Add').click()
        cy.contains('Done').click()
        cy.get('.cf-icon.remove').should('exist')
      })
    })

    // redis was affected by the change that was written to address https://github.com/influxdata/influxdb/issues/15500
    describe('configuring redis', () => {
      beforeEach(() => {
        // These clicks launch move through configuration modals rather than navigate to new pages
        cy.contains('Create Configuration')
          .click()
          .then(() => {
            cy.contains('Redis')
              .click()
              .then(() => {
                cy.contains('Continue')
                  .click()
                  .then(() => {
                    cy.contains('redis').click()
                  })
              })
          })
      })

      it('can add and delete urls', () => {
        cy.get('input[title="servers"]').type('michael collins')
        cy.contains('Add').click()

        cy.contains('michael collins').should('exist', () => {
          cy.get('input[title="servers"]').type('alan bean')
          cy.contains('Add').click()

          cy.contains('alan bean')
            .should('exist')
            .then($server => {
              $server.contains('Delete').click()
              $server.contains('Confirm').click()
              cy.contains('alan bean').should('not.exist')

              cy.contains('Done').click()
              cy.get('.cf-icon.checkmark').should('exist')
            })
        })
      })

      it('does nothing when clicking done with no urls', () => {
        cy.contains('Done')
          .click()
          .then(() => {
            cy.contains('redis').should('exist')
          })
      })
    })

    // fix for https://github.com/influxdata/influxdb/issues/15730
    it('creates a configuration with a unique label and opens it', () => {
      cy.contains('Create Configuration').click()

      cy.contains('Docker').click()

      cy.contains('Continue').click()

      cy.contains('docker').click()

      cy.get('[name="endpoint"]').type('http://localhost')

      cy.contains('Done').click()
      cy.get('input[title="Telegraf Configuration Name"]').type(
        '{selectall}Label 1'
      )
      cy.get('input[title="Telegraf Configuration Description"]').type(
        'Description 1'
      )

      cy.contains('Create and Verify').click()
      cy.contains('Finish').click()
      cy.contains('Your configurations have been saved')

      cy.contains('Label 1').click()
      cy.contains('Telegraf Configuration - Label 1').should('exist')
    })

    describe('Label creation and searching', () => {
      beforeEach(() => {
        const description = 'Config Description'
        cy.get('@org').then(({id}: Organization) => {
          cy.createTelegraf('newteleg', description, id, 'newbucket')
        })
        cy.reload()
      })

      it('Can add label', () => {
        cy.getByTestID('inline-labels--add').click()
        cy.getByTestID('inline-labels--popover-field').type('zoe')
        cy.getByTestID('inline-labels--create-new').click()
        cy.getByTestID('overlay--container').should('exist')
        cy.getByTestID('create-label-form--submit').click()
        cy.getByTestID('label--pill zoe').should('exist')
        // search by label
        cy.getByTestID('search-widget')
          .should('be.visible')
          .clear()
          .type('zoe')

        cy.getByTestID('resource-card').should('have.length', 1)
        cy.getByTestID('resource-card').should('contain', 'newteleg')
      })
    })
  })
})

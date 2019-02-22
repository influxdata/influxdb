import {Bucket} from '@influxdata/influx'

describe('Buckets', () => {
  beforeEach(() => {
    cy.flush()

    cy.setupUser().then(({body}) => {
      const {
        org: {id},
        bucket,
      } = body
      cy.wrap(bucket).as('bucket')

      cy.signin(id)

      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/buckets_tab`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a bucket', () => {
      const newBucket = '🅱️ucket'
      cy.getByDataTest('table-row').should('have.length', 1)

      cy.contains('Create').click()
      cy.getByDataTest('overlay--container').within(() => {
        cy.getByInputName('name').type(newBucket)
        cy.get('.button')
          .contains('Create')
          .click()
      })

      cy.getByDataTest('table-row')
        .should('have.length', 2)
        .and('contain', newBucket)
    })

    it('can update a buckets name and retention rules', () => {
      const newName = 'newdefbuck'

      cy.get<Bucket>('@bucket').then(({name}) => {
        cy.contains(name).click()
      })

      cy.getByDataTest('retention-intervals').click()

      cy.getByInputName('days').type('{uparrow}')
      cy.getByInputName('hours').type('{uparrow}')
      cy.getByInputName('minutes').type('{uparrow}')
      cy.getByInputName('seconds').type('{uparrow}')

      cy.getByDataTest('overlay--container').within(() => {
        cy.getByInputName('name')
          .clear()
          .type(newName)

        cy.contains('Save').click()
      })

      cy.getByDataTest('table-row')
        .should('contain', '1 day')
        .and('contain', newName)
    })
  })
})

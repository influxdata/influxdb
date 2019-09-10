import {Bucket, Organization} from '../../src/types'

describe('Buckets', () => {
  beforeEach(() => {
    cy.flush()

    cy.signin().then(({body}) => {
      const {
        org: {id},
        bucket,
      } = body
      cy.wrap(body.org).as('org')
      cy.wrap(bucket).as('bucket')
      cy.fixture('routes').then(({orgs}) => {
        cy.visit(`${orgs}/${id}/load-data/buckets`)
      })
    })
  })

  describe('from the org view', () => {
    it('can create a bucket', () => {
      const newBucket = '🅱️ucket'
      cy.getByTestID('bucket--card').should('have.length', 1)

      cy.getByTestID('Create Bucket').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('name').type(newBucket)
        cy.get('.cf-button')
          .contains('Create')
          .click()
      })

      cy.getByTestID('bucket--card')
        .should('have.length', 2)
        .and('contain', newBucket)
    })

    it.only("can update a bucket's retention rules", () => {
      cy.get<Bucket>('@bucket').then(({name}: Bucket) => {
        cy.getByTestID(`bucket--card ${name}`).click()
      })

      cy.getByTestID('retention-intervals--button').click()
      cy.getByTestID('duration-selector--button').click()
      cy.getByTestID('duration-selector--7d').click()

      cy.getByTestID('overlay--container').within(() => {
        cy.contains('Save').click()
      })

      cy.getByTestID('bucket--card').should('contain', '7 days')

      cy.get<Bucket>('@bucket').then(({name}: Bucket) => {
        cy.getByTestID(`bucket--card ${name}`).click()
      })

      cy.getByTestID('retention-never--button').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.contains('Save').click()
      })

      cy.getByTestID('overlay--container').should('not.be.visible')
    })

    it.skip('can delete a bucket', () => {
      cy.get<Organization>('@org').then(({id, name}: Organization) => {
        cy.createBucket(id, name, 'newbucket1')
        cy.createBucket(id, name, 'newbucket2')
      })

      cy.getByTestID('bucket--card').should('have.length', 3)

      cy.getByTestID('confirmation-button')
        .last()
        .click({force: true})

      cy.getByTestID('bucket--card').should('have.length', 2)
    })
  })
})

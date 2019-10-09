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

  describe('from the buckets index page', () => {
    it('can create a bucket', () => {
      const newBucket = '🅱️ucket'
      cy.getByTestID(`bucket--card ${newBucket}`).should('not.exist')

      cy.getByTestID('Create Bucket').click()
      cy.getByTestID('overlay--container').within(() => {
        cy.getByInputName('name').type(newBucket)
        cy.get('.cf-button')
          .contains('Create')
          .click()
      })

      cy.getByTestID(`bucket--card ${newBucket}`).should('exist')
    })

    it("can update a bucket's retention rules", () => {
      cy.get<Bucket>('@bucket').then(({name}: Bucket) => {
        cy.getByTestID(`bucket--card--name ${name}`).click()
        cy.getByTestID(`bucket--card ${name}`).should('not.contain', '7 days')
      })

      cy.getByTestID('retention-intervals--button').click()
      cy.getByTestID('duration-selector--button').click()
      cy.getByTestID('duration-selector--7d').click()

      cy.getByTestID('overlay--container').within(() => {
        cy.contains('Save').click()
      })

      cy.get<Bucket>('@bucket').then(({name}: Bucket) => {
        cy.getByTestID(`bucket--card ${name}`).should('contain', '7 days')
      })
    })

    it('can delete a bucket', () => {
      const bucket1 = 'newbucket1'
      const bucket2 = 'newbucket2'
      cy.get<Organization>('@org').then(({id, name}: Organization) => {
        cy.createBucket(id, name, bucket1)
        cy.createBucket(id, name, bucket2)
      })

      cy.getByTestID(`bucket--card--name ${bucket1}`).should('exist')

      cy.getByTestID(`context-delete-menu ${bucket1}`).click()
      cy.getByTestID(`context-delete-bucket ${bucket1}`).click()

      cy.getByTestID(`bucket--card--name ${bucket1}`).should('not.exist')
    })
  })

  describe('Routing directly to the edit overlay', () => {
    it('reroutes to buckets view if bucket does not exist', () => {
      cy.get<Organization>('@org').then(({id}: Organization) => {
        cy.fixture('routes').then(({orgs}) => {
          const idThatDoesntExist = '261234d1a7f932e4'
          cy.visit(`${orgs}/${id}/load-data/buckets/${idThatDoesntExist}/edit`)
          cy.location('pathname').should(
            'be',
            `${orgs}/${id}/load-data/buckets/`
          )
        })
      })
    })

    it('displays overlay if bucket does exist', () => {
      cy.get<Organization>('@org').then(({id: orgID}: Organization) => {
        cy.fixture('routes').then(({orgs}) => {
          cy.get<Bucket>('@bucket').then(({id: bucketID}: Bucket) => {
            cy.visit(`${orgs}/${orgID}/load-data/buckets/${bucketID}/edit`)
            cy.location('pathname').should(
              'be',
              `${orgs}/${orgID}/load-data/buckets/${bucketID}/edit`
            )
          })
          cy.getByTestID(`overlay`).should('exist')
        })
      })
    })
  })
})

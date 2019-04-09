interface TestUser {
  username: string
  password: string
  org: string
  bucket: string
}

describe('Onboarding', () => {
  let user: TestUser

  beforeEach(() => {
    cy.flush()

    cy.fixture('user').then(u => {
      user = u
    })

    cy.visit('/')
  })

  it('Can Onboard to Quick Start', () => {
    cy.server()

    //Will want to capture response from this
    cy.route('POST', 'api/v2/setup').as('orgSetup')

    //Check splash page
    cy.location('pathname').should('include', 'onboarding/0')
    cy.getByTestID('init-step--head-main').contains('Welcome to InfluxDB 2.0')
    cy.getByTestID('credits').contains('Powered by')
    cy.getByTestID('credits').contains('InfluxData')

    //Continue
    cy.getByTestID('onboarding-get-started').click()

    cy.location('pathname').should('include', 'onboarding/1')

    cy.location('pathname').should('include', 'onboarding/1')

    //Check navigation bar
    cy.getByTestID('nav-step--welcome').click()

    //Check splash page
    cy.getByTestID('init-step--head-main').contains('Welcome to InfluxDB 2.0')
    cy.getByTestID('credits').contains('Powered by')
    cy.getByTestID('credits').contains('InfluxData')

    //Continue
    cy.getByTestID('onboarding-get-started').click()

    //Check onboarding page - nav bar
    cy.getByTestID('nav-step--welcome').contains('Welcome')
    cy.getByTestID('nav-step--welcome')
      .parent()
      .children('span')
      .should($span => {
        expect($span).to.have.class('checkmark')
      })

    cy.getByTestID('nav-step--setup')
      .contains('Initial User Setup')
      .should('be.visible')
    cy.getByTestID('nav-step--setup').should('have.class', 'current')
    cy.getByTestID('nav-step--setup')
      .parent()
      .children('span')
      .children('span')
      .should($span => {
        expect($span)
          .to.have.class('icon')
          .and.to.have.class('circle-thick')
      })

    cy.getByTestID('nav-step--complete')
      .parent()
      .should($el => {
        expect($el).to.have.class('unclickable')
      })

    //Check onboarding page headers and controls
    cy.getByTestID('admin-step--head-main').contains('Setup Initial User')

    cy.getByTestID('next').should('be.disabled')

    cy.getByTestID('next')
      .children('.button--label')
      .contains('Continue')

    //Check tooltips
    cy.getByTestID('form-elem--orgname')
      .children('[data-testid=form--label]')
      .children('div.question-mark-tooltip')
      .trigger('mouseenter')
      .children('div[data-id=tooltip]')
      .contains('An organization is')
      .should($tt => {
        expect($tt).to.have.class('show')
      })

    cy.getByTestID('form-elem--bucketname')
      .children('[data-testid=form--label]')
      .children('div.question-mark-tooltip')
      .trigger('mouseenter')
      .children('div[data-id=tooltip]')
      .contains('A bucket is')
      .should($tt => {
        expect($tt).to.have.class('show')
      })

    //Input fields
    cy.getByTestID('input-field--username').type(user.username)
    cy.getByTestID('input-field--password').type(user.password)
    cy.getByTestID('input-field--password-chk').type(user.password)
    cy.getByTestID('input-field--orgname').type(user.org)
    cy.getByTestID('input-field--bucketname').type(user.bucket)

    cy.getByTestID('next')
      .children('.button--label')
      .contains('Continue')

    cy.getByTestID('next')
      .should('be.enabled')
      .click()

    cy.wait('@orgSetup')

    cy.get('@orgSetup').then(xhr => {
      let orgId: string = xhr.responseBody.org.id

      //wait for new page to load
      cy.location('pathname').should('include', 'onboarding/2')

      cy.getByTestID('notification-success').should($msg => {
        expect($msg).to.contain(
          'Initial user details have been successfully set'
        )
      })

      cy.getByTestID('notification-success')
        .should('be.visible')
        .children('button.notification-close')
        .should('be.visible')

      //check navbar
      cy.getByTestID('nav-step--complete').should('have.class', 'current')

      cy.getByTestID('nav-step--welcome').should('have.class', 'checkmark')
      cy.getByTestID('nav-step--setup').should('have.class', 'checkmark')

      cy.getByTestID('button--advanced').should('be.visible')

      cy.getByTestID('button--conf-later').should('be.visible')

      //advance to Quick Start
      cy.getByTestID('button--quick-start').click()

      cy.location('pathname').should('equal', '/orgs/' + orgId)

      cy.getByTestID('notification-success').should($msg => {
        expect($msg).to.contain(
          'The InfluxDB Scraper has been configured for http://localhost:9999/metrics'
        )
      })

      cy.getByTestID('notification-success')
        .eq(0)
        .should('be.visible')
        .children('button.notification-close')
        .click()

      cy.getByTestID('notification-success')
        .eq(1)
        .should('be.visible')
        .children('button.notification-close')
        .click()

      cy.getByTestID('notification-success')
        .eq(0)
        .should('not.exist')
    })
  })

  it('Can onboard to advanced', () => {
    cy.server()

    cy.route('POST', 'api/v2/setup').as('orgSetup')

    //Check splash page
    cy.location('pathname').should('include', 'onboarding/0')

    //Continue
    cy.getByTestID('onboarding-get-started').click()
    cy.location('pathname').should('include', 'onboarding/1')

    //Input fields
    cy.getByTestID('input-field--username').type(user.username)
    cy.getByTestID('input-field--password').type(user.password)
    cy.getByTestID('input-field--password-chk').type(user.password)
    cy.getByTestID('input-field--orgname').type(user.org)
    cy.getByTestID('input-field--bucketname').type(user.bucket)

    cy.getByTestID('next').click()

    cy.wait('@orgSetup')

    cy.get('@orgSetup').then(xhr => {
      let orgId: string = xhr.responseBody.org.id

      //wait for new page to load
      cy.location('pathname').should('include', 'onboarding/2')

      //advance to Advanced
      cy.getByTestID('button--advanced').click()

      //wait for new page to load
      cy.location('pathname').should('match', /orgs\/.*\/buckets/)

      cy.location('pathname').should('include', orgId)
    })
  })

  it('Can onboard to configure later', () => {
    cy.server()

    cy.route('POST', 'api/v2/setup').as('orgSetup')

    //Check splash page
    cy.location('pathname').should('include', 'onboarding/0')

    //Continue
    cy.getByTestID('onboarding-get-started').click()
    cy.location('pathname').should('include', 'onboarding/1')

    //Input fields
    cy.getByTestID('input-field--username').type(user.username)
    cy.getByTestID('input-field--password').type(user.password)
    cy.getByTestID('input-field--password-chk').type(user.password)
    cy.getByTestID('input-field--orgname').type(user.org)
    cy.getByTestID('input-field--bucketname').type(user.bucket)


    cy.getByTestID('next').click()

    cy.wait('@orgSetup')

    cy.get('@orgSetup').then(xhr => {
      let orgId: string = xhr.responseBody.org.id
      //wait for new page to load

      cy.location('pathname').should('include', 'onboarding/2')

      //advance to Advanced
      cy.getByTestID('button--conf-later').click()

      cy.location('pathname').should('include', orgId)
    })
  })

  it('respects field requirements', () => {
    //Continue
    cy.getByTestID('onboarding-get-started').click()

    cy.getByTestID('input-field--username').type(user.username)

    cy.getByTestID('next')
      .should('be.disabled')
      .children('.button--label')
      .contains('Continue')

    cy.getByTestID('input-field--password').type(user.password)

    cy.getByTestID('next')
      .should('be.disabled')
      .children('.button--label')
      .contains('Continue')

    cy.getByTestID('input-field--password-chk').type('drowssap')

    //check password mismatch
    cy.getByTestID('form--element-error').should(
      'have.text',
      'Passwords do not match'
    )

    cy.getByTestID('input-error').should('have.class', 'alert-triangle')

    cy.getByTestID('input-field--orgname').type(user.org)
    cy.getByTestID('input-field--bucketname').type(user.bucket)

    cy.getByTestID('next')
      .should('be.disabled')
      .children('.button--label')
      .contains('Continue')

    cy.getByTestID('input-field--password-chk')
      .clear()
      .type(user.password)

    cy.getByTestID('input-error').should('not.exist')

    cy.getByTestID('next')
      .should('be.enabled')
      .children('.button--label')
      .contains('Continue')

    //check cleared username
    cy.getByTestID('input-field--username').clear()

    cy.getByTestID('next')
      .should('be.disabled')
      .children('.button--label')
      .contains('Continue')

    cy.getByTestID('input-field--username').type(user.username)

    cy.getByTestID('next')
      .should('be.enabled')
      .children('.button--label')
      .contains('Continue')

    //check cleared password
    cy.getByTestID('input-field--password').clear()

    cy.getByTestID('form--element-error').should(
      'have.text',
      'Passwords do not match'
    )

    cy.getByTestID('next')
      .should('be.disabled')
      .children('.button--label')
      .contains('Continue')

    cy.getByTestID('input-field--password').type(user.password)

    cy.getByTestID('next')
      .should('be.enabled')
      .children('.button--label')
      .contains('Continue')

    //check cleared org name
    cy.getByTestID('input-field--orgname').clear()

    cy.getByTestID('next')
      .should('be.disabled')
      .children('.button--label')
      .contains('Continue')

    cy.getByTestID('input-field--orgname').type(user.org)

    cy.getByTestID('next')
      .should('be.enabled')
      .children('.button--label')
      .contains('Continue')

    //check cleared bucket name
    cy.getByTestID('input-field--bucketname').clear()

    cy.getByTestID('next')
      .should('be.disabled')
      .children('.button--label')
      .contains('Continue')

    cy.getByTestID('input-field--bucketname').type(user.bucket)

    cy.getByTestID('next')
      .should('be.enabled')
      .children('.button--label')
      .contains('Continue')

  })

  it('Can onboard to advanced', () => {

    cy.server()

    cy.route('POST', 'api/v2/setup').as('orgSetup')

    //Check splash page
    cy.location('pathname').should('include', 'onboarding/0')

    //Continue
    cy.getByTestID('button').click()
    cy.location('pathname').should('include', 'onboarding/1')

    //Input fields
    cy.getByTestID('input-field')
      .eq(0)
      .type(user.username)
    cy.getByTestID('input-field')
      .eq(1)
      .type(user.password)
    cy.getByTestID('input-field')
      .eq(2)
      .type(user.password)
    cy.getByTestID('input-field')
      .eq(3)
      .type(user.org)
    cy.getByTestID('input-field')
      .eq(4)
      .type(user.bucket)

    cy.getByTestID('button').click()

    cy.wait('@orgSetup')

    cy.get('@orgSetup').then(xhr => {
      let orgId: string = xhr.responseBody.org.id

      //wait for new page to load
      cy.location('pathname').should('include', 'onboarding/2')

      //advance to Advanced
      cy.getByTestID('button')
        .eq(1)
        .contains('Advanced')
        .click()

      //wait for new page to load
      cy.location('pathname').should('match', /orgs\/.*\/buckets/)

      cy.location('pathname').should('include', orgId)
    })
  })

  it('Can onboard to configure later', () => {
    cy.server()

    cy.route('POST', 'api/v2/setup').as('orgSetup')

    //Check splash page
    cy.location('pathname').should('include', 'onboarding/0')

    //Continue
    cy.getByTestID('button').click()
    cy.location('pathname').should('include', 'onboarding/1')

    //Input fields
    cy.getByTestID('input-field')
      .eq(0)
      .type(user.username)
    cy.getByTestID('input-field')
      .eq(1)
      .type(user.password)
    cy.getByTestID('input-field')
      .eq(2)
      .type(user.password)
    cy.getByTestID('input-field')
      .eq(3)
      .type(user.org)
    cy.getByTestID('input-field')
      .eq(4)
      .type(user.bucket)

    cy.getByTestID('button').click()

    cy.wait('@orgSetup')

    cy.get('@orgSetup').then(xhr => {
      let orgId: string = xhr.responseBody.org.id
      //wait for new page to load

      cy.location('pathname').should('include', 'onboarding/2')

      //advance to Advanced
      cy.getByTestID('button')
        .eq(2)
        .contains('Configure Later')
        .click()

      cy.location('pathname').should('include', orgId)
    })
  })

  it('respects field requirements', () => {
    //Continue
    cy.getByTestID('button').click()

    cy.getByTestID('input-field')
      .eq(0)
      .type(user.username)

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.disabled')

    cy.getByTestID('input-field')
      .eq(1)
      .type(user.password)

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.disabled')

    cy.getByTestID('input-field')
      .eq(2)
      .type('drowssap')

    //check password mismatch
    cy.getByTestID('form--element-error').should(
      'have.text',
      'Passwords do not match'
    )

    cy.getByTestID('input-error').should('have.class', 'alert-triangle')

    cy.getByTestID('input-field')
      .eq(3)
      .type(user.org)
    cy.getByTestID('input-field')
      .eq(4)
      .type(user.bucket)

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.disabled')

    cy.getByTestID('input-field')
      .eq(2)
      .clear()
      .type(user.password)

    cy.getByTestID('input-error').should('not.exist')

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.enabled')

    //check cleared username
    cy.getByTestID('input-field')
      .eq(0)
      .clear()

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.disabled')

    cy.getByTestID('input-field')
      .eq(0)
      .type(user.username)

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.enabled')

    //check cleared password
    cy.getByTestID('input-field')
      .eq(1)
      .clear()

    cy.getByTestID('form--element-error').should(
      'have.text',
      'Passwords do not match'
    )

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.disabled')

    cy.getByTestID('input-field')
      .eq(1)
      .type(user.password)

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.enabled')

    //check cleared org name
    cy.getByTestID('input-field')
      .eq(3)
      .clear()

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.disabled')

    cy.getByTestID('input-field')
      .eq(3)
      .type(user.org)

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.enabled')

    //check cleared bucket name
    cy.getByTestID('input-field')
      .eq(4)
      .clear()

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.disabled')

    cy.getByTestID('input-field')
      .eq(4)
      .type(user.bucket)

    cy.getByTestID('button')
      .contains('Continue')
      .should('be.enabled')
  })
})



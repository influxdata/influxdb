interface TestUser {
  username: string
  password: string
  org: string
  bucket: string
}

const defTimeOut = 3000

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
    //Check splash page
    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/0')
    cy.get('h3.wizard-step--title').contains('Welcome to InfluxDB 2.0')
    cy.get('div.wizard--credits').contains('Powered by')
    cy.get('div.wizard--credits').contains('InfluxData')

    //Continue
    cy.get("button[title='Get Started']").click()

    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/1')
    //Check navigation bar
    cy.get("div.wizard--progress-title.checkmark:contains('Welcome')").click()

    //Check splash page
    cy.get('h3.wizard-step--title').contains('Welcome to InfluxDB 2.0')
    cy.get('div.wizard--credits').contains('Powered by')
    cy.get('div.wizard--credits').contains('InfluxData')

    //Continue
    cy.get("button[title='Get Started']").click()

    //Check onboarding page - nav bar
    cy.get('div.wizard--progress-title.current').contains('Initial User Setup')
    cy.get('span.wizard--progress-icon.current > span').should($span => {
      expect($span)
        .to.have.class('icon')
        .and.to.have.class('circle-thick')
    })
    cy.get('div.wizard--progress-title:contains("Complete")')
      .parent()
      .should($el => {
        expect($el).to.have.class('unclickable')
      })

    //Check onboarding page headers and controls
    cy.get('h3.wizard-step--title').contains('Setup Initial User')

    cy.get('button.button-primary')
      .contains('Continue')
      .should('be.disabled')

    //Check tooltips
    cy.get('label:contains("Initial Organization Name")')
      .children('div.question-mark-tooltip')
      .trigger('mouseenter')

    cy.get('div#admin_org_tooltip-tooltip')
      .contains(
        'An organization is a workspace for a group of users requiring access to time series data, dashboards, and other resources.\n' +
          '        You can create organizations for different functional groups, teams, or projects.'
      )
      .should($tt => {
        expect($tt).to.have.class('show')
      })

    cy.get('label:contains("Initial Organization Name")')
      .children('div.question-mark-tooltip')
      .trigger('mouseleave')

    cy.get('div#admin_org_tooltip-tooltip').should($tt => {
      expect($tt).to.not.have.class('show')
    })

    cy.get('label:contains("Initial Bucket Name")')
      .children('div.question-mark-tooltip')
      .trigger('mouseenter')

    cy.get('div#admin_bucket_tooltip-tooltip')
      .contains(
        'A bucket is where your time series data is stored with a retention policy.'
      )
      .should($tt => {
        expect($tt).to.have.class('show')
      })

    cy.get('label:contains("Initial Bucket Name")')
      .children('div.question-mark-tooltip')
      .trigger('mouseleave')

    cy.get('div.influx-tooltip#admin_bucket_tooltip-tooltip').should($tt => {
      expect($tt).to.not.have.class('show')
    })

    //Input fields
    cy.get('input[title=Username]').type(user.username)
    cy.get('input[title=Password]').type(user.password)
    cy.get('input[title="Confirm Password"]').type(user.password)
    cy.get('input[title="Initial Organization Name"]').type(user.org)
    cy.get('input[title="Initial Bucket Name"]').type(user.bucket)

    cy.get('button.button-primary')
      .contains('Continue')
      .should('be.enabled')
    cy.get('button:contains("Continue")').click()

    //wait for new page to load
    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/2')

    cy.getByTestID('notification-success').should($msg => {
      expect($msg).to.contain('Initial user details have been successfully set')
    })

    cy.get('div[data-testid=notification-success] span.icon.checkmark').should(
      'be.visible'
    )
    cy.get(
      'div[data-testid=notification-success] button.notification-close'
    ).should('be.visible')

    //check navbar
    cy.get('div.wizard--progress-title.current').should($div => {
      expect($div).to.contain('Complete')
    })
    cy.get('div.wizard--progress-title.checkmark').should($div => {
      expect($div).to.contain('Welcome')
      expect($div).to.contain('Initial User Setup')
    })

    cy.get('button.button-success')
      .contains('Quick Start')
      .should('be.visible')
    cy.get('button.button-success')
      .contains('Advanced')
      .should('be.visible')
    cy.get('button.button-success')
      .contains('Configure Later')
      .should('be.visible')

    //advance to Quick Start
    cy.get('button.button-success')
      .contains('Quick Start')
      .click()

    cy.location('pathname', {timeout: defTimeOut}).should('include', '/me')

    cy.getByTestID('notification-success').should($msg => {
      expect($msg).to.contain(
        'The InfluxDB Scraper has been configured for http://localhost:9999/metrics'
      )
    })

    cy.get('div[data-testid=notification-success] span.icon.checkmark').should(
      'be.visible'
    )

    cy.get(
      'div[data-testid=notification-success] button.notification-close'
    ).should('be.visible')
  })

  it('Can onboard to advanced', () => {

    //Check splash page
    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/0')

    //Continue
    cy.get("button[title='Get Started']").click()
    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/1')

    //Input fields
    cy.get('input[title=Username]').type(user.username)
    cy.get('input[title=Password]').type(user.password)
    cy.get('input[title="Confirm Password"]').type(user.password)
    cy.get('input[title="Initial Organization Name"]').type(user.org)
    cy.get('input[title="Initial Bucket Name"]').type(user.bucket)

    cy.get('button:contains("Continue")').click()

    //wait for new page to load
    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/2')

    //advance to Advanced
    cy.get('button.button-success')
      .contains('Advanced')
      .click()

    //wait for new page to load
    cy.location('pathname', {timeout: defTimeOut}).should('match', /organizations\/.*\/buckets/)

  })

  it('Can onboard to configure later', () => {
    //Check splash page
    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/0')

    //Continue
    cy.get("button[title='Get Started']").click()
    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/1')

    //Input fields
    cy.get('input[title=Username]').type(user.username)
    cy.get('input[title=Password]').type(user.password)
    cy.get('input[title="Confirm Password"]').type(user.password)
    cy.get('input[title="Initial Organization Name"]').type(user.org)
    cy.get('input[title="Initial Bucket Name"]').type(user.bucket)

    cy.get('button:contains("Continue")').click()

    //wait for new page to load
    cy.location('pathname', {timeout: defTimeOut}).should('include', 'onboarding/2')

    //advance to Advanced
    cy.get('button.button-success')
      .contains('Configure Later')
      .click()

    cy.location('pathname', {timeout: defTimeOut}).should('include', '/me')

  })
})

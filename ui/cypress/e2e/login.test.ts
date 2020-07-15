interface LoginUser {
  username: string
  password: string
}

describe('The Login Page', () => {
  let user: LoginUser
  beforeEach(() => {
    cy.flush()

    cy.fixture('user').then(u => {
      user = u
    })

    cy.setupUser().then(({body}) => {
      cy.wrap(body.org.id).as('orgID')
    })

    cy.visit('/')
  })

  it('can login and logout', () => {
    cy.getByInputName('username').type(user.username)
    cy.getByInputName('password').type(user.password)
    cy.get('button[type=submit]').click()

    cy.getByTestID('tree-nav').should('exist')

    cy.getByTestID('logout--button').click()

    cy.getByTestID('signin-page--content').should('exist')

    // try to access a protected route
    cy.get<string>('@orgID').then(orgID => {
      cy.visit(`/orgs/${orgID}`)
    })

    // assert that user is routed to signin
    cy.getByTestID('signin-page--content').should('exist')
  })

  describe('login failure', () => {
    it('if username is not present', () => {
      cy.getByInputName('password').type(user.password)
      cy.get('button[type=submit]').click()

      cy.getByTestID('notification-error').should('exist')
    })

    it('if password is not present', () => {
      cy.getByInputName('username').type(user.username)
      cy.get('button[type=submit]').click()

      cy.getByTestID('notification-error').should('exist')
    })

    it('if username is incorrect', () => {
      cy.getByInputName('username').type('not-a-user')
      cy.getByInputName('password').type(user.password)
      cy.get('button[type=submit]').click()

      cy.getByTestID('notification-error').should('exist')
    })

    it('if password is incorrect', () => {
      cy.getByInputName('username').type(user.username)
      cy.getByInputName('password').type('not-a-password')
      cy.get('button[type=submit]').click()

      cy.getByTestID('notification-error').should('exist')
    })
  })
})

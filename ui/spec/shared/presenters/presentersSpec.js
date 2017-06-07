import {buildRoles, buildClusterAccounts} from 'shared/presenters'

describe('Presenters', function() {
  describe('roles utils', function() {
    describe('buildRoles', function() {
      describe('when a role has no users', function() {
        it("sets a role's users as an empty array", function() {
          const roles = [
            {
              name: 'Marketing',
              permissions: {
                '': ['ViewAdmin'],
              },
            },
          ]

          const actual = buildRoles(roles)

          expect(actual[0].users).to.eql([])
        })
      })

      describe('when a role has no permissions', function() {
        it("set's a roles permission as an empty array", function() {
          const roles = [
            {
              name: 'Marketing',
              users: ['roley@influxdb.com', 'will@influxdb.com'],
            },
          ]

          const actual = buildRoles(roles)

          expect(actual[0].permissions).to.eql([])
        })
      })

      describe('when a role has users and permissions', function() {
        beforeEach(function() {
          const roles = [
            {
              name: 'Marketing',
              permissions: {
                '': ['ViewAdmin'],
                db1: ['ReadData'],
                db2: ['ReadData', 'AddRemoveNode'],
              },
              users: ['roley@influxdb.com', 'will@influxdb.com'],
            },
          ]

          this.roles = buildRoles(roles)
        })

        it('each role has a name and a list of users (if they exist)', function() {
          const role = this.roles[0]
          expect(role.name).to.equal('Marketing')
          expect(role.users).to.contain('roley@influxdb.com')
          expect(role.users).to.contain('will@influxdb.com')
        })

        it('transforms permissions into a list of objects and each permission has a list of resources', function() {
          expect(this.roles[0].permissions).to.eql([
            {
              name: 'ViewAdmin',
              displayName: 'View Admin',
              description: 'Can view or edit admin screens',
              resources: [''],
            },
            {
              name: 'ReadData',
              displayName: 'Read',
              description: 'Can read data',
              resources: ['db1', 'db2'],
            },
            {
              name: 'AddRemoveNode',
              displayName: 'Add/Remove Nodes',
              description: 'Can add/remove nodes from a cluster',
              resources: ['db2'],
            },
          ])
        })
      })
    })
  })

  describe('cluster utils', function() {
    describe('buildClusterAccounts', function() {
      // TODO: break down this test into smaller individual assertions.
      it('adds role information to each cluster account and parses permissions', function() {
        const users = [
          {
            name: 'jon@example.com',
            hash: 'xxxxx',
            permissions: {
              '': ['ViewAdmin'],
              db1: ['ReadData'],
            },
          },
          {
            name: 'ned@example.com',
            hash: 'xxxxx',
          },
        ]

        const roles = [
          {
            name: 'Admin',
            permissions: {
              db2: ['ViewAdmin'],
            },
            users: ['jon@example.com', 'ned@example.com'],
          },
          {
            name: 'Marketing',
            permissions: {
              db3: ['ReadData'],
            },
            users: ['jon@example.com'],
          },
        ]

        const actual = buildClusterAccounts(users, roles)

        const expected = [
          {
            name: 'jon@example.com',
            hash: 'xxxxx',
            permissions: [
              {
                name: 'ViewAdmin',
                displayName: 'View Admin',
                description: 'Can view or edit admin screens',
                resources: [''],
              },
              {
                name: 'ReadData',
                displayName: 'Read',
                description: 'Can read data',
                resources: ['db1'],
              },
            ],
            roles: [
              {
                name: 'Admin',
                permissions: [
                  {
                    name: 'ViewAdmin',
                    displayName: 'View Admin',
                    description: 'Can view or edit admin screens',
                    resources: ['db2'],
                  },
                ],
                users: ['jon@example.com', 'ned@example.com'],
              },
              {
                name: 'Marketing',
                permissions: [
                  {
                    name: 'ReadData',
                    displayName: 'Read',
                    description: 'Can read data',
                    resources: ['db3'],
                  },
                ],
                users: ['jon@example.com'],
              },
            ],
          },
          {
            name: 'ned@example.com',
            hash: 'xxxxx',
            permissions: [],
            roles: [
              {
                name: 'Admin',
                permissions: [
                  {
                    name: 'ViewAdmin',
                    displayName: 'View Admin',
                    description: 'Can view or edit admin screens',
                    resources: ['db2'],
                  },
                ],
                users: ['jon@example.com', 'ned@example.com'],
              },
            ],
          },
        ]

        expect(actual).to.eql(expected)
      })

      it('can handle empty results for users and roles', function() {
        const users = undefined
        const roles = undefined

        const actual = buildClusterAccounts(users, roles)

        expect(actual).to.eql([])
      })

      it('sets roles to an empty array if a user has no roles', function() {
        const users = [
          {
            name: 'ned@example.com',
            hash: 'xxxxx',
          },
        ]
        const roles = []

        const actual = buildClusterAccounts(users, roles)

        expect(actual[0].roles).to.eql([])
      })
    })
  })
})

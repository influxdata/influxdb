import {
  buildRoles,
  buildClusterAccounts,
  buildDefaultYLabel,
} from 'shared/presenters'
import defaultQueryConfig from 'utils/defaultQueryConfig'

describe('Presenters', () => {
  describe('roles utils', () => {
    describe('buildRoles', () => {
      describe('when a role has no users', () => {
        it("sets a role's users as an empty array", () => {
          const roles = [
            {
              name: 'Marketing',
              permissions: {
                '': ['ViewAdmin'],
              },
            },
          ]

          const actual = buildRoles(roles)

          expect(actual[0].users).toEqual([])
        })
      })

      describe('when a role has no permissions', () => {
        it("set's a roles permission as an empty array", () => {
          const roles = [
            {
              name: 'Marketing',
              users: ['roley@influxdb.com', 'will@influxdb.com'],
            },
          ]

          const actual = buildRoles(roles)

          expect(actual[0].permissions).toEqual([])
        })
      })

      describe('when a role has users and permissions', () => {
        let roles
        beforeEach(() => {
          const rs = [
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

          roles = buildRoles(rs)
        })

        it('each role has a name and a list of users (if they exist)', () => {
          const role = roles[0]
          const expected = ['will@influxdb.com', 'roley@influxdb.com']

          expect(role.name).toBe('Marketing')
          expect(role.users).toEqual(expect.arrayContaining(expected))
        })

        it('transforms permissions into a list of objects and each permission has a list of resources', () => {
          expect(roles[0].permissions).toEqual([
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

  describe('cluster utils', () => {
    describe('buildClusterAccounts', () => {
      // TODO: break down this test into smaller individual assertions.
      it('adds role information to each cluster account and parses permissions', () => {
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

        expect(actual).toEqual(expected)
      })

      it('can handle empty results for users and roles', () => {
        const users = undefined
        const roles = undefined

        const actual = buildClusterAccounts(users, roles)

        expect(actual).toEqual([])
      })

      it('sets roles to an empty array if a user has no roles', () => {
        const users = [
          {
            name: 'ned@example.com',
            hash: 'xxxxx',
          },
        ]
        const roles = []

        const actual = buildClusterAccounts(users, roles)

        expect(actual[0].roles).toEqual([])
      })
    })
  })

  describe('buildDefaultYLabel', () => {
    it('can return the correct string for field', () => {
      const query = defaultQueryConfig({id: 1})
      const fields = [{value: 'usage_system', type: 'field'}]
      const measurement = 'm1'
      const queryConfig = {...query, measurement, fields}
      const actual = buildDefaultYLabel(queryConfig)

      expect(actual).toBe('m1.usage_system')
    })

    it('can return the correct string for funcs with args', () => {
      const query = defaultQueryConfig({id: 1})
      const field = {value: 'usage_system', type: 'field'}
      const args = {
        value: 'mean',
        type: 'func',
        args: [field],
        alias: '',
      }

      const f1 = {
        value: 'derivative',
        type: 'func',
        args: [args],
        alias: '',
      }

      const fields = [f1]
      const measurement = 'm1'
      const queryConfig = {...query, measurement, fields}
      const actual = buildDefaultYLabel(queryConfig)

      expect(actual).toBe('m1.derivative_mean_usage_system')
    })

    it('returns a label of empty string if the query config is empty', () => {
      const query = defaultQueryConfig({id: 1})
      const actual = buildDefaultYLabel(query)

      expect(actual).toBe('')
    })
  })
})

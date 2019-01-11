import {ResourceOwner, User} from 'src/api'

export const resouceOwner: ResourceOwner[] = [
  {
    id: '1',
    name: 'John',
    status: User.StatusEnum.Active,
    links: {
      self: '/api/v2/users/1',
    },
    role: ResourceOwner.RoleEnum.Owner,
  },
  {
    id: '2',
    name: 'Jane',
    status: User.StatusEnum.Active,
    links: {
      self: '/api/v2/users/2',
    },
    role: ResourceOwner.RoleEnum.Owner,
  },
  {
    id: '3',
    name: 'Smith',
    status: User.StatusEnum.Active,
    links: {
      self: '/api/v2/users/3',
    },
    role: ResourceOwner.RoleEnum.Owner,
  },
]

import {
  CloudUser as GenCloudUser,
  Invite as GenInvite,
} from 'src/client/unifyRoutes'
import {RemoteDataState} from 'src/types'

export type Role = 'member' | 'owner'

export interface DraftCloudUser {
  email: string
  role: Role
  status: RemoteDataState
}

export interface DraftInvite {
  email: string
  role: Role
  status: RemoteDataState
}

export interface CloudUser extends GenCloudUser {
  status: RemoteDataState
}

export interface Invite extends GenInvite {
  status: RemoteDataState
}

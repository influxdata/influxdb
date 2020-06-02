import {DraftInvite, Role, RemoteDataState} from 'src/types'

export const draftInvite: DraftInvite = {
  email: '',
  role: 'owner' as const,
  status: RemoteDataState.NotStarted,
}

export const roles: Role[] = ['owner']

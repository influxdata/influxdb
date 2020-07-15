// Libraries
import {Dispatch} from 'redux'
import {push, RouterAction} from 'connected-react-router'
import {normalize} from 'normalizr'

// APIs
import {getErrorMessage} from 'src/utils/api'
import * as api from 'src/client'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {
  Action,
  setOrgs,
  addOrg,
  removeOrg,
  editOrg,
} from 'src/organizations/actions/creators'

// Constants
import {CLOUD} from 'src/shared/constants'

import {
  orgCreateSuccess,
  orgCreateFailed,
  bucketCreateSuccess,
  bucketCreateFailed,
  orgEditSuccess,
  orgEditFailed,
  orgRenameSuccess,
  orgRenameFailed,
} from 'src/shared/copy/notifications'

import {fireOrgIdReady} from 'src/shared/utils/analytics'

// Schemas
import {orgSchema, arrayOfOrgs} from 'src/schemas'

// Types
import {
  Organization,
  RemoteDataState,
  NotificationAction,
  Bucket,
  AppThunk,
  OrgEntities,
} from 'src/types'

export const getOrganizations = () => async (
  dispatch: Dispatch<Action>
): Promise<Organization[]> => {
  try {
    dispatch(setOrgs(RemoteDataState.Loading))

    const resp = await api.getOrgs({})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const {orgs} = resp.data

    const organizations = normalize<Organization, OrgEntities, string[]>(
      orgs,
      arrayOfOrgs
    )

    if (CLOUD) {
      fireOrgIdReady(organizations.result)
    }

    dispatch(setOrgs(RemoteDataState.Done, organizations))

    return orgs
  } catch (error) {
    console.error(error)
    dispatch(setOrgs(RemoteDataState.Error, null))
  }
}

export const createOrgWithBucket = (
  org: Organization,
  bucket: Bucket
): AppThunk<Promise<void>> => async (
  dispatch: Dispatch<Action | RouterAction | NotificationAction>
) => {
  let createdOrg: Organization

  try {
    const orgResp = await api.postOrg({data: org})
    if (orgResp.status !== 201) {
      throw new Error(orgResp.data.message)
    }

    createdOrg = orgResp.data

    dispatch(notify(orgCreateSuccess()))

    const normOrg = normalize<Organization, OrgEntities, string>(
      createdOrg,
      orgSchema
    )

    dispatch(addOrg(normOrg))
    dispatch(push(`/orgs/${createdOrg.id}`))

    const bucketResp = await api.postBucket({
      data: {...bucket, orgID: createdOrg.id},
    })

    if (bucketResp.status !== 201) {
      throw new Error(bucketResp.data.message)
    }

    dispatch(notify(bucketCreateSuccess()))
  } catch (error) {
    console.error(error)

    if (!createdOrg) {
      dispatch(notify(orgCreateFailed()))
    }
    const message = getErrorMessage(error)
    dispatch(notify(bucketCreateFailed(message)))
  }
}

export const createOrg = (org: Organization) => async (
  dispatch: Dispatch<Action | RouterAction | NotificationAction>
): Promise<void> => {
  try {
    const resp = await api.postOrg({data: org})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const createdOrg = resp.data
    const normOrg = normalize<Organization, OrgEntities, string>(
      createdOrg,
      orgSchema
    )

    dispatch(addOrg(normOrg))
    dispatch(push(`/orgs/${createdOrg.id}`))

    dispatch(notify(orgCreateSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(notify(orgCreateFailed()))
  }
}

export const deleteOrg = (org: Organization) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const resp = await api.deleteOrg({orgID: org.id})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeOrg(org.id))
  } catch (e) {
    console.error(e)
  }
}

export const updateOrg = (org: Organization) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.patchOrg({orgID: org.id, data: org})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updatedOrg = resp.data
    const normOrg = normalize<Organization, OrgEntities, string>(
      updatedOrg,
      orgSchema
    )

    dispatch(editOrg(normOrg))

    dispatch(notify(orgEditSuccess()))
  } catch (error) {
    dispatch(notify(orgEditFailed()))
    console.error(error)
  }
}

export const renameOrg = (
  originalName: string,
  org: Organization
): AppThunk<Promise<void>> => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.patchOrg({orgID: org.id, data: org})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const updatedOrg = resp.data

    const normOrg = normalize<Organization, OrgEntities, string>(
      updatedOrg,
      orgSchema
    )

    dispatch(editOrg(normOrg))
    dispatch(notify(orgRenameSuccess(updatedOrg.name)))
  } catch (error) {
    dispatch(notify(orgRenameFailed(originalName)))
    console.error(error)
  }
}

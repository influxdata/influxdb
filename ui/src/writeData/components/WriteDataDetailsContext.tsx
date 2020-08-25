// Libraries
import React, {FC, ReactNode, createContext, useState} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Utils
import {getAll} from 'src/resources/selectors'
import {getOrg} from 'src/organizations/selectors'

// Types
import {
  AppState,
  ResourceType,
  Bucket,
  Authorization,
  Organization,
} from 'src/types'

interface ComponentProps {
  children: ReactNode
}

type ReduxProps = ConnectedProps<typeof connector>

type Props = ComponentProps & ReduxProps

export interface WriteDataDetailsContextType {
  organization: Organization
  origin: string
  bucket: Bucket
  buckets: Bucket[]
  changeBucket: (bucket: Bucket) => void
  token: Authorization
  tokens: Authorization[]
  changeToken: (token: Authorization) => void
}

export const DEFAULT_WRITE_DATA_DETAILS_CONTEXT: WriteDataDetailsContextType = {
  organization: null,
  origin: '',
  bucket: null,
  buckets: [],
  changeBucket: () => {},
  token: null,
  tokens: [],
  changeToken: () => {},
}

export const WriteDataDetailsContext = createContext<
  WriteDataDetailsContextType
>(DEFAULT_WRITE_DATA_DETAILS_CONTEXT)

const WriteDataDetailsContextProvider: FC<Props> = ({
  organization,
  buckets,
  tokens,
  children,
}) => {
  const userBuckets = buckets.filter(b => b.type === 'user')
  const initialToken = tokens.length ? tokens[0] : null
  const [bucket, changeBucket] = useState<Bucket>(userBuckets[0])
  const [token, changeToken] = useState<Authorization>(initialToken)
  const origin = window.location.origin

  const value = {
    organization,
    origin,
    bucket,
    buckets: userBuckets,
    changeBucket,
    token,
    tokens,
    changeToken,
  }

  return (
    <WriteDataDetailsContext.Provider value={value}>
      {children}
    </WriteDataDetailsContext.Provider>
  )
}

const mstp = (state: AppState) => {
  const buckets = getAll<Bucket>(state, ResourceType.Buckets)
  const tokens = getAll<Authorization>(state, ResourceType.Authorizations)
  const organization = getOrg(state)

  return {
    buckets,
    tokens,
    organization,
  }
}

const connector = connect(mstp)

export default connector(WriteDataDetailsContextProvider)

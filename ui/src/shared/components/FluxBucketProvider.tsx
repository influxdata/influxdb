// Libraries
import {FC, useEffect} from 'react'
import {connect} from 'react-redux'

import {AppState, Bucket, ResourceType} from 'src/types'
import {getAll} from 'src/resources/selectors'

import loadServer from 'src/external/monaco.flux.server'

interface Props {
  buckets: Bucket[]
}

// using a singleton to mirror redux state
let BUCKETS: Bucket[] = []

function resolveBuckets() {
  return new Promise(resolve => {
    resolve(BUCKETS.map(b => b.name))
  })
}

loadServer().then(server => {
  server.register_buckets_callback(resolveBuckets)
})

const FluxBucketProvider: FC<Props> = ({buckets}) => {
  useEffect(() => {
    BUCKETS = buckets
  }, [buckets])

  return null
}

const mstp = (state: AppState): Props => {
  const buckets = getAll<Bucket>(state, ResourceType.Buckets)

  return {
    buckets,
  }
}

export default connect<Props, {}>(
  mstp,
  null
)(FluxBucketProvider)

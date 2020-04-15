// Libraries
import {FC} from 'react'
import {connect} from 'react-redux'

import {AppState, Bucket, ResourceType} from 'src/types'
import {getAll} from 'src/resources/selectors'

import loadServer from 'src/external/monaco.flux.server'

const FluxBucketProvider: FC<{}> = () => {
  return null
}

const mstp = (state: AppState): {} => {
  const buckets = getAll<Bucket>(state, ResourceType.Buckets)

  loadServer().then(server => {
    server.updateBuckets(buckets.map(b => b.name))
  })

  return {}
}

export default connect<{}, {}>(
  mstp,
  null
)(FluxBucketProvider)

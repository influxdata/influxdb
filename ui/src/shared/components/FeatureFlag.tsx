import {SFC} from 'react'

interface Props {
  name?: string
  children?: any
}

const FeatureFlag: SFC<Props> = props => {
  if (process.env.NODE_ENV === 'development') {
    return props.children
  }

  return null
}

export default FeatureFlag

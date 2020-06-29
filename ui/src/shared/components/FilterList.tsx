// Libraries
import {connect} from 'react-redux'

// Components
import FilterList, {StateProps, OwnProps} from 'src/shared/components/Filter'

import {AppState} from 'src/types'

const mstp = (state: AppState) => {
  return {labels: state.resources.labels.byID}
}

// Typing a generic connected component proved to be tricky:
// https://github.com/piotrwitek/react-redux-typescript-guide/issues/55
export default function FilterListContainer<T>() {
  return connect<StateProps>(mstp)(
    FilterList as new (props: OwnProps<T>) => FilterList<T>
  )
}

import _ from 'lodash'
import {saveToLocalStorage} from '../localStorage'

// Types
import {Store} from 'redux'
import {LocalStorage} from 'src/types/localStorage'

/**
 * Redux store enhancer (https://github.com/reactjs/redux/blob/master/docs/Glossary.md)
 * responsible for sending updates on data explorer state to a server to persist.
 * It subscribes a listener function to the store -- meaning every time the store emits an update
 * (after some state has changed), we'll have a chance to react.
 *
 * Debouncing the saveToLocalStorage to ensure we are stringify and setItem at most once per second.
 */

export default function persistState() {
  return next => (reducer, initialState: LocalStorage, enhancer) => {
    const store: Store<LocalStorage> = next(reducer, initialState, enhancer)
    const throttleMs = 1000

    store.subscribe(
      _.throttle(() => {
        saveToLocalStorage(store.getState())
      }, throttleMs)
    )

    return store
  }
}

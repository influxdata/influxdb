import {monaco} from '@monaco-editor/react'

const preInitCallbacks = []
let hasLoaded = false
let instance

monaco
  .init()
  .then(monaco => {
    instance = monaco

    while (preInitCallbacks.length) {
      preInitCallbacks.pop().call(this, instance)
    }
    hasLoaded = true
  })
  .catch(error =>
    console.error('An error occurred during initialization of Monaco: ', error)
  )

export default () =>
  new Promise(f => {
    if (!hasLoaded) {
      preInitCallbacks.push(f)
      return
    }

    f(instance)
  })

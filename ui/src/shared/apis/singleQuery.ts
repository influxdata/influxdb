import {File} from 'src/types'
import {runQuery} from 'src/shared/apis/query'

/*\

    Declare this at the module level to create a CancelBox mutex
    for runQuery. this will allow only one query to be run at a time, and
    pass its results to any calls made to it while waiting for the api.
    usage:
      const mutex = RunQueryPromiseMutex()

      function execute(query, state) {
        // return runQuery(orgID, query, extern, abortController)
        mutex.run(orgID, query, extern)
      }

\*/
export function RunQueryPromiseMutex() {
    const cached = []
    let processing = false

    const ret = {
        run: (orgID: string, query: string, extern?: File) => {
            return {
                promise: new Promise((resolve, reject) => {
                    if (processing) {
                        cached.push({
                            resolve,
                            reject,
                            cancel: () => {}
                        })

                        return
                    }

                    processing = true
                    const abortController = new AbortController()
                    cached.push({
                        resolve,
                        reject,
                        cancel: () => {
                            abortController.abort();
                        }
                    })

                    runQuery(orgID, query, extern, abortController)
                        .promise
                        .then((...args) => {
                            ret.resolve.apply(ret, args)
                        })
                        .catch((error: Error) => {
                            ret.reject(error)
                        })
                }),
                cancel: ret.cancel
            }
        },
        resolve: (...args) => {
            let curr

            while (cached.length) {
                curr = cached.pop()
                curr.resolve.apply(curr, args)
            }

            processing = false
        },
        reject: (error: Error) => {
            let curr

            while (cached.length) {
                curr = cached.pop()
                curr.reject(error)
                curr.cancel()
            }

            processing = false
        },
        cancel: () => {
            let curr

            while (cached.length) {
                curr = cached.pop()
                curr.cancel()
            }

            processing = false
        }
    }

    return ret
}

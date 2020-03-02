import {sendMessage, initialize} from 'src/external/monaco.flux.messages'

const queue = []
let _module = null,
  loading = false

type BucketCallback = () => Promise<string[]>

export interface LSPServer {
  send: (string) => Promise<object>
  register_buckets_callback: (BucketCallback) => void
}

export default function loader(): Promise<LSPServer> {
  return new Promise(resolve => {
    if (_module) {
      resolve(_module as LSPServer)
      return
    }

    queue.push(resolve)

    if (loading) {
      return
    }

    loading = true

    import('@influxdata/flux-lsp-browser').then(({Server}) => {
      _module = new Server(false)
      _module.send = function(message) {
        return sendMessage(message, _module)
      }

      sendMessage(initialize, _module)

      queue.reduce((prev, curr) => {
        return prev.then(() => {
          return curr(_module as LSPServer)
        })
      }, Promise.resolve([]))
    })
  })
}

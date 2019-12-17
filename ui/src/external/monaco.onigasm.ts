import {loadWASM} from 'onigasm' // peer dependency of 'monaco-textmate'

let wasm: boolean = false
let loading: boolean = false
const queue: Array<() => void> = []

export default function loader() {
  return new Promise(resolve => {
    if (wasm) {
      resolve()
      return
    }

    queue.push(resolve)

    if (loading) {
      return
    }

    loading = true

    loadWASM(require(`onigasm/lib/onigasm.wasm`)).then(() => {
      wasm = true
      queue.forEach(c => c())
    })
  })
}

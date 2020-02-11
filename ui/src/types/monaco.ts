import * as allMonaco from 'monaco-editor/esm/vs/editor/editor.api'
export {Server, ServerResponse} from '@influxdata/flux-lsp-browser'

export type MonacoType = typeof allMonaco
export type EditorType = allMonaco.editor.IStandaloneCodeEditor

export const FLUXLANGID = 'flux' as const

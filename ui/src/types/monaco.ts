import * as allMonaco from 'monaco-editor/esm/vs/editor/editor.api'

import * as lsp from '@influxdata/flux-lsp-browser'

export type ServerResponse = lsp.ServerResponse
export type MonacoType = typeof allMonaco
export type EditorType = allMonaco.editor.IStandaloneCodeEditor

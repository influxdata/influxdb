import {ast, emptyAST} from 'src/flux/constants/ast'
import * as editor from 'src/flux/constants/editor'
import * as argTypes from 'src/flux/constants/argumentTypes'
import * as funcNames from 'src/flux/constants/funcNames'
import * as builder from 'src/flux/constants/builder'
import * as vis from 'src/flux/constants/vis'
import * as explorer from 'src/flux/constants/explorer'

const MAX_RESPONSE_BYTES = 1e7 // 10 MB

export {
  ast,
  emptyAST,
  funcNames,
  argTypes,
  editor,
  builder,
  vis,
  explorer,
  MAX_RESPONSE_BYTES,
}

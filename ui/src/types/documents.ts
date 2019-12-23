import {Document as DocumentBase, DocumentCreate as DocCreate} from 'src/client'

import {Labels} from 'src/types'

export interface Document extends DocumentBase {
  labels?: Labels
}

export interface DocumentCreate extends DocCreate {}

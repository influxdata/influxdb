import {Telegraf as GenTelegraf} from 'src/client'
import {Label} from 'src/types'

export interface Telegraf extends GenTelegraf {
  label?: Label[]
}

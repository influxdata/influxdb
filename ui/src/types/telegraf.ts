import {Telegraf as GenTelegraf} from 'src/client'

export interface Telegraf extends Omit<GenTelegraf, 'labels'> {
  labels?: string[]
}

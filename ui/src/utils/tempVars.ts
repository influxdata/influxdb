interface Source {
  telegraf: string
  defaultRP: string
}

interface Value {
  value: string
  type: string
  selected: boolean
}

interface TempVar {
  tempVar: string
  id: string
  type: string
  values: Value[]
}

export const generateForHosts = (source: Source): TempVar[] => [
  {
    tempVar: ':db:',
    id: 'db',
    type: 'constant',
    values: [{value: source.telegraf, type: 'constant', selected: true}],
  },
  {
    tempVar: ':rp:',
    id: 'rp',
    type: 'constant',
    values: [{value: source.defaultRP, type: 'constant', selected: true}],
  },
]

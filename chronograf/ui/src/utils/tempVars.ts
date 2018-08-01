import {Source, Template, TemplateValueType, TemplateType} from 'src/types'

export const generateForHosts = (source: Source): Template[] => [
  {
    tempVar: ':db:',
    id: 'db',
    type: TemplateType.Constant,
    label: '',
    values: [
      {
        value: source.telegraf,
        type: TemplateValueType.Constant,
        selected: true,
        localSelected: true,
      },
    ],
  },
  {
    tempVar: ':rp:',
    id: 'rp',
    type: TemplateType.Constant,
    label: '',
    values: [
      {
        value: source.defaultRP,
        type: TemplateValueType.Constant,
        selected: true,
        localSelected: true,
      },
    ],
  },
]

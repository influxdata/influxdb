export default {
  columns: [
    {
      name: 'severity',
      position: 1,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
        {
          type: 'label',
          value: 'icon',
        },
        {
          type: 'label',
          value: 'text',
        },
      ],
    },
    {
      name: 'timestamp',
      position: 2,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
    {
      name: 'message',
      position: 3,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
    {
      name: 'facility',
      position: 4,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
    {
      name: 'time',
      position: 0,
      encodings: [
        {
          type: 'visibility',
          value: 'hidden',
        },
      ],
    },
    {
      name: 'procid',
      position: 5,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
        {
          type: 'displayName',
          value: 'Proc ID',
        },
      ],
    },
    {
      name: 'host',
      position: 7,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
      ],
    },
    {
      name: 'appname',
      position: 6,
      encodings: [
        {
          type: 'visibility',
          value: 'visible',
        },
        {
          type: 'displayName',
          value: 'Application',
        },
      ],
    },
  ],
}
